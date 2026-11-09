// epoll_server.cpp — 单 Reactor(epoll ET) + 线程池 + 长度前缀协议的 KV 服务
// 编译: g++ -std=c++17 -O2 -pthread epoll_server.cpp -o epoll_server
// 运行: ./epoll_server 9000 4

#include <arpa/inet.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

constexpr size_t   kMaxConns = 1024;
constexpr uint32_t kMaxBody  = 4096;

// ==================== 线程池 ====================
class ThreadPool {
public:
    explicit ThreadPool(std::size_t n) {
        for (std::size_t i = 0; i < n; ++i)
            workers_.emplace_back([this] { worker_loop(); });
    }
    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_)
            if (t.joinable()) t.join();
    }

    void submit(std::function<void()> fn) {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            tasks_.push(std::move(fn));
        }
        cv_.notify_one();
    }

    std::size_t size() const { return workers_.size(); }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> fn;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                cv_.wait(lk, [this] { return stop_ || !tasks_.empty(); });
                if (stop_ && tasks_.empty()) return;
                fn = std::move(tasks_.front());
                tasks_.pop();
            }
            fn();
        }
    }

    std::vector<std::thread>           workers_;
    std::queue<std::function<void()>>  tasks_;
    std::mutex                         mtx_;
    std::condition_variable            cv_;
    bool                               stop_ = false;
};

// ==================== KV 存储 ====================
class KvStore {
public:
    std::string get(const std::string& k) {
        std::shared_lock<std::shared_mutex> lk(mtx_);
        auto it = map_.find(k);
        return it == map_.end() ? std::string() : it->second;
    }
    void set(const std::string& k, const std::string& v) {
        std::unique_lock<std::shared_mutex> lk(mtx_);
        map_[k] = v;
    }
    bool del(const std::string& k) {
        std::unique_lock<std::shared_mutex> lk(mtx_);
        return map_.erase(k) > 0;
    }
    std::size_t count() {
        std::shared_lock<std::shared_mutex> lk(mtx_);
        return map_.size();
    }

private:
    std::unordered_map<std::string, std::string> map_;
    std::shared_mutex                            mtx_;
};

// ==================== 业务处理 ====================
// 请求体: SET k v | GET k | DEL k
// 响应体: OK | VALUE:v | NIL | ERR:xxx
std::string handle(const std::string& req, KvStore& kv) {
    if (req.empty()) return "ERR:empty";

    std::size_t sp1 = req.find(' ');
    std::string cmd = req.substr(0, sp1);

    if (cmd == "SET") {
        if (sp1 == std::string::npos) return "ERR:bad args";
        std::size_t sp2 = req.find(' ', sp1 + 1);
        if (sp2 == std::string::npos) return "ERR:bad args";
        kv.set(req.substr(sp1 + 1, sp2 - sp1 - 1), req.substr(sp2 + 1));
        return "OK";
    }
    if (cmd == "GET") {
        if (sp1 == std::string::npos) return "ERR:bad args";
        std::string v = kv.get(req.substr(sp1 + 1));
        return v.empty() ? "NIL" : "VALUE:" + v;
    }
    if (cmd == "DEL") {
        if (sp1 == std::string::npos) return "ERR:bad args";
        return kv.del(req.substr(sp1 + 1)) ? "OK" : "NIL";
    }
    return "ERR:unknown cmd";
}

// ==================== 连接 ====================
struct Conn {
    int                fd = -1;
    std::atomic<bool>  alive{true};
    std::string        in;      // 读缓冲，用于拆粘包
};
using ConnPtr = std::shared_ptr<Conn>;

// ==================== 服务器 ====================
class Server {
public:
    Server(int port, std::size_t workers)
        : port_(port), pool_(workers) {}

    void run() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) { perror("socket"); return; }

        int on = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        set_nonblock(listen_fd_);

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port        = htons(static_cast<uint16_t>(port_));

        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            perror("bind");
            return;
        }
        if (::listen(listen_fd_, SOMAXCONN) < 0) {
            perror("listen");
            return;
        }

        epfd_ = ::epoll_create1(0);
        if (epfd_ < 0) { perror("epoll_create1"); return; }
        add_event(listen_fd_, EPOLLIN | EPOLLET);

        std::cout << "epoll server listening on " << port_
                  << ", workers = " << pool_.size() << std::endl;

        start_stat();

        std::vector<epoll_event> evs(256);
        while (running_) {
            int n = ::epoll_wait(epfd_, evs.data(), static_cast<int>(evs.size()), 1000);
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("epoll_wait");
                break;
            }
            for (int i = 0; i < n; ++i) {
                int      fd = evs[i].data.fd;
                uint32_t e  = evs[i].events;

                if (fd == listen_fd_) {
                    accept_all();
                    continue;
                }
                if (e & EPOLLIN) read_all(fd);
                if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) close_conn(fd);
            }
        }
    }

private:
    static void set_nonblock(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    void add_event(int fd, uint32_t events) {
        epoll_event ev{};
        ev.events   = events;
        ev.data.fd  = fd;
        if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            perror("epoll_ctl ADD");
            close_conn(fd);
        }
    }

    void del_event(int fd) {
        ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
    }

    // ET 模式下必须循环 accept 到 EAGAIN，否则会漏掉连接
    void accept_all() {
        while (true) {
            sockaddr_in cli{};
            socklen_t   len = sizeof(cli);
            int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&cli), &len);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR) continue;
                perror("accept");
                break;
            }

            {
                std::lock_guard<std::mutex> lk(conn_mtx_);
                if (conns_.size() >= kMaxConns) {
                    ::close(fd);
                    continue;
                }
                auto c = std::make_shared<Conn>();
                c->fd  = fd;
                conns_[fd] = c;
            }
            set_nonblock(fd);
            { int nd = 1; ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd)); }
            add_event(fd, EPOLLIN | EPOLLET | EPOLLRDHUP);
            ++accepted_;
        }
    }

    void read_all(int fd) {
        ConnPtr c;
        {
            std::lock_guard<std::mutex> lk(conn_mtx_);
            auto it = conns_.find(fd);
            if (it == conns_.end()) return;
            c = it->second;
        }

        char buf[4096];
        while (true) {
            ssize_t n = ::read(fd, buf, sizeof(buf));
            if (n > 0) {
                c->in.append(buf, static_cast<std::size_t>(n));
                recv_bytes_ += static_cast<uint64_t>(n);
                if (c->in.size() > kMaxBody * 4) {
                    close_conn(fd);
                    return;
                }
            } else if (n == 0) {
                close_conn(fd);
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                close_conn(fd);
                return;
            }
        }

        // 拆包：4 字节长度（网络序） + 请求体
        while (c->in.size() >= 4) {
            uint32_t be = 0;
            std::memcpy(&be, c->in.data(), 4);
            uint32_t len = ntohl(be);
            if (len > kMaxBody) {
                close_conn(fd);
                return;
            }
            if (c->in.size() < 4 + len) break;

            std::string req = c->in.substr(4, len);
            c->in.erase(0, 4 + len);

            pool_.submit([this, c, req = std::move(req)]() mutable {
                on_request(c, req);
            });
        }
    }

    void on_request(ConnPtr c, const std::string& req) {
        if (!c->alive.load()) return;

        std::string resp = handle(req, kv_);

        std::string out;
        out.resize(4 + resp.size());
        uint32_t len = htonl(static_cast<uint32_t>(resp.size()));
        std::memcpy(out.data(), &len, 4);
        std::memcpy(out.data() + 4, resp.data(), resp.size());

        // 简化：响应体很小，直接 send。EAGAIN 或写失败则标记连接失效，
        // 由 IO 线程在 EPOLLRDHUP 时回收。生产做法是挂写队列 + EPOLLOUT。
        ssize_t n = ::send(c->fd, out.data(), out.size(), MSG_NOSIGNAL);
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            c->alive.store(false);
        }
        ++done_;
    }

    void close_conn(int fd) {
        ConnPtr c;
        {
            std::lock_guard<std::mutex> lk(conn_mtx_);
            auto it = conns_.find(fd);
            if (it == conns_.end()) return;
            c = it->second;
            conns_.erase(it);
        }
        c->alive.store(false);
        del_event(fd);
        ::close(fd);
        ++closed_;
    }

    void start_stat() {
        std::thread([this] {
            auto     last      = std::chrono::steady_clock::now();
            uint64_t last_done = 0;
            uint64_t last_recv = 0;
            while (running_) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                auto   now = std::chrono::steady_clock::now();
                uint64_t d = done_.load();
                uint64_t r = recv_bytes_.load();
                double sec = std::chrono::duration<double>(now - last).count();
                if (sec <= 0) sec = 1.0;

                std::size_t conns = 0;
                {
                    std::lock_guard<std::mutex> lk(conn_mtx_);
                    conns = conns_.size();
                }

                std::cout << "[stat] qps "
                          << static_cast<uint64_t>((d - last_done) / sec)
                          << "  recv_bytes " << (r - last_recv)
                          << "  done " << d
                          << "  conns " << conns
                          << "  kv " << kv_.count()
                          << std::endl;

                last      = now;
                last_done = d;
                last_recv = r;
            }
        }).detach();
    }

    int          port_;
    int          listen_fd_ = -1;
    int          epfd_      = -1;
    ThreadPool   pool_;
    KvStore      kv_;

    std::mutex                            conn_mtx_;
    std::unordered_map<int, ConnPtr>      conns_;

    std::atomic<bool>     running_{true};
    std::atomic<uint64_t> accepted_{0};
    std::atomic<uint64_t> closed_{0};
    std::atomic<uint64_t> done_{0};
    std::atomic<uint64_t> recv_bytes_{0};
};

}  // namespace

int main(int argc, char** argv) {
    int         port    = 9000;
    std::size_t workers = std::thread::hardware_concurrency();
    if (workers == 0) workers = 4;

    if (argc >= 2) port    = std::atoi(argv[1]);
    if (argc >= 3) workers = static_cast<std::size_t>(std::atoi(argv[2]));

    Server server(port, workers);
    server.run();
    return 0;
}
