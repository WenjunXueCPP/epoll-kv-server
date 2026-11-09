// bench_client.cpp — epoll-kv-server 压测客户端
// 每线程一条长连接，round-trip 模式：发一条 SET，收完整响应后才发下一条
// 编译: g++ -std=c++17 -O2 -pthread bench_client.cpp -o bench_client
// 运行: ./bench_client 127.0.0.1 9000 4 20000

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

namespace {

int connect_to(const char* ip, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); std::exit(1); }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        std::fprintf(stderr, "bad ip: %s\n", ip);
        std::exit(1);
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("connect");
        std::exit(1);
    }
        { int nd = 1; ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd)); }

    return fd;
}

bool send_all(int fd, const char* p, std::size_t n) {
    while (n > 0) {
        ssize_t w = ::send(fd, p, n, 0);
        if (w <= 0) return false;
        p += w;
        n -= static_cast<std::size_t>(w);
    }
    return true;
}

bool recv_exact(int fd, char* p, std::size_t n) {
    while (n > 0) {
        ssize_t r = ::recv(fd, p, n, 0);
        if (r <= 0) return false;
        p += r;
        n -= static_cast<std::size_t>(r);
    }
    return true;
}

struct Result {
    long                ok   = 0;
    long                fail = 0;
    std::vector<double> lat_us;   // 每条请求的往返延迟（微秒）
};

}  // namespace

int main(int argc, char** argv) {
    const char* ip         = argc >= 2 ? argv[1] : "127.0.0.1";
    int         port       = argc >= 3 ? std::atoi(argv[2]) : 9000;
    int         threads    = argc >= 4 ? std::atoi(argv[3]) : 4;
    long        per_thread = argc >= 5 ? std::atol(argv[4]) : 20000;

    std::printf("bench: %s:%d  threads=%d  per_thread=%ld  total=%ld\n",
                ip, port, threads, per_thread, threads * per_thread);

    std::vector<Result> results(static_cast<std::size_t>(threads));

    auto worker = [&](int tid) {
        Result& r = results[static_cast<std::size_t>(tid)];
        r.lat_us.reserve(static_cast<std::size_t>(per_thread));

        int  fd   = connect_to(ip, port);
        long base = static_cast<long>(tid) * per_thread;

        for (long i = 0; i < per_thread; ++i) {
            std::string body = "SET k" + std::to_string(base + i) + " v";

            char     hdr[4];
            uint32_t len = htonl(static_cast<uint32_t>(body.size()));
            std::memcpy(hdr, &len, 4);

            auto t0 = std::chrono::steady_clock::now();

                        std::string out(4 + body.size(), '\0');
            std::memcpy(out.data(), hdr, 4);
            std::memcpy(out.data() + 4, body.data(), body.size());
            if (!send_all(fd, out.data(), out.size())) {

                ++r.fail;
                break;
            }

            uint32_t be = 0;
            if (!recv_exact(fd, reinterpret_cast<char*>(&be), 4)) {
                ++r.fail;
                break;
            }
            uint32_t rlen = ntohl(be);
            std::string resp(rlen, '\0');
            if (rlen > 0 && !recv_exact(fd, resp.data(), rlen)) {
                ++r.fail;
                break;
            }

            auto t1 = std::chrono::steady_clock::now();
            r.lat_us.push_back(
                std::chrono::duration<double, std::micro>(t1 - t0).count());

            if (resp == "OK") ++r.ok;
            else              ++r.fail;
        }

        ::close(fd);
    };

    auto wall0 = std::chrono::steady_clock::now();

    std::vector<std::thread> ts;
    ts.reserve(static_cast<std::size_t>(threads));
    for (int i = 0; i < threads; ++i) ts.emplace_back(worker, i);
    for (auto& t : ts) t.join();

    auto   wall1 = std::chrono::steady_clock::now();
    double secs  = std::chrono::duration<double>(wall1 - wall0).count();

    long                ok = 0, fail = 0;
    std::vector<double> all;
    for (auto& r : results) {
        ok += r.ok;
        fail += r.fail;
        all.insert(all.end(), r.lat_us.begin(), r.lat_us.end());
    }

    if (all.empty()) {
        std::printf("no sample, all requests failed\n");
        return 1;
    }

    std::sort(all.begin(), all.end());
    double sum = 0;
    for (double v : all) sum += v;

    auto pct = [&](double p) {
        std::size_t idx = static_cast<std::size_t>(all.size() * p);
        if (idx >= all.size()) idx = all.size() - 1;
        return all[idx];
    };

    std::printf("---- result ----\n");
    std::printf("ok %ld   fail %ld\n", ok, fail);
    std::printf("wall      %.3f s\n", secs);
    std::printf("throughput %.0f req/s\n", static_cast<double>(ok) / secs);
    std::printf("latency  avg %.1f us   p50 %.1f us   p99 %.1f us   max %.1f us\n",
                sum / static_cast<double>(all.size()),
                pct(0.50), pct(0.99), all.back());
    return 0;
}