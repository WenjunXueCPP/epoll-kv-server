# epoll-kv-server

用 epoll（ET 模式）手写 IO 层的内存 KV 服务，附自写压测客户端。

和常见"套一个网络库"的项目不同，这里的 socket 读写、事件分发、连接管理全部手写，没有第三方网络库参与。

## 协议

请求 = 4 字节网络序长度前缀（只计 body）+ body；响应 = 4 字节长度前缀 + 响应体。

| 命令 | 请求体 | 响应 |
|---|---|---|
| 设置 | `SET k v` | `OK` |
| 读取 | `GET k` | `VALUE:v` / `NIL` |
| 删除 | `DEL k` | `OK` / `NIL` |
| 未知命令 | — | `ERR:xxx` |

长度前缀用于解决 TCP 粘包与半包：一次 `read` 可能拿到半条或两条，服务端按前缀攒够再处理。

## 架构

```
main
 └─ Server
     ├─ epoll_create1 / epoll_wait —— 单 Reactor，主线程
     ├─ listen fd：ET + EPOLLIN，accept 循环到 EAGAIN
     ├─ client fd：ET + EPOLLIN | EPOLLRDHUP
     │    └─ read 循环到 EAGAIN，按长度前缀拆包，submit 到线程池
     ├─ ThreadPool：mutex + condvar + 任务队列，默认 4 worker
     │    └─ handle()：SET / GET / DEL → send 响应
     └─ KvStore：unordered_map + std::shared_mutex
```

ET 模式的要点：`accept` 和 `read` 都必须循环到 `EAGAIN`。边沿触发只报一次状态变化，不读到空会把剩余连接和剩余数据漏掉。

## 编译与运行

```bash
g++ -std=c++17 -O2 -pthread epoll_server.cpp -o epoll_server
g++ -std=c++17 -O2 -pthread bench_client.cpp  -o bench_client
```

```bash
./epoll_server 9000 4
```

参数 `./epoll_server <port> <worker_num>`；worker 缺省取 `std::thread::hardware_concurrency()`。

单条请求自测：

```bash
python3 -c "import socket,struct;s=socket.create_connection(('127.0.0.1',9000));b=b'SET a 1';s.sendall(struct.pack('>I',len(b))+b);print(s.recv(64))"
# b'\x00\x00\x00\x02OK'
```

## 压测

```bash
./bench_client 127.0.0.1 9000 4 20000
```

每连接一条长连接，round-trip 模式：发一条 → 收完整响应 → 再发下一条。测的是完整处理链路加内核往返，不是把数据塞进发送缓冲区的速率。

环境：VMware Ubuntu，4 核 / 3 GB，loopback。

### 结果

| 连接数 | 请求数 | 吞吐 | avg | P50 | P99 |
|---|---|---|---|---|---|
| 1 | 80000 | 7777 req/s | 127.5 µs | 96.9 µs | 634.9 µs |
| 4 | 80000 | 14889 req/s | 266.4 µs | 194.7 µs | 1445.3 µs |
| 8 | 80000 | 24403 req/s | 324.8 µs | 281.7 µs | 1185.8 µs |

三组全部 `fail 0`。并发 1 → 8 吞吐放大 3.14 倍；延迟随并发上升符合排队规律。

### 一个值得记下的坑：41 ms 延迟

第一轮压测吞吐只有 97 req/s，P50 稳定停在 41020 µs：

```
ok 11823  fail 4
wall 121.642 s
throughput 97 req/s
latency avg 41142.4 us  p50 41020.5 us  p99 43539.8 us  max 83917.0 us
```

41 ms 是 Nagle 算法与对端 delayed ACK（最多 40 ms）互相等待的特征值，不是服务端处理慢。4 个连接合计 97 req/s，折合每连接约 24 条/秒，正好对应 41 ms 的单条往返。

修复：两端 `setsockopt(IPPROTO_TCP, TCP_NODELAY)`，并把请求 header 与 body 合并成一次 `send`。

```
ok 80000  fail 0
wall 5.373 s
throughput 14889 req/s
latency avg 266.4 us  p50 194.7 us  p99 1445.3 us  max 8112.0 us
```

吞吐 97 → 14889（约 153 倍），P50 41.0 ms → 0.19 ms（约 154 倍）。两个倍数一致，说明瓶颈同源。

## 已知简化

- 响应直接 `send`，没有应用层写队列 + `EPOLLOUT`，发送缓冲满时会丢数据
- 未做 fd 复用保护：连接关闭后 fd 被新连接复用时，旧事件可能串到新连接
- `kMaxConns = 1024`、`kMaxBody = 4096` 为固定上限
- 单 Reactor：所有 IO 在一个线程，worker 只处理业务逻辑

## 目录

```
epoll_server.cpp   服务端
bench_client.cpp   压测客户端
```
