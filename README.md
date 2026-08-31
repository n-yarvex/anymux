## ✨ 特性

- 🚀 **多路复用**：一个 TCP 连接同时运行多个流
- 📦 **流控**：基于缓冲区的简单流控，缓冲区满时丢弃新数据帧
- 🔄 **版本协商**：客户端/服务器通过 Settings 帧协商协议版本（当前 v2）
- ⏱️ **超时控制**：握手超时、写超时、读/写截止时间（Deadline）
- 🔒 **并发安全**：Session、Stream 所有导出方法均为 goroutine 安全
- 🧹 **低内存分配**：帧缓冲区、负载缓冲区均使用 `sync.Pool`，降低 GC 压力

---

## 📦 安装

```
go get <module-path>/mux
```

---

## 🚀 快速开始

### 服务端示例

```go
package main

import (
	"io"
	"log"
	"net"

	"yourmodule/mux"
)

func main() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go func(c net.Conn) {
			sess := mux.NewServerSession(c, func(st *mux.Stream) {
				go handleStream(st)
			}, mux.DefaultConfig())
			sess.Run()
		}(conn)
	}
}

func handleStream(st *mux.Stream) {
	defer st.Close()
	io.Copy(st, st) // echo
}
```

### 客户端示例

```go
package main

import (
	"log"
	"net"

	"yourmodule/mux"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	sess := mux.NewClientSession(conn, mux.DefaultConfig())
	sess.Run()
	defer sess.Close()

	st, err := sess.OpenStream()
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	st.Write([]byte("hello mux"))
	buf := make([]byte, 1024)
	n, _ := st.Read(buf)
	log.Println(string(buf[:n]))
}
```

---

## 🧩 核心概念

### Session（会话）

一个 TCP 连接上的复用管理器，负责帧收发、流表维护、版本协商与协议状态。

- 客户端：`NewClientSession` 创建，可主动 `OpenStream`
- 服务器：`NewServerSession` 创建，通过 `onNewStream` 回调接收新流

### Stream（流）

独立的双向数据通道，实现 `io.ReadWriteCloser`，语义上类似 `net.Conn`（`Read`/`Write`/`Close`/`SetDeadline` 等）。每个流拥有独立的接收缓冲区、独立的读写截止时间和独立的关闭状态，互不影响。

### PipeDeadline

内部使用的截止时间原语，`Set` 到期后向 `Wait()` 返回的 channel 发出一次性信号，供 `Read`/`Write` 阻塞等待时唤醒；配合 `Expired()` 判断是否已超时。

---

## ⚙️ 配置

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `HandshakeTimeout` | `time.Duration` | 3s | 流握手超时（双方协商到 v2 且 `sid ≥ 1` 时生效），超时直接关闭整个会话 |
| `WriteTimeout` | `time.Duration` | 5s | 底层连接写超时，每次写帧前设置，写完立即清除 |
| `MaxFrameSize` | `int` | 64KB | 单帧负载最大长度，取值范围 `[1, 65535]`，超出范围会被 `normalizeConfig` 钳制；数据写入超过该值会自动分片 |
| `StreamBufferSize` | `int` | 64KB | 每个流接收缓冲区上限，超限时新到达的数据帧被丢弃并触发流关闭 |

```go
cfg := mux.Config{
	HandshakeTimeout: 5 * time.Second,
	WriteTimeout:     10 * time.Second,
	MaxFrameSize:     32 * 1024,
	StreamBufferSize: 128 * 1024,
}
// 未设置或非法的字段会被 normalizeConfig 重置为默认值
```

---

## 📚 API 参考

### 构造函数

`NewClientSession(conn net.Conn, cfg Config) *Session`
创建客户端会话，可通过 `OpenStream` 主动发起新流。

`NewServerSession(conn net.Conn, onNewStream func(*Stream), cfg Config) *Session`
创建服务器会话，收到对端 `SYN` 帧且已完成版本协商时回调 `onNewStream`。**回调内不可执行阻塞逻辑**，需自行 `go` 出去处理。

`DefaultConfig() Config`
返回默认配置：握手超时 3s、写超时 5s、最大帧 64KB、流缓冲区 64KB。

---

### Session 方法

| 方法 | 说明 |
| --- | --- |
| `Run()` | 启动接收循环（`recvLoop`），客户端同时发送版本协商 Settings 帧；仅首次调用生效，非阻塞 |
| `OpenStream() (*Stream, error)` | 仅客户端可调用；若尚未完成版本协商会阻塞等待，超时返回错误；成功后发送 `SYN` 帧 |
| `Close() error` | 关闭会话：关闭全部流、设置连接读写截止时间为当前时间、等待接收循环退出（至多 100ms）、关闭底层连接 |
| `IsClosed() bool` | 判断会话是否已关闭 |

---

### Stream 方法

| 方法 | 说明 |
| --- | --- |
| `Read(p []byte) (int, error)` | 从接收缓冲区读取数据；无数据时阻塞，直到有数据、流关闭或读截止时间到达 |
| `Write(p []byte) (int, error)` | 按 `MaxFrameSize` 自动分片写出；写失败会关闭流并关闭整个会话 |
| `Close() error` | 关闭流并向对端发送 `FIN` 帧，幂等 |
| `SetReadDeadline(t time.Time) error` | 设置读截止时间，零值表示取消 |
| `SetWriteDeadline(t time.Time) error` | 设置写截止时间，零值表示取消 |
| `SetDeadline(t time.Time) error` | 同时设置读、写截止时间 |
| `HandshakeSuccess() error` | 服务端确认接受新流，发送空 `SYNACK`；仅在协议 v2 下可用 |
| `HandshakeFailure(err error) error` | 服务端拒绝新流，发送携带错误信息的 `SYNACK` 并使流失败；仅在协议 v2 下可用 |
| `HandshakeDone() <-chan struct{}` | 握手完成（成功或失败）时关闭的 channel，可用于 `select` |
| `WaitHandshake() error` | 阻塞直至握手完成，返回握手错误（若有） |
| `LocalAddr() net.Addr` | 返回底层连接的本地地址 |
| `RemoteAddr() net.Addr` | 返回底层连接的远程地址 |

---

## 📡 协议概要

帧格式（7 字节定长头 + 变长负载），大端序：

```
+--------+-------------+-----------+-----------------+
| cmd:1B | sid:4B(BE)  | len:2B(BE)|   payload:len B  |
+--------+-------------+-----------+-----------------+
```

| 命令 | 值 | 方向 | 说明 |
| --- | --- | --- | --- |
| `cmdWaste` | 0 | ↔ | 保留/填充帧，接收方忽略 |
| `cmdSYN` | 1 | C→S | 打开新流 |
| `cmdPSH` | 2 | ↔ | 数据帧，负载即流数据 |
| `cmdFIN` | 3 | ↔ | 关闭流，对端收到后移除本地流表项 |
| `cmdSettings` | 4 | C→S | 客户端版本协商请求，负载形如 `v=2\n` |
| `cmdAlert` | 5 | S→C / C→S | 协议错误告警，收发双方收到后均终止连接 |
| `cmdUpdatePaddingScheme` | 6 | — | 保留字段，当前实现未使用 |
| `cmdSYNACK` | 7 | ↔ | 握手确认：空负载表示成功，非空负载表示失败原因 |
| `cmdHeartRequest` | 8 | ↔ | 心跳请求，收到后立即回复 `cmdHeartResponse` |
| `cmdHeartResponse` | 9 | ↔ | 心跳响应 |
| `cmdServerSettings` | 10 | S→C | 服务器版本协商响应，负载形如 `v=2\n` |

### 建流流程（v2 协议）

1. 会话建立后，客户端 `Run()` 发送 `cmdSettings(v=2)`。
2. 服务器收到后记录版本并回复 `cmdServerSettings(v=2)`；若版本不支持则回复 `cmdAlert` 并断开。
3. 客户端收到 `cmdServerSettings` 后标记协议版本，并为已存在的流启动握手计时器。
4. 客户端 `OpenStream()` 发送 `cmdSYN`；服务器收到后创建 `Stream` 并回调 `onNewStream`。
5. 服务器业务逻辑调用 `HandshakeSuccess()` / `HandshakeFailure(err)` 发送 `cmdSYNACK`；客户端收到后停止握手计时器，若失败则关闭该流。
6. 若握手计时器（`HandshakeTimeout`）到期仍未收到 `SYNACK`，客户端**关闭整个会话**（而非单个流）。

### 数据与关闭

- 数据通过 `cmdPSH` 传输，单帧最大 `MaxFrameSize`，超出部分由 `Write` 自动分片为多帧。
- 任意一端 `Close()` 流会发送 `cmdFIN`；对端收到后从流表中移除并将本地 `Stream` 标记为 `io.EOF`。
- `cmdAlert` 是致命错误，接收方会直接终止整个接收循环并关闭会话。

---

## ⚠️ 注意事项

1. **必须调用 `Run()`**：否则不会启动接收循环，`OpenStream`/`Close` 可能一直阻塞或行为异常。
2. **`onNewStream` 回调不可阻塞**：内部以 `go onNewStream(st)` 调用，但回调本身若同步做重活仍会拖慢单条 goroutine，建议回调内部再 `go` 出去处理业务。
3. **及时读取**：`StreamBufferSize` 满后新到达的数据帧会被丢弃，并触发该流因"接收缓冲区溢出"而关闭。
4. **写超时是连接级的**：`WriteTimeout` 作用于底层 `net.Conn`，所有流共用同一把写锁（`connLock`），高并发写入时会相互排队。
5. **`Close()` 可能短暂阻塞**：会等待接收循环退出，最多等待 100ms。
6. **部分写入需处理**：`Write` 在遇到 `io.ErrShortWrite` 等错误时可能返回 `n < len(p)`，调用方需要检查返回值。
7. **`MaxFrameSize` 建议双端一致**：接收方对超过本地 `MaxFrameSize` 的帧会直接丢弃负载并跳过该帧，不会报错，可能导致数据丢失，务必保证客户端/服务器配置一致。