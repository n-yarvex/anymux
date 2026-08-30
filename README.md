## ✨ 特性

- 🚀 **多路复用**：一个 TCP 连接同时运行多个流
- 📦 **流控**：基于缓冲区的简单流控，防止内存耗尽
- 🔄 **版本协商**：支持协议版本协商（当前 v2）
- ⏱️ **超时控制**：握手超时、写超时、读写截止时间
- 🔒 **并发安全**：所有方法 goroutine 安全
- 🧹 **低内存分配**：预分配与分片策略，减少 GC 压力

---

## 📦 安装

```bash
go get github.com/n-yarvex/anymux
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

	"github.com/n-yarvex/anymux"
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
			sess := anymux.NewServerSession(c, func(st *anymux.Stream) {
				go handleStream(st)
			}, anymux.DefaultConfig)
			sess.Run()
		}(conn)
	}
}

func handleStream(st *anymux.Stream) {
	defer st.Close()
	io.Copy(st, st) // echo
}
```

### 客户端示例

```go
package main

import (
	"io"
	"log"
	"net"

	"github.com/n-yarvex/anymux"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	sess := anymux.NewClientSession(conn, anymux.DefaultConfig)
	sess.Run()
	defer sess.Close()

	st, err := sess.OpenStream()
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	st.Write([]byte("hello anymux"))
	buf := make([]byte, 1024)
	n, _ := st.Read(buf)
	log.Println(string(buf[:n]))
}
```

---

## 🧩 核心概念

### Session（会话）

一个 TCP 连接上的复用管理器，负责帧收发、流表维护、协议状态。

- 客户端：`NewClientSession`，可主动 `OpenStream`
- 服务器：`NewServerSession`，通过回调接收新流

### Stream（流）

独立的双向数据通道，类似 `net.Conn`。每个流有自己的缓冲区、截止时间和关闭状态。

---

## ⚙️ 配置

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `HandshakeTimeout` | `time.Duration` | 3s | 流握手超时（v2+ 且 sid≥2 时生效），超时关闭会话 |
| `WriteTimeout` | `time.Duration` | 5s | 底层写超时，每次写帧设置，写后清除 |
| `MaxFrameSize` | `int` | 64KB | 单帧负载最大长度（1~65535），数据写入自动分片 |
| `StreamBufferSize` | `int` | 64KB | 每个流接收缓冲区上限，满时丢弃新数据帧 |

```go
config := anymux.Config{
	HandshakeTimeout: 5 * time.Second,
	WriteTimeout:     10 * time.Second,
	MaxFrameSize:     32 * 1024,
	StreamBufferSize: 128 * 1024,
}
```

---

## 📚 API 参考

### 构造函数

`NewClientSession(conn net.Conn, config Config) *Session`

创建客户端会话，可主动打开流。

`NewServerSession(conn net.Conn, onNewStream func(*Stream), config Config) *Session`

创建服务器会话，收到 SYN 时回调 `onNewStream`。回调需快速返回，实际处理放入 goroutine。

---

### Session 方法

| 方法 | 说明 |
|---|---|
| `Run()` | 启动接收循环，必须调用（非阻塞） |
| `OpenStream() (*Stream, error)` | 客户端打开新流，发送 SYN，等待 SYNACK（v2+） |
| `Close() error` | 关闭会话，关闭所有流，等待接收循环退出 |
| `IsClosed() bool` | 判断会话是否已关闭 |

---

### Stream 方法

| 方法 | 说明 |
|---|---|
| `Read(p []byte) (int, error)` | 读数据，支持读截止时间；无数据时阻塞 |
| `Write(p []byte) (int, error)` | 写数据，自动分帧；可能部分写入 |
| `Close() error` | 关闭流，发送 FIN 帧 |
| `SetReadDeadline(t time.Time) error` | 设置读截止时间，零值取消 |
| `SetWriteDeadline(t time.Time) error` | 设置写截止时间，零值取消 |
| `SetDeadline(t time.Time) error` | 同时设置读写截止时间 |
| `HandshakeSuccess() error` | 发送空 SYNACK 确认握手（v2+） |
| `HandshakeFailure(err error) error` | 发送 SYNACK 携带错误信息（v2+） |
| `LocalAddr() net.Addr` | 本地地址（若底层连接支持） |
| `RemoteAddr() net.Addr` | 远程地址（若底层连接支持） |

---

## 📡 协议概要

帧格式：`[cmd:1][sid:4][len:2][payload:len]`，大端序。

| 命令 | 值 | 方向 | 说明 |
|---|---|---|---|
| `SYN` | 1 | C→S | 打开新流 |
| `PSH` | 2 | ↔ | 数据帧 |
| `FIN` | 3 | ↔ | 关闭流 |
| `VERSION_REQUEST` | 4 | C→S | 版本请求（`v=2\n`） |
| `ERROR` | 5 | S→C | 协议错误 |
| `SYNACK` | 7 | ↔ | 握手确认/失败 |
| `VERSION_RESPONSE` | 10 | S→C | 版本响应（`v=2\n`） |

- 客户端 `Run()` 时发送版本请求，服务器回复版本响应。
- 双方版本 ≥2 时，流建立需要 SYNACK 确认，超时由 `HandshakeTimeout` 控制。
- 关闭流发送 FIN，收到后移除本地流。

---

## ⚠️ 注意事项

1. **必须调用 `Run()`**：否则无法接收数据，`Close()` 会阻塞。
2. **回调不可阻塞**：服务器 `onNewStream` 中必须启动新 goroutine 处理流。
3. **及时读取**：流缓冲区满后新数据直接丢弃，需保证读取速度。
4. **写超时是连接级**：写操作内部加锁，但超时影响整个连接。
5. **关闭可能阻塞**：等待接收循环退出，若底层连接不响应截止时间可能卡住。
6. **部分写入**：`Write` 可能返回 `n < len(p)`，需处理。

---
