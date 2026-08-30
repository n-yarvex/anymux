# anymux特性

· 多路复用：单个 TCP 连接承载多个并发流
· 流控：基于缓冲区的简单流控，防止内存耗尽
· 版本协商：支持协议版本协商（当前 v2）
· 超时控制：握手超时、写超时、读写截止时间
· 并发安全：Session 和 Stream 可安全并发使用
· 低内存分配：数据路径使用预分配和零拷贝策略（适度）

安装

```bash
go get github.com/n-yarvex/anymux
```

快速开始

服务器端

```go
package main

import (
	"io"
	"log"
	"net"

	"github.com/yourusername/anymux"
)

func main() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
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
	io.Copy(st, st) // 简单回显
}
```

客户端

```go
package main

import (
	"io"
	"log"
	"net"

	"github.com/yourusername/anymux"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		log.Fatal(err)
	}
	sess := anymux.NewClientSession(conn, anymux.DefaultConfig)
	sess.Run()

	st, err := sess.OpenStream()
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	st.Write([]byte("hello"))
	buf := make([]byte, 1024)
	n, _ := st.Read(buf)
	log.Println(string(buf[:n]))
}
```

核心概念

Session

Session 表示一个 TCP 连接上的复用会话，管理所有流，维护协议状态，并负责底层连接的读写。Session 分为客户端和服务器两种角色：

· 客户端 Session：通过 NewClientSession 创建，可以主动打开流。
· 服务器 Session：通过 NewServerSession 创建，当收到对端 SYN 帧时，通过回调通知新流。

Stream

Stream 表示一个独立的双向数据通道，行为类似 net.Conn。一个 Session 可以同时拥有多个 Stream。每个 Stream 拥有自己的缓冲区、读写截止时间和关闭状态。

API 参考

类型

type Config struct

配置参数，均可在创建 Session 时指定。若传入零值，则自动使用 DefaultConfig。

字段 类型 默认值 说明
HandshakeTimeout time.Duration 3s 流握手超时时间（仅 v2+ 协议，且 sid>=2 时生效）
WriteTimeout time.Duration 5s 底层连接写超时时间，每次写帧时设置
MaxFrameSize int 64KB 单帧负载最大长度（1~65535），超出自动截断或分片
StreamBufferSize int 64KB 每个 Stream 接收缓冲区的最大大小

预定义变量：

```go
var DefaultConfig = Config{
	HandshakeTimeout: 3 * time.Second,
	WriteTimeout:     5 * time.Second,
	MaxFrameSize:     64 * 1024,
	StreamBufferSize: 64 * 1024,
}
```

type Session struct

内部结构不导出，通过构造函数创建。

type Stream struct

实现了 io.ReadWriteCloser 接口，并提供类似 net.Conn 的方法。

构造函数

func NewClientSession(conn net.Conn, config Config) *Session

创建客户端会话。客户端可以调用 OpenStream 主动打开流。

```go
sess := anymux.NewClientSession(conn, anymux.DefaultConfig)
```

func NewServerSession(conn net.Conn, onNewStream func(*Stream), config Config) *Session

创建服务器会话。当收到对端 SYN 帧时，调用 onNewStream 回调。回调在新的 goroutine 中执行，应尽快返回，将流交给其他 goroutine 处理。

```go
sess := anymux.NewServerSession(conn, func(st *anymux.Stream) {
	go handleStream(st)
}, anymux.DefaultConfig)
```

Session 方法

func (s *Session) Run()

启动会话的后台接收循环。对于客户端，还会先发送版本协商请求。必须在创建会话后调用一次，否则无法接收数据。该函数非阻塞（接收循环在独立 goroutine 中运行）。

func (s *Session) OpenStream() (*Stream, error)

客户端打开一个新流。发送 SYN 帧，并等待对端确认（v2+ 协议通过 SYNACK，老版本无确认）。返回的 Stream 即可用于读写。若会话已关闭或底层连接错误则返回错误。

注意：仅客户端可以调用，服务器调用会返回错误。

func (s *Session) Close() error

关闭会话。关闭所有流，唤醒所有阻塞的读写，关闭底层连接。可安全多次调用。会等待接收循环退出（若接收循环阻塞在底层读上，可能需要先设置连接截止时间）。

func (s *Session) IsClosed() bool

返回会话是否已关闭。

Stream 方法

func (s *Stream) Read(p []byte) (n int, err error)

读取数据。行为类似 io.Reader，但支持超时（通过 SetReadDeadline）。若缓冲区有数据立即返回；否则阻塞直到数据到达、流关闭或截止时间到期。返回错误可能为 os.ErrDeadlineExceeded 或流关闭原因。

func (s *Stream) Write(p []byte) (int, error)

写入数据。数据会被分帧发送，自动分割成不超过 MaxFrameSize 的块。若写截止时间到期或流关闭则返回错误。注意：写操作不保证原子性，可能部分写入（返回已写字节数和错误）。

func (s *Stream) Close() error

关闭流。发送 FIN 帧通知对端（如果尚未关闭），并释放本地资源。多次调用安全。关闭后读写返回错误。

func (s *Stream) SetReadDeadline(t time.Time) error

设置读截止时间。t 为零值时取消截止。到期后阻塞的 Read 返回 os.ErrDeadlineExceeded。

func (s *Stream) SetWriteDeadline(t time.Time) error

设置写截止时间。到期后 Write 返回 os.ErrDeadlineExceeded。

func (s *Stream) SetDeadline(t time.Time) error

同时设置读写截止时间。

func (s *Stream) HandshakeSuccess() error

用于应用层握手确认。当应用层完成自己的握手后调用，会发送一个空的 SYNACK 帧给对端（仅当协议版本>=2）。多次调用只有第一次生效。

func (s *Stream) HandshakeFailure(err error) error

发送握手失败信息给对端（SYNACK 携带错误消息），仅当协议版本>=2 且 err != nil。多次调用只有第一次生效。

func (s *Stream) LocalAddr() net.Addr

返回底层连接的本地地址，若连接支持 LocalAddr() 方法。

func (s *Stream) RemoteAddr() net.Addr

返回底层连接的远程地址，若连接支持 RemoteAddr() 方法。

协议说明

帧格式

所有帧由 7 字节固定头和可变负载组成：

```
[0]       Command (1 byte)
[1..4]    Stream ID (4 bytes, big-endian, uint32)
[5..6]    Length (2 bytes, big-endian, uint16)
[7..]     Payload (Length bytes)
```

命令类型

值 名称 方向 说明
1 SYN C→S 客户端打开新流
2 PSH 双向 数据帧，承载流数据
3 FIN 双向 关闭流
4 VERSION_REQUEST C→S 版本协商请求，负载为 "v=2\n"
5 ERROR S→C 错误（目前仅用于服务器在未收到版本请求前收到 SYN）
7 SYNACK 双向 握手确认/失败，负载为错误消息或空
10 VERSION_RESPONSE S→C 版本协商响应，负载为 "v=2\n"

版本协商

· 客户端在 Run() 时发送 VERSION_REQUEST。
· 服务器回复 VERSION_RESPONSE。
· 双方记录对端版本。若版本>=2，支持 SYNACK 流握手确认和超时机制。

流建立

· 客户端调用 OpenStream，发送 SYN 帧，SID 从 1 递增。
· 服务器收到 SYN 后创建 Stream 并回调。
· 若双方版本>=2，客户端会等待服务器发送 SYNACK（由应用层调用 HandshakeSuccess 触发），并在超时后关闭会话。

流关闭

· 任何一方调用 Close 会发送 FIN 帧。
· 收到 FIN 帧的一方会关闭本地流，并从 Session 中移除。
· 会话关闭时，所有流被强制关闭。

错误处理

库导出的错误：

· ErrSessionClosed：会话已关闭。
· ErrStreamClosed：流已关闭（保留，实际多返回 net.ErrClosed 或 io.ErrClosedPipe）。
· os.ErrDeadlineExceeded：超时。
· net.ErrClosed：流关闭时读操作返回。
· io.ErrClosedPipe：写操作关闭时返回。

建议使用 errors.Is 进行判断。

并发安全

· Session 和 Stream 的所有方法都是并发安全的。
· OpenStream 可以并发调用。
· Close 可以并发调用。
· 写操作内部使用锁保证不会交织。
· 建议每个 Stream 由单个 goroutine 读写，避免不必要的竞争。

注意事项

1. 必须调用 Run()：创建会话后必须调用 Run() 启动接收循环，否则无法接收任何数据。
2. 服务器 onNewStream 回调不能阻塞：回调在新的 goroutine 中执行，但应该快速返回，将流交给其他 goroutine 处理。
3. 流缓冲区限制：每个流的接收缓冲区最大为 StreamBufferSize，如果对端发送速度超过读取速度，缓冲区满后数据帧会被丢弃（pushData 返回 false）。因此需要及时读取。
4. 写超时全局影响：writeDataFrame 和 writeControlFrame 会修改底层连接的写截止时间，写完后清除。并发写时通过锁串行化。
5. 关闭阻塞：Session.Close() 会等待接收循环退出，如果接收循环因底层连接阻塞而无法退出，Close 会阻塞。建议设置连接截止时间或先关闭连接。
6. 流 ID 管理：流 ID 由客户端分配，从 1 开始递增，使用 uint32 可能回绕，但实际使用中不会出现。

完整示例

完整的 echo 服务器和客户端可参考快速开始部分，将两者分别运行即可测试。


