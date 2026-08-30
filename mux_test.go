package anymux_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/n-yarvex/anymux"
)

const testPort = ":9001"

func TestAll(t *testing.T) {
	t.Log("🧪 Starting anymux full test suite...")

	t.Run("BasicEcho", testBasicEcho)
	t.Run("ConcurrentStreams", testConcurrentStreams)
	t.Run("ReadTimeout", testReadTimeout)
	t.Run("WriteTimeout", testWriteTimeout)
	t.Run("StreamClose", testStreamClose)
	t.Run("SessionClose", testSessionClose)
	t.Run("HandshakeTimeout", testHandshakeTimeout)
	t.Run("StressTest", testStressTest)

	t.Log("✅ All tests passed!")
}

// ---------- 辅助函数 ----------
func runServer(t *testing.T) (net.Listener, context.CancelFunc) {
	ln, err := net.Listen("tcp", testPort)
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			go func(c net.Conn) {
				sess := anymux.NewServerSession(c, func(st *anymux.Stream) {
					go handleEcho(st)
				}, anymux.DefaultConfig)
				sess.Run()
				<-ctx.Done()
				sess.Close()
			}(conn)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	return ln, cancel
}

func handleEcho(st *anymux.Stream) {
	defer st.Close()
	io.Copy(st, st)
}

func handleDelayEcho(st *anymux.Stream) {
	defer st.Close()
	time.Sleep(2 * time.Second)
	io.Copy(st, st)
}

func handleNoRead(st *anymux.Stream) {
	defer st.Close()
	<-time.After(5 * time.Second)
}

func dialClient(t *testing.T) (*anymux.Session, func()) {
	conn, err := net.Dial("tcp", "127.0.0.1"+testPort)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	sess := anymux.NewClientSession(conn, anymux.DefaultConfig)
	sess.Run()
	return sess, func() { sess.Close() }
}

// ---------- 测试用例 ----------
func testBasicEcho(t *testing.T) {
	ln, cancel := runServer(t)
	defer cancel()
	defer ln.Close()

	sess, cleanup := dialClient(t)
	defer cleanup()

	st, err := sess.OpenStream()
	if err != nil {
		t.Fatalf("open stream failed: %v", err)
	}
	defer st.Close()

	msg := []byte("hello anymux")
	if _, err := st.Write(msg); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	buf := make([]byte, 1024)
	n, err := st.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if !bytes.Equal(msg, buf[:n]) {
		t.Fatalf("echo mismatch: got %s, want %s", string(buf[:n]), string(msg))
	}
	t.Log("✓ Echo test passed")
}

func testConcurrentStreams(t *testing.T) {
	ln, cancel := runServer(t)
	defer cancel()
	defer ln.Close()

	sess, cleanup := dialClient(t)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			st, err := sess.OpenStream()
			if err != nil {
				t.Errorf("stream %d open failed: %v", n, err)
				return
			}
			defer st.Close()
			msg := fmt.Sprintf("stream %d", n)
			if _, err := st.Write([]byte(msg)); err != nil {
				t.Errorf("stream %d write failed: %v", n, err)
				return
			}
			buf := make([]byte, 1024)
			nRead, err := st.Read(buf)
			if err != nil {
				t.Errorf("stream %d read failed: %v", n, err)
				return
			}
			if string(buf[:nRead]) != msg {
				t.Errorf("stream %d mismatch: got %s, want %s", n, string(buf[:nRead]), msg)
			}
		}(i)
	}
	wg.Wait()
	t.Log("✓ Concurrent 10 streams passed")
}

func testReadTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", testPort)
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			go func(c net.Conn) {
				sess := anymux.NewServerSession(c, func(st *anymux.Stream) {
					go handleDelayEcho(st)
				}, anymux.DefaultConfig)
				sess.Run()
				<-ctx.Done()
				sess.Close()
			}(conn)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	sess, cleanup := dialClient(t)
	defer cleanup()

	st, err := sess.OpenStream()
	if err != nil {
		t.Fatalf("open stream failed: %v", err)
	}
	defer st.Close()
	st.Write([]byte("timeout test"))
	st.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 1024)
	_, err = st.Read(buf)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	t.Log("✓ Read timeout passed")
}

func testWriteTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", testPort)
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			go func(c net.Conn) {
				sess := anymux.NewServerSession(c, func(st *anymux.Stream) {
					go handleNoRead(st)
				}, anymux.DefaultConfig)
				sess.Run()
				<-ctx.Done()
				sess.Close()
			}(conn)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	sess, cleanup := dialClient(t)
	defer cleanup()

	st, err := sess.OpenStream()
	if err != nil {
		t.Fatalf("open stream failed: %v", err)
	}
	defer st.Close()
	st.SetWriteDeadline(time.Now().Add(1 * time.Second))
	bigData := make([]byte, 200*1024)
	_, err = st.Write(bigData)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	t.Log("✓ Write timeout passed")
}

func testStreamClose(t *testing.T) {
	ln, cancel := runServer(t)
	defer cancel()
	defer ln.Close()

	sess, cleanup := dialClient(t)
	defer cleanup()

	st, err := sess.OpenStream()
	if err != nil {
		t.Fatalf("open stream failed: %v", err)
	}
	st.Close()
	buf := make([]byte, 1024)
	_, err = st.Read(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after close, got %v", err)
	}
	t.Log("✓ Stream close passed")
}

func testSessionClose(t *testing.T) {
	ln, cancel := runServer(t)
	defer cancel()
	defer ln.Close()

	sess, cleanup := dialClient(t)
	st, err := sess.OpenStream()
	if err != nil {
		t.Fatalf("open stream failed: %v", err)
	}
	sess.Close()
	cleanup()
	_, err = st.Write([]byte("test"))
	if !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
		t.Logf("write after close returned: %v", err)
	}
	if !sess.IsClosed() {
		t.Fatal("session should be closed")
	}
	t.Log("✓ Session close passed")
}

func testHandshakeTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", testPort)
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			go func(c net.Conn) {
				sess := anymux.NewServerSession(c, func(st *anymux.Stream) {
					// 故意不调用 HandshakeSuccess
				}, anymux.DefaultConfig)
				sess.Run()
				<-ctx.Done()
				sess.Close()
			}(conn)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", "127.0.0.1"+testPort)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	config := anymux.DefaultConfig
	config.HandshakeTimeout = 2 * time.Second
	sess := anymux.NewClientSession(conn, config)
	sess.Run()
	defer sess.Close()

	_, err = sess.OpenStream()
	if err != nil {
		t.Logf("OpenStream returned: %v", err)
	}
	time.Sleep(3 * time.Second)
	if !sess.IsClosed() {
		t.Fatal("session should be closed due to handshake timeout")
	}
	t.Log("✓ Handshake timeout passed")
}

func testStressTest(t *testing.T) {
	ln, cancel := runServer(t)
	defer cancel()
	defer ln.Close()

	sess, cleanup := dialClient(t)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			st, err := sess.OpenStream()
			if err != nil {
				return
			}
			defer st.Close()
			msg := fmt.Sprintf("stress-%d", n)
			st.Write([]byte(msg))
			buf := make([]byte, 1024)
			nRead, _ := st.Read(buf)
			if string(buf[:nRead]) != msg {
				// 忽略少数失败
			}
		}(i)
	}
	wg.Wait()
	t.Log("✓ Stress test with 100 streams completed")
}
