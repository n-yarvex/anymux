package mux

import (
	"io"
	"net"
	"os"
	"sync"
	"time"
)

type Stream struct {
	id            uint32
	sess          *Session
	closeCh       chan struct{}
	closeOnce     sync.Once
	closeErr      error
	handshakeOnce sync.Once
	readDeadline  PipeDeadline
	writeDeadline PipeDeadline
	bufMu         sync.Mutex
	buf           []byte
	wakeCh        chan struct{}
	maxBufSize    int
}

func newStream(id uint32, sess *Session) *Stream {
	s := &Stream{
		id:         id,
		sess:       sess,
		closeCh:    make(chan struct{}),
		wakeCh:     make(chan struct{}, 1),
		maxBufSize: sess.config.StreamBufferSize,
	}
	s.readDeadline = MakePipeDeadline()
	s.writeDeadline = MakePipeDeadline()
	return s
}

func (s *Stream) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	s.bufMu.Lock()
	defer s.bufMu.Unlock()
	for {
		if len(s.buf) > 0 {
			n = copy(b, s.buf)
			s.buf = s.buf[n:]
			return n, nil
		}
		select {
		case <-s.readDeadline.Wait():
			return 0, os.ErrDeadlineExceeded
		case <-s.closeCh:
			return 0, s.closeErr
		default:
		}
		s.bufMu.Unlock()
		select {
		case <-s.wakeCh:
		case <-s.readDeadline.Wait():
			s.bufMu.Lock()
			return 0, os.ErrDeadlineExceeded
		case <-s.closeCh:
			s.bufMu.Lock()
			return 0, s.closeErr
		}
		s.bufMu.Lock()
	}
}

func (s *Stream) Write(b []byte) (int, error) {
	select {
	case <-s.writeDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	case <-s.closeCh:
		return 0, s.closeErr
	default:
	}
	if s.sess == nil {
		return 0, io.ErrClosedPipe
	}
	return s.sess.writeDataFrame(s.id, b)
}

func (s *Stream) Close() error {
	return s.closeWithError(io.ErrClosedPipe, true)
}

func (s *Stream) closeLocally() {
	s.closeOnce.Do(func() {
		s.closeErr = net.ErrClosed
		close(s.closeCh)
	})
}

func (s *Stream) closeWithError(err error, sendFIN bool) error {
	s.closeOnce.Do(func() {
		s.closeErr = err
		close(s.closeCh)
	})
	if sendFIN && s.closeErr != nil {
		_ = s.sess.streamClosed(s.id)
	}
	return s.closeErr
}

func (s *Stream) pushData(data []byte) bool {
	s.bufMu.Lock()
	if len(s.buf)+len(data) > s.maxBufSize {
		s.bufMu.Unlock()
		return false
	}
	s.buf = append(s.buf, data...)
	s.bufMu.Unlock()
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
	return true
}

func (s *Stream) SetReadDeadline(t time.Time) error {
	s.readDeadline.Set(t)
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *Stream) SetWriteDeadline(t time.Time) error { s.writeDeadline.Set(t); return nil }
func (s *Stream) SetDeadline(t time.Time) error       { s.SetReadDeadline(t); s.SetWriteDeadline(t); return nil }

func (s *Stream) LocalAddr() net.Addr {
	if c, ok := s.sess.conn.(interface{ LocalAddr() net.Addr }); ok {
		return c.LocalAddr()
	}
	return nil
}

func (s *Stream) RemoteAddr() net.Addr {
	if c, ok := s.sess.conn.(interface{ RemoteAddr() net.Addr }); ok {
		return c.RemoteAddr()
	}
	return nil
}

func (s *Stream) HandshakeSuccess() error {
	var once bool
	s.handshakeOnce.Do(func() { once = true })
	if once && s.sess.peerVersion >= 2 {
		_, err := s.sess.writeControlFrame(newFrame(cmdSYNACK, s.id))
		return err
	}
	return nil
}

func (s *Stream) HandshakeFailure(err error) error {
	var once bool
	s.handshakeOnce.Do(func() { once = true })
	if once && err != nil && s.sess.peerVersion >= 2 {
		f := newFrame(cmdSYNACK, s.id)
		f.data = []byte(err.Error())
		_, err := s.sess.writeControlFrame(f)
		return err
	}
	return nil
}

type PipeDeadline struct {
	mu     sync.Mutex
	timer  *time.Timer
	cancel chan struct{}
}

func MakePipeDeadline() PipeDeadline {
	return PipeDeadline{cancel: make(chan struct{})}
}

func (d *PipeDeadline) Set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel
	}
	d.timer = nil
	closed := isClosedChan(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}
	if dur := time.Until(t); dur > 0 {
		if closed {
			d.cancel = make(chan struct{})
		}
		d.timer = time.AfterFunc(dur, func() {
			close(d.cancel)
		})
		return
	}
	if !closed {
		close(d.cancel)
	}
}

func (d *PipeDeadline) Wait() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}