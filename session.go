package mux

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Session struct {
	conn           net.Conn
	connLock       sync.Mutex
	streams        map[uint32]*Stream
	streamId       uint32
	streamLock     sync.RWMutex
	dieOnce        sync.Once
	die            chan struct{}
	dieHook        func()
	synDone        func()
	synDoneLock    sync.Mutex
	peerVersion    byte
	isClient       bool
	onNewStream    func(*Stream)
	config         Config
	recvLoopExited chan struct{}
}

func NewClientSession(conn net.Conn, config Config) *Session {
	if config == (Config{}) {
		config = DefaultConfig
	}
	config.MaxFrameSize = clamp(config.MaxFrameSize, 1, 65535)
	if config.StreamBufferSize <= 0 {
		config.StreamBufferSize = 64 * 1024
	}
	return &Session{
		conn:           conn,
		isClient:       true,
		config:         config,
		die:            make(chan struct{}),
		streams:        make(map[uint32]*Stream),
		recvLoopExited: make(chan struct{}),
	}
}

func NewServerSession(conn net.Conn, onNewStream func(*Stream), config Config) *Session {
	if config == (Config{}) {
		config = DefaultConfig
	}
	config.MaxFrameSize = clamp(config.MaxFrameSize, 1, 65535)
	if config.StreamBufferSize <= 0 {
		config.StreamBufferSize = 64 * 1024
	}
	return &Session{
		conn:           conn,
		onNewStream:    onNewStream,
		config:         config,
		die:            make(chan struct{}),
		streams:        make(map[uint32]*Stream),
		recvLoopExited: make(chan struct{}),
	}
}

func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func (s *Session) Run() {
	if !s.isClient {
		go s.recvLoop()
		return
	}
	f := newFrame(cmdVersionRequest, 0)
	f.data = []byte("v=2\n")
	s.writeControlFrame(f)
	go s.recvLoop()
}

func (s *Session) IsClosed() bool {
	select {
	case <-s.die:
		return true
	default:
		return false
	}
}

func (s *Session) Close() error {
	var done bool
	s.dieOnce.Do(func() {
		close(s.die)
		done = true
	})
	if !done {
		return ErrSessionClosed
	}
	if s.dieHook != nil {
		s.dieHook()
		s.dieHook = nil
	}
	s.streamLock.Lock()
	for _, st := range s.streams {
		st.closeLocally()
	}
	s.streams = make(map[uint32]*Stream)
	s.streamLock.Unlock()
	s.conn.SetDeadline(time.Now())
	<-s.recvLoopExited
	return s.conn.Close()
}

func (s *Session) OpenStream() (*Stream, error) {
	if !s.isClient {
		return nil, errors.New("mux: only client can open stream")
	}
	if s.IsClosed() {
		return nil, ErrSessionClosed
	}
	sid := atomic.AddUint32(&s.streamId, 1)
	stream := newStream(sid, s)
	s.streamLock.Lock()
	if s.IsClosed() {
		s.streamLock.Unlock()
		return nil, ErrSessionClosed
	}
	s.streams[sid] = stream
	s.streamLock.Unlock()
	if _, err := s.writeControlFrame(newFrame(cmdSYN, sid)); err != nil {
		s.streamLock.Lock()
		delete(s.streams, sid)
		s.streamLock.Unlock()
		stream.closeLocally()
		return nil, err
	}
	if sid >= 2 && s.peerVersion >= 2 {
		s.synDoneLock.Lock()
		if s.synDone != nil {
			s.synDone()
		}
		s.synDone = newDeadlineWatcher(s.config.HandshakeTimeout, func() { s.Close() })
		s.synDoneLock.Unlock()
	}
	return stream, nil
}

func (s *Session) recvLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("mux: recvLoop panic: %v", r)
		}
		close(s.recvLoopExited)
		s.Close()
	}()
	var hdr rawHeader
	var settingsReceived bool
	for {
		if s.IsClosed() {
			return
		}
		_, err := io.ReadFull(s.conn, hdr[:])
		if err != nil {
			return
		}
		sid := hdr.Sid()
		length := int(hdr.Len())
		if length > s.config.MaxFrameSize {
			io.CopyN(io.Discard, s.conn, int64(length))
			continue
		}
		switch hdr.Cmd() {
		case cmdPSH:
			if length > 0 {
				buf := make([]byte, length)
				if _, err := io.ReadFull(s.conn, buf); err != nil {
					return
				}
				s.streamLock.RLock()
				st, ok := s.streams[sid]
				s.streamLock.RUnlock()
				if ok {
					_ = st.pushData(buf)
				}
			}
		case cmdSYN:
			if !s.isClient && !settingsReceived {
				f := newFrame(cmdError, 0)
				f.data = []byte("settings missing")
				s.writeControlFrame(f)
				return
			}
			s.streamLock.Lock()
			if _, ok := s.streams[sid]; !ok {
				st := newStream(sid, s)
				s.streams[sid] = st
				if s.onNewStream != nil {
					go s.onNewStream(st)
				} else {
					st.Close()
				}
			}
			s.streamLock.Unlock()
		case cmdSYNACK:
			s.synDoneLock.Lock()
			if s.synDone != nil {
				s.synDone()
				s.synDone = nil
			}
			s.synDoneLock.Unlock()
			if length > 0 {
				buf := make([]byte, length)
				if _, err := io.ReadFull(s.conn, buf); err != nil {
					return
				}
				s.streamLock.RLock()
				st, ok := s.streams[sid]
				s.streamLock.RUnlock()
				if ok {
					st.closeWithError(&streamError{msg: string(buf)}, false)
				}
			}
		case cmdFIN:
			s.streamLock.Lock()
			st, ok := s.streams[sid]
			delete(s.streams, sid)
			s.streamLock.Unlock()
			if ok {
				st.closeLocally()
			}
		case cmdVersionRequest:
			if length > 0 {
				buf := make([]byte, length)
				if _, err := io.ReadFull(s.conn, buf); err != nil {
					return
				}
				if !s.isClient {
					settingsReceived = true
					m := parseStringMap(buf)
					if v, _ := strconv.Atoi(m["v"]); v >= 2 {
						s.peerVersion = byte(v)
						f := newFrame(cmdVersionResponse, 0)
						f.data = []byte("v=2\n")
						s.writeControlFrame(f)
					}
				}
			}
		case cmdVersionResponse:
			if length > 0 {
				buf := make([]byte, length)
				if _, err := io.ReadFull(s.conn, buf); err != nil {
					return
				}
				if s.isClient {
					m := parseStringMap(buf)
					if v, _ := strconv.Atoi(m["v"]); v >= 2 {
						s.peerVersion = byte(v)
					}
				}
			}
		default:
			if length > 0 {
				io.CopyN(io.Discard, s.conn, int64(length))
			}
		}
	}
}

func (s *Session) streamClosed(sid uint32) error {
	if s.IsClosed() {
		return ErrSessionClosed
	}
	s.streamLock.Lock()
	_, ok := s.streams[sid]
	if !ok {
		s.streamLock.Unlock()
		return nil
	}
	delete(s.streams, sid)
	s.streamLock.Unlock()
	_, err := s.writeControlFrame(newFrame(cmdFIN, sid))
	return err
}

func (s *Session) writeDataFrame(sid uint32, data []byte) (int, error) {
	total := len(data)
	if total == 0 {
		return 0, nil
	}
	maxPayload := s.config.MaxFrameSize
	written := 0
	for written < total {
		chunkSize := total - written
		if chunkSize > maxPayload {
			chunkSize = maxPayload
		}
		chunk := data[written : written+chunkSize]
		buf := make([]byte, 7+chunkSize)
		buf[0] = cmdPSH
		binary.BigEndian.PutUint32(buf[1:5], sid)
		binary.BigEndian.PutUint16(buf[5:7], uint16(chunkSize))
		copy(buf[7:], chunk)
		s.conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
		if _, err := s.writeConn(buf); err != nil {
			s.conn.SetWriteDeadline(time.Time{})
			return written, err
		}
		written += chunkSize
	}
	s.conn.SetWriteDeadline(time.Time{})
	return written, nil
}

func (s *Session) writeControlFrame(f frame) (int, error) {
	ln := len(f.data)
	if ln > 65535 {
		ln = 65535
		f.data = f.data[:ln]
	}
	buf := make([]byte, 7+ln)
	buf[0] = f.cmd
	binary.BigEndian.PutUint32(buf[1:5], f.sid)
	binary.BigEndian.PutUint16(buf[5:7], uint16(ln))
	copy(buf[7:], f.data)
	s.conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	_, err := s.writeConn(buf)
	s.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		s.Close()
	}
	return ln, err
}

func (s *Session) writeConn(b []byte) (int, error) {
	s.connLock.Lock()
	defer s.connLock.Unlock()
	return s.conn.Write(b)
}

func parseStringMap(b []byte) map[string]string {
	m := make(map[string]string)
	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		kv := strings.SplitN(line, "=", 2)
		if len(kv) == 2 {
			m[kv[0]] = kv[1]
		}
	}
	return m
}

func newDeadlineWatcher(d time.Duration, onTimeout func()) func() {
	t := time.NewTimer(d)
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			t.Stop()
		case <-t.C:
			onTimeout()
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

type streamError struct{ msg string }

func (e *streamError) Error() string { return e.msg }