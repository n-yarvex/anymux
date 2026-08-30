package mux

import (
	"errors"
	"time"
)

const (
	cmdSYN             = 1
	cmdPSH             = 2
	cmdFIN             = 3
	cmdVersionRequest  = 4
	cmdError           = 5
	cmdSYNACK          = 7
	cmdVersionResponse = 10
)

type rawHeader [7]byte

func (h rawHeader) Cmd() byte   { return h[0] }
func (h rawHeader) Sid() uint32 {
	return uint32(h[1])<<24 | uint32(h[2])<<16 | uint32(h[3])<<8 | uint32(h[4])
}
func (h rawHeader) Len() uint16 { return uint16(h[5])<<8 | uint16(h[6]) }

type Config struct {
	HandshakeTimeout time.Duration
	WriteTimeout     time.Duration
	MaxFrameSize     int
	StreamBufferSize int
}

var DefaultConfig = Config{
	HandshakeTimeout: 3 * time.Second,
	WriteTimeout:     5 * time.Second,
	MaxFrameSize:     64 * 1024,
	StreamBufferSize: 64 * 1024,
}

var (
	ErrSessionClosed = errors.New("mux: session closed")
	ErrStreamClosed  = errors.New("mux: stream closed")
)

type frame struct {
	cmd  byte
	sid  uint32
	data []byte
}

func newFrame(cmd byte, sid uint32) frame { return frame{cmd: cmd, sid: sid} }