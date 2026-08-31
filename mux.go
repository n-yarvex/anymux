package mux
import (
"encoding/binary"
"sync"
"time"
)
type rawHeader [7]byte
func (h rawHeader) Cmd() byte   { return h[0] }
func (h rawHeader) Sid() uint32 { return binary.BigEndian.Uint32(h[1:5]) }
func (h rawHeader) Len() uint16 { return binary.BigEndian.Uint16(h[5:7]) }
type frame struct {
cmd  byte
sid  uint32
data []byte
}
func newFrame(cmd byte, sid uint32, data []byte) frame {
return frame{cmd: cmd, sid: sid, data: data}
}
const (
cmdWaste               byte = 0
cmdSYN                 byte = 1
cmdPSH                 byte = 2
cmdFIN                 byte = 3
cmdSettings            byte = 4
cmdAlert               byte = 5
cmdUpdatePaddingScheme byte = 6
cmdSYNACK              byte = 7
cmdHeartRequest        byte = 8
cmdHeartResponse       byte = 9
cmdServerSettings      byte = 10
)
type Config struct {
HandshakeTimeout time.Duration
WriteTimeout     time.Duration
MaxFrameSize     int
StreamBufferSize int
}
func DefaultConfig() Config {
return Config{
HandshakeTimeout: 3 * time.Second,
WriteTimeout:     5 * time.Second,
MaxFrameSize:     64 * 1024,
StreamBufferSize: 64 * 1024,
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
func normalizeConfig(cfg Config) Config {
if cfg.HandshakeTimeout <= 0 {
cfg.HandshakeTimeout = 3 * time.Second
}
if cfg.WriteTimeout <= 0 {
cfg.WriteTimeout = 5 * time.Second
}
if cfg.MaxFrameSize <= 0 {
cfg.MaxFrameSize = 64 * 1024
}
cfg.MaxFrameSize = clamp(cfg.MaxFrameSize, 1, 65535)
if cfg.StreamBufferSize <= 0 {
cfg.StreamBufferSize = 64 * 1024
}
return cfg
}
var framePool = sync.Pool{
New: func() interface{} { return make([]byte, 0, 7+65535) },
}
func getFrameBuf(size int) []byte {
buf := framePool.Get().([]byte)
if cap(buf) < size {
framePool.Put(buf[:0])
buf = make([]byte, size, size*2)
}
return buf[:size]
}
func putFrameBuf(buf []byte) {
framePool.Put(buf[:0])
}
var payloadPool = sync.Pool{
New: func() interface{} { return make([]byte, 0, 65535) },
}
func getPayloadBuf(size int) []byte {
buf := payloadPool.Get().([]byte)
if cap(buf) < size {
payloadPool.Put(buf[:0])
buf = make([]byte, size, size*2)
}
return buf[:size]
}
func putPayloadBuf(buf []byte) {
payloadPool.Put(buf[:0])
}
type PipeDeadline struct {
mu       sync.Mutex
timer    *time.Timer
ch       chan struct{}
deadline time.Time
seq      uint64
}
func NewPipeDeadline() *PipeDeadline {
return &PipeDeadline{ch: make(chan struct{}, 1)}
}
func (d *PipeDeadline) Set(t time.Time) {
d.mu.Lock()
if d.timer != nil {
d.timer.Stop()
d.timer = nil
}
d.seq++
seq := d.seq
d.deadline = t
if t.IsZero() {
d.mu.Unlock()
return
}
dur := time.Until(t)
if dur <= 0 {
d.mu.Unlock()
d.signal(seq)
return
}
d.timer = time.AfterFunc(dur, func() {
d.mu.Lock()
if d.seq != seq {
d.mu.Unlock()
return
}
d.timer = nil
d.mu.Unlock()
d.signal(seq)
})
d.mu.Unlock()
}
func (d *PipeDeadline) signal(seq uint64) {
d.mu.Lock()
if d.seq != seq {
d.mu.Unlock()
return
}
d.mu.Unlock()
select {
case d.ch <- struct{}{}:
default:
}
}
func (d *PipeDeadline) Wait() <-chan struct{} {
return d.ch
}
func (d *PipeDeadline) Expired() bool {
d.mu.Lock()
defer d.mu.Unlock()
return !d.deadline.IsZero() && time.Now().After(d.deadline)
}