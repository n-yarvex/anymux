package mux
import (
"bytes"
"encoding/binary"
"errors"
"io"
"net"
"os"
"sync"
"sync/atomic"
"time"
)
var (
ErrSessionClosed = errors.New("mux: session closed")
ErrStreamClosed  = errors.New("mux: stream closed")
)
type Session struct {
conn         net.Conn
config       Config
isClient     bool
die          chan struct{}
dieOnce      sync.Once
recvExit     chan struct{}
recvStarted  chan struct{}
runOnce      sync.Once
streams      map[uint32]*Stream
streamsMu    sync.RWMutex
nextStreamID uint32
peerVersion  atomic.Uint32
onNewStream  func(*Stream)
connLock     sync.Mutex
writeTimeout time.Duration
versionCh    chan struct{}
versionOnce  sync.Once
}
func NewClientSession(conn net.Conn, cfg Config) *Session {
cfg = normalizeConfig(cfg)
return &Session{
conn:         conn,
config:       cfg,
isClient:     true,
die:          make(chan struct{}),
recvExit:     make(chan struct{}),
recvStarted:  make(chan struct{}),
streams:      make(map[uint32]*Stream),
writeTimeout: cfg.WriteTimeout,
versionCh:    make(chan struct{}),
}
}
func NewServerSession(conn net.Conn, onNewStream func(*Stream), cfg Config) *Session {
cfg = normalizeConfig(cfg)
return &Session{
conn:         conn,
config:       cfg,
isClient:     false,
die:          make(chan struct{}),
recvExit:     make(chan struct{}),
recvStarted:  make(chan struct{}),
streams:      make(map[uint32]*Stream),
onNewStream:  onNewStream,
writeTimeout: cfg.WriteTimeout,
versionCh:    make(chan struct{}),
}
}
func (s *Session) Run() {
s.runOnce.Do(func() {
go s.recvLoop()
close(s.recvStarted)
if s.isClient {
if err := s.writeFrame(newFrame(cmdSettings, 0, []byte("v=2\n"))); err != nil {
s.Close()
}
}
})
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
return nil
}
s.streamsMu.Lock()
for _, st := range s.streams {
st.closeLocked(ErrSessionClosed, false)
}
s.streams = make(map[uint32]*Stream)
s.streamsMu.Unlock()
s.conn.SetDeadline(time.Now())
select {
case <-s.recvStarted:
select {
case <-s.recvExit:
case <-time.After(100 * time.Millisecond):
}
default:
}
return s.conn.Close()
}
func (s *Session) OpenStream() (*Stream, error) {
if !s.isClient {
return nil, errors.New("mux: server cannot open stream")
}
if s.IsClosed() {
return nil, ErrSessionClosed
}
if s.peerVersion.Load() < 2 {
timer := time.NewTimer(s.config.HandshakeTimeout)
defer timer.Stop()
select {
case <-s.versionCh:
case <-s.die:
return nil, ErrSessionClosed
case <-timer.C:
return nil, errors.New("mux: version negotiation timeout")
}
}
sid := atomic.AddUint32(&s.nextStreamID, 1)
st := newStream(sid, s)
s.streamsMu.Lock()
if s.IsClosed() {
s.streamsMu.Unlock()
return nil, ErrSessionClosed
}
s.streams[sid] = st
s.streamsMu.Unlock()
if err := s.writeFrame(newFrame(cmdSYN, sid, nil)); err != nil {
s.streamsMu.Lock()
delete(s.streams, sid)
s.streamsMu.Unlock()
st.closeLocked(err, false)
return nil, err
}
return st, nil
}
func (s *Session) writeDataFrame(sid uint32, data []byte, deadline *PipeDeadline) (int, error) {
if len(data) == 0 {
return 0, nil
}
max := s.config.MaxFrameSize
if max <= 0 {
max = 64 * 1024
}
total := len(data)
written := 0
for written < total {
if deadline != nil && deadline.Expired() {
return written, os.ErrDeadlineExceeded
}
chunk := data[written:]
if len(chunk) > max {
chunk = chunk[:max]
}
f := newFrame(cmdPSH, sid, chunk)
if err := s.writeFrame(f); err != nil {
return written, err
}
written += len(chunk)
}
return written, nil
}
func (s *Session) writeFrame(f frame) error {
if len(f.data) > 65535 {
f.data = f.data[:65535]
}
length := len(f.data)
total := 7 + length
buf := getFrameBuf(total)
buf[0] = f.cmd
binary.BigEndian.PutUint32(buf[1:5], f.sid)
binary.BigEndian.PutUint16(buf[5:7], uint16(length))
if length > 0 {
copy(buf[7:], f.data)
}
s.connLock.Lock()
if s.writeTimeout > 0 {
s.conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
}
n, err := s.conn.Write(buf)
if s.writeTimeout > 0 {
s.conn.SetWriteDeadline(time.Time{})
}
s.connLock.Unlock()
putFrameBuf(buf)
if n < len(buf) {
s.Close()
if err == nil {
err = io.ErrShortWrite
}
}
return err
}
func (s *Session) streamClosed(sid uint32) {
s.streamsMu.Lock()
delete(s.streams, sid)
s.streamsMu.Unlock()
}
func (s *Session) recvLoop() {
defer func() {
close(s.recvExit)
s.Close()
}()
var hdr rawHeader
settingsReceived := false
for {
if s.IsClosed() {
return
}
_, err := io.ReadFull(s.conn, hdr[:])
if err != nil {
return
}
length := int(hdr.Len())
if length > s.config.MaxFrameSize {
if _, err := io.CopyN(io.Discard, s.conn, int64(length)); err != nil {
return
}
continue
}
var payload []byte
if length > 0 {
payload = getPayloadBuf(length)
if _, err := io.ReadFull(s.conn, payload); err != nil {
putPayloadBuf(payload)
return
}
}
cmd := hdr.Cmd()
sid := hdr.Sid()
switch cmd {
case cmdWaste:
case cmdPSH:
if length == 0 {
break
}
s.streamsMu.RLock()
st, ok := s.streams[sid]
s.streamsMu.RUnlock()
if ok {
if !st.pushData(payload) {
select {
case <-st.closeCh:
default:
st.closeLocked(errors.New("mux: receive buffer overflow"), true)
}
}
}
case cmdSYN:
if !s.isClient && !settingsReceived {
if err := s.writeFrame(newFrame(cmdAlert, 0, []byte("settings missing"))); err != nil {
putPayloadBuf(payload)
return
}
putPayloadBuf(payload)
return
}
s.streamsMu.Lock()
if _, ok := s.streams[sid]; !ok {
st := newStream(sid, s)
s.streams[sid] = st
if s.onNewStream != nil {
go s.onNewStream(st)
} else {
delete(s.streams, sid)
st.closeLocked(io.EOF, false)
go s.writeFrame(newFrame(cmdSYNACK, sid, []byte("stream rejected")))
go s.writeFrame(newFrame(cmdFIN, sid, nil))
}
}
s.streamsMu.Unlock()
case cmdSYNACK:
s.streamsMu.RLock()
st, ok := s.streams[sid]
s.streamsMu.RUnlock()
if ok {
st.stopHandshakeTimer()
if length > 0 {
hErr := errors.New("mux: " + string(payload))
st.setHandshakeError(hErr)
st.closeLocked(hErr, false)
s.streamClosed(sid)
}
}
case cmdFIN:
s.streamsMu.Lock()
st, ok := s.streams[sid]
if ok {
delete(s.streams, sid)
}
s.streamsMu.Unlock()
if ok {
st.closeLocked(io.EOF, false)
}
case cmdSettings:
if !s.isClient {
settingsReceived = true
m := parseMap(payload)
if v, ok := m["v"]; ok && v == "2" {
s.peerVersion.Store(2)
if err := s.writeFrame(newFrame(cmdServerSettings, 0, []byte("v=2\n"))); err != nil {
putPayloadBuf(payload)
return
}
} else {
if err := s.writeFrame(newFrame(cmdAlert, 0, []byte("unsupported version"))); err != nil {
putPayloadBuf(payload)
return
}
putPayloadBuf(payload)
return
}
}
case cmdServerSettings:
if s.isClient {
m := parseMap(payload)
if v, ok := m["v"]; ok && v == "2" {
s.peerVersion.Store(2)
s.versionOnce.Do(func() {
close(s.versionCh)
})
s.streamsMu.RLock()
for _, st := range s.streams {
if st.id >= 1 {
st.setHandshakeTimer(s.config.HandshakeTimeout)
}
}
s.streamsMu.RUnlock()
} else {
if err := s.writeFrame(newFrame(cmdAlert, 0, []byte("unsupported version"))); err != nil {
putPayloadBuf(payload)
return
}
putPayloadBuf(payload)
return
}
}
case cmdAlert:
putPayloadBuf(payload)
return
case cmdHeartRequest:
if err := s.writeFrame(newFrame(cmdHeartResponse, sid, nil)); err != nil {
putPayloadBuf(payload)
return
}
case cmdHeartResponse:
default:
}
if payload != nil {
putPayloadBuf(payload)
}
}
}
func parseMap(b []byte) map[string]string {
m := make(map[string]string)
for _, line := range bytes.Split(b, []byte("\n")) {
line = bytes.TrimSpace(line)
if len(line) == 0 {
continue
}
kv := bytes.SplitN(line, []byte("="), 2)
if len(kv) == 2 {
m[string(bytes.TrimSpace(kv[0]))] = string(bytes.TrimSpace(kv[1]))
}
}
return m
}