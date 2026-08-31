package mux
import (
"errors"
"io"
"net"
"os"
"sync"
"sync/atomic"
"time"
)
var _ io.ReadWriteCloser = (*Stream)(nil)
type Stream struct {
id                uint32
sess              *Session
closeCh           chan struct{}
closeOnce         sync.Once
closeErr          error
readMu            sync.Mutex
readBuf           []byte
readCond          *sync.Cond
writeMu           sync.Mutex
handshakeOnce     sync.Once
handshakeCh       chan struct{}
handshakeChanOnce sync.Once
handshakeErr      error
handshakeErrMu    sync.RWMutex
readDeadline      *PipeDeadline
writeDeadline     *PipeDeadline
maxBufSize        int
timerMu           sync.Mutex
handshakeTimer    *time.Timer
handshakeDone     uint32
}
func newStream(id uint32, sess *Session) *Stream {
s := &Stream{
id:            id,
sess:          sess,
closeCh:       make(chan struct{}),
handshakeCh:   make(chan struct{}),
maxBufSize:    sess.config.StreamBufferSize,
readDeadline:  NewPipeDeadline(),
writeDeadline: NewPipeDeadline(),
}
s.readCond = sync.NewCond(&s.readMu)
go s.watchReadDeadline()
if sess.isClient && sess.peerVersion.Load() >= 2 && id >= 1 {
s.setHandshakeTimer(sess.config.HandshakeTimeout)
}
return s
}
func (s *Stream) watchReadDeadline() {
for {
select {
case <-s.closeCh:
return
case <-s.readDeadline.Wait():
s.readMu.Lock()
s.readCond.Broadcast()
s.readMu.Unlock()
}
}
}
func (s *Stream) setHandshakeTimer(d time.Duration) {
s.timerMu.Lock()
defer s.timerMu.Unlock()
if atomic.LoadUint32(&s.handshakeDone) == 1 {
return
}
if s.handshakeTimer != nil {
s.handshakeTimer.Stop()
}
s.handshakeTimer = time.AfterFunc(d, func() {
s.sess.Close()
})
}
func (s *Stream) stopHandshakeTimer() {
s.timerMu.Lock()
if s.handshakeTimer != nil {
s.handshakeTimer.Stop()
s.handshakeTimer = nil
}
atomic.StoreUint32(&s.handshakeDone, 1)
s.timerMu.Unlock()
s.handshakeChanOnce.Do(func() {
close(s.handshakeCh)
})
}
func (s *Stream) HandshakeDone() <-chan struct{} {
return s.handshakeCh
}
func (s *Stream) HandshakeError() error {
s.handshakeErrMu.RLock()
defer s.handshakeErrMu.RUnlock()
return s.handshakeErr
}
func (s *Stream) WaitHandshake() error {
<-s.handshakeCh
return s.HandshakeError()
}
func (s *Stream) setHandshakeError(err error) {
s.handshakeErrMu.Lock()
s.handshakeErr = err
s.handshakeErrMu.Unlock()
}
func (s *Stream) Read(b []byte) (int, error) {
if len(b) == 0 {
return 0, nil
}
s.readMu.Lock()
defer s.readMu.Unlock()
for {
if len(s.readBuf) > 0 {
n := copy(b, s.readBuf)
s.readBuf = s.readBuf[n:]
if len(s.readBuf) == 0 {
s.readBuf = nil
}
return n, nil
}
if s.readDeadline.Expired() {
return 0, os.ErrDeadlineExceeded
}
select {
case <-s.closeCh:
return 0, s.closeErr
default:
}
s.readCond.Wait()
}
}
func (s *Stream) Write(b []byte) (int, error) {
if len(b) == 0 {
return 0, nil
}
s.writeMu.Lock()
defer s.writeMu.Unlock()
if s.writeDeadline.Expired() {
return 0, os.ErrDeadlineExceeded
}
select {
case <-s.closeCh:
return 0, s.closeErr
default:
}
remaining := len(b)
written := 0
for remaining > 0 {
if s.writeDeadline.Expired() {
return written, os.ErrDeadlineExceeded
}
select {
case <-s.closeCh:
return written, s.closeErr
default:
}
chunkSize := remaining
if chunkSize > s.sess.config.MaxFrameSize {
chunkSize = s.sess.config.MaxFrameSize
}
n, err := s.sess.writeDataFrame(s.id, b[written:written+chunkSize], s.writeDeadline)
if err != nil {
s.closeLocked(err, false)
s.sess.Close()
return written + n, err
}
if n < chunkSize {
return written + n, io.ErrShortWrite
}
written += n
remaining -= n
}
return written, nil
}
func (s *Stream) Close() error {
s.closeLocked(io.ErrClosedPipe, true)
return nil
}
func (s *Stream) closeLocked(err error, sendFIN bool) {
s.closeOnce.Do(func() {
s.closeErr = err
close(s.closeCh)
s.readMu.Lock()
s.readCond.Broadcast()
s.readMu.Unlock()
s.handshakeErrMu.Lock()
if s.handshakeErr == nil {
select {
case <-s.handshakeCh:
default:
s.handshakeErr = ErrStreamClosed
}
}
s.handshakeErrMu.Unlock()
s.stopHandshakeTimer()
if sendFIN && !s.sess.IsClosed() {
go s.sess.writeFrame(newFrame(cmdFIN, s.id, nil))
s.sess.streamClosed(s.id)
}
})
}
func (s *Stream) pushData(data []byte) bool {
select {
case <-s.closeCh:
return false
default:
}
s.readMu.Lock()
if len(s.readBuf)+len(data) > s.maxBufSize {
s.readMu.Unlock()
return false
}
s.readBuf = append(s.readBuf, data...)
s.readMu.Unlock()
s.readCond.Broadcast()
return true
}
func (s *Stream) SetReadDeadline(t time.Time) error {
s.readDeadline.Set(t)
s.readMu.Lock()
s.readCond.Broadcast()
s.readMu.Unlock()
return nil
}
func (s *Stream) SetWriteDeadline(t time.Time) error {
s.writeDeadline.Set(t)
return nil
}
func (s *Stream) SetDeadline(t time.Time) error {
s.SetReadDeadline(t)
s.SetWriteDeadline(t)
return nil
}
func (s *Stream) LocalAddr() net.Addr {
return s.sess.conn.LocalAddr()
}
func (s *Stream) RemoteAddr() net.Addr {
return s.sess.conn.RemoteAddr()
}
func (s *Stream) HandshakeSuccess() error {
if s.sess.peerVersion.Load() < 2 {
return errors.New("mux: handshake not supported by peer")
}
var done bool
s.handshakeOnce.Do(func() { done = true })
if done {
s.stopHandshakeTimer()
return s.sess.writeFrame(newFrame(cmdSYNACK, s.id, nil))
}
return nil
}
func (s *Stream) HandshakeFailure(err error) error {
if s.sess.peerVersion.Load() < 2 {
return errors.New("mux: handshake not supported by peer")
}
var done bool
s.handshakeOnce.Do(func() { done = true })
if done && err != nil {
s.setHandshakeError(err)
s.stopHandshakeTimer()
return s.sess.writeFrame(newFrame(cmdSYNACK, s.id, []byte(err.Error())))
}
return nil
}