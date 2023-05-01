// Copyright (c) 2019 Andy Pan
// Copyright (c) 2018 Joshua J Baker
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux || freebsd || dragonfly || darwin
// +build linux freebsd dragonfly darwin

package gnet

import (
	"github.com/panjf2000/gnet/v2/pkg/gfd"
	"io"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"

	"github.com/panjf2000/gnet/v2/internal/bs"
	gio "github.com/panjf2000/gnet/v2/internal/io"
	"github.com/panjf2000/gnet/v2/internal/netpoll"
	"github.com/panjf2000/gnet/v2/internal/socket"
	"github.com/panjf2000/gnet/v2/pkg/buffer/elastic"
	gerrors "github.com/panjf2000/gnet/v2/pkg/errors"
	bsPool "github.com/panjf2000/gnet/v2/pkg/pool/byteslice"
)

type conn struct {
	gfd            gfd.GFD                // file descriptor
	ctx            interface{}            // user-defined context
	peer           unix.Sockaddr          // remote socket address
	localAddr      net.Addr               // local addr
	remoteAddr     net.Addr               // remote addr
	loop           *eventloop             // connected event-loop
	outboundBuffer elastic.Buffer         // buffer for data that is eligible to be sent to the peer
	pollAttachment netpoll.PollAttachment // connection attachment for poller
	inboundBuffer  elastic.RingBuffer     // buffer for leftover data from the peer
	buffer         []byte                 // buffer for the latest bytes
	isDatagram     bool                   // UDP protocol
	opened         bool                   // connection opened event fired
}

func newTCPConn(fd int, el *eventloop, sa unix.Sockaddr, localAddr, remoteAddr net.Addr) (c *conn) {
	c = &conn{
		gfd:            gfd.NewGFD(fd, el.idx),
		peer:           sa,
		loop:           el,
		localAddr:      localAddr,
		remoteAddr:     remoteAddr,
		pollAttachment: netpoll.PollAttachment{FD: fd, Type: netpoll.PollAttachmentStream},
	}
	ela, _ := elastic.New(el.engine.opts.WriteBufferCap)
	c.outboundBuffer = *ela
	return
}

func (c *conn) releaseTCP() {
	c.opened = false
	c.peer = nil
	c.ctx = nil
	c.buffer = nil
	if addr, ok := c.localAddr.(*net.TCPAddr); ok && c.localAddr != c.loop.ln.addr && len(addr.Zone) > 0 {
		bsPool.Put(bs.StringToBytes(addr.Zone))
	}
	if addr, ok := c.remoteAddr.(*net.TCPAddr); ok && len(addr.Zone) > 0 {
		bsPool.Put(bs.StringToBytes(addr.Zone))
	}
	c.localAddr = nil
	c.remoteAddr = nil
	c.inboundBuffer.Done()
	c.outboundBuffer.Release()
	c.pollAttachment.FD, c.pollAttachment.Type = 0, 0
}

func newUDPConn(fd int, el *eventloop, localAddr net.Addr, sa unix.Sockaddr, connected bool) (c *conn) {
	c = &conn{
		gfd:            gfd.NewGFD(fd, el.idx),
		peer:           sa,
		loop:           el,
		localAddr:      localAddr,
		remoteAddr:     socket.SockaddrToUDPAddr(sa),
		isDatagram:     true,
		pollAttachment: netpoll.PollAttachment{FD: fd, Type: netpoll.PollAttachmentDatagram},
	}
	if connected {
		c.peer = nil
	}
	return
}

func (c *conn) releaseUDP() {
	c.ctx = nil
	if addr, ok := c.localAddr.(*net.UDPAddr); ok && c.localAddr != c.loop.ln.addr && len(addr.Zone) > 0 {
		bsPool.Put(bs.StringToBytes(addr.Zone))
	}
	if addr, ok := c.remoteAddr.(*net.UDPAddr); ok && len(addr.Zone) > 0 {
		bsPool.Put(bs.StringToBytes(addr.Zone))
	}
	c.localAddr = nil
	c.remoteAddr = nil
	c.buffer = nil
	c.pollAttachment.FD, c.pollAttachment.Type = 0, 0
}

func (c *conn) open(buf []byte) error {
	n, err := unix.Write(c.gfd.Fd(), buf)
	if err != nil && err == unix.EAGAIN {
		_, _ = c.outboundBuffer.Write(buf)
		return nil
	}

	if err == nil && n < len(buf) {
		_, _ = c.outboundBuffer.Write(buf[n:])
	}

	return err
}

func (c *conn) write(data []byte) (n int, err error) {
	n = len(data)
	// If there is pending data in outbound buffer, the current data ought to be appended to the outbound buffer
	// for maintaining the sequence of network packets.
	if !c.outboundBuffer.IsEmpty() {
		_, _ = c.outboundBuffer.Write(data)
		return
	}

	var sent int
	if sent, err = unix.Write(c.gfd.Fd(), data); err != nil {
		// A temporary error occurs, append the data to outbound buffer, writing it back to the peer in the next round.
		if err == unix.EAGAIN {
			_, _ = c.outboundBuffer.Write(data)
			err = c.loop.poller.ModReadWrite(&c.pollAttachment)
			return
		}
		return -1, c.loop.closeConn(c, os.NewSyscallError("write", err))
	}
	// Failed to send all data back to the peer, buffer the leftover data for the next round.
	if sent < n {
		_, _ = c.outboundBuffer.Write(data[sent:])
		err = c.loop.poller.ModReadWrite(&c.pollAttachment)
	}
	return
}

func (c *conn) writev(bs [][]byte) (n int, err error) {
	for _, b := range bs {
		n += len(b)
	}

	// If there is pending data in outbound buffer, the current data ought to be appended to the outbound buffer
	// for maintaining the sequence of network packets.
	if !c.outboundBuffer.IsEmpty() {
		_, _ = c.outboundBuffer.Writev(bs)
		return
	}

	var sent int
	if sent, err = gio.Writev(c.gfd.Fd(), bs); err != nil {
		// A temporary error occurs, append the data to outbound buffer, writing it back to the peer in the next round.
		if err == unix.EAGAIN {
			_, _ = c.outboundBuffer.Writev(bs)
			err = c.loop.poller.ModReadWrite(&c.pollAttachment)
			return
		}
		return -1, c.loop.closeConn(c, os.NewSyscallError("write", err))
	}
	// Failed to send all data back to the peer, buffer the leftover data for the next round.
	if sent < n {
		var pos int
		for i := range bs {
			bn := len(bs[i])
			if sent < bn {
				bs[i] = bs[i][sent:]
				pos = i
				break
			}
			sent -= bn
		}
		_, _ = c.outboundBuffer.Writev(bs[pos:])
		err = c.loop.poller.ModReadWrite(&c.pollAttachment)
	}
	return
}

type asyncWriteHook struct {
	callback AsyncCallback
	data     []byte
}

func (c *conn) asyncWrite(hook *asyncWriteHook) (err error) {
	if !c.opened {
		return nil
	}

	_, err = c.write(hook.data)
	if hook.callback != nil {
		_ = hook.callback(c, err)
	}
	return
}

type asyncWritevHook struct {
	callback AsyncCallback
	data     [][]byte
}

func (c *conn) asyncWritev(hook *asyncWritevHook) (err error) {
	if !c.opened {
		return nil
	}

	_, err = c.writev(hook.data)
	if hook.callback != nil {
		_ = hook.callback(c, err)
	}
	return
}

func (c *conn) sendTo(buf []byte) error {
	if c.peer == nil {
		return unix.Send(c.gfd.Fd(), buf, 0)
	}
	return unix.Sendto(c.gfd.Fd(), buf, 0, c.peer)
}

func (c *conn) resetBuffer() {
	c.buffer = c.buffer[:0]
	c.inboundBuffer.Reset()
}

// ================================== Non-concurrency-safe API's ==================================

func (c *conn) Read(p []byte) (n int, err error) {
	if c.inboundBuffer.IsEmpty() {
		n = copy(p, c.buffer)
		c.buffer = c.buffer[n:]
		if n == 0 && len(p) > 0 {
			err = io.EOF
		}
		return
	}
	n, _ = c.inboundBuffer.Read(p)
	if n == len(p) {
		return
	}
	m := copy(p[n:], c.buffer)
	n += m
	c.buffer = c.buffer[m:]
	return
}

func (c *conn) Next(n int) (buf []byte, err error) {
	inBufferLen := c.inboundBuffer.Buffered()
	if totalLen := inBufferLen + len(c.buffer); n > totalLen {
		return nil, io.ErrShortBuffer
	} else if n <= 0 {
		n = totalLen
	}
	if c.inboundBuffer.IsEmpty() {
		buf = c.buffer[:n]
		c.buffer = c.buffer[n:]
		return
	}
	head, tail := c.inboundBuffer.Peek(n)
	defer c.inboundBuffer.Discard(n) //nolint:errcheck
	if len(head) >= n {
		return head[:n], err
	}
	c.loop.cache.Reset()
	c.loop.cache.Write(head)
	c.loop.cache.Write(tail)
	if inBufferLen >= n {
		return c.loop.cache.Bytes(), err
	}

	remaining := n - inBufferLen
	c.loop.cache.Write(c.buffer[:remaining])
	c.buffer = c.buffer[remaining:]
	return c.loop.cache.Bytes(), err
}

func (c *conn) Peek(n int) (buf []byte, err error) {
	inBufferLen := c.inboundBuffer.Buffered()
	if totalLen := inBufferLen + len(c.buffer); n > totalLen {
		return nil, io.ErrShortBuffer
	} else if n <= 0 {
		n = totalLen
	}
	if c.inboundBuffer.IsEmpty() {
		return c.buffer[:n], err
	}
	head, tail := c.inboundBuffer.Peek(n)
	if len(head) >= n {
		return head[:n], err
	}
	c.loop.cache.Reset()
	c.loop.cache.Write(head)
	c.loop.cache.Write(tail)
	if inBufferLen >= n {
		return c.loop.cache.Bytes(), err
	}

	remaining := n - inBufferLen
	c.loop.cache.Write(c.buffer[:remaining])
	return c.loop.cache.Bytes(), err
}

func (c *conn) Discard(n int) (int, error) {
	inBufferLen := c.inboundBuffer.Buffered()
	tempBufferLen := len(c.buffer)
	if inBufferLen+tempBufferLen < n || n <= 0 {
		c.resetBuffer()
		return inBufferLen + tempBufferLen, nil
	}
	if c.inboundBuffer.IsEmpty() {
		c.buffer = c.buffer[n:]
		return n, nil
	}

	discarded, _ := c.inboundBuffer.Discard(n)
	if discarded < inBufferLen {
		return discarded, nil
	}

	remaining := n - inBufferLen
	c.buffer = c.buffer[remaining:]
	return n, nil
}

func (c *conn) Write(p []byte) (int, error) {
	if c.isDatagram {
		if err := c.sendTo(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	return c.write(p)
}

func (c *conn) Writev(bs [][]byte) (int, error) {
	if c.isDatagram {
		return 0, gerrors.ErrUnsupportedOp
	}
	return c.writev(bs)
}

func (c *conn) ReadFrom(r io.Reader) (int64, error) {
	return c.outboundBuffer.ReadFrom(r)
}

func (c *conn) WriteTo(w io.Writer) (n int64, err error) {
	if !c.inboundBuffer.IsEmpty() {
		if n, err = c.inboundBuffer.WriteTo(w); err != nil {
			return
		}
	}
	var m int
	m, err = w.Write(c.buffer)
	n += int64(m)
	c.buffer = c.buffer[m:]
	return
}

func (c *conn) Flush() error {
	if c.outboundBuffer.IsEmpty() {
		return nil
	}

	return c.loop.write(c)
}

func (c *conn) InboundBuffered() int {
	return c.inboundBuffer.Buffered() + len(c.buffer)
}

func (c *conn) OutboundBuffered() int {
	return c.outboundBuffer.Buffered()
}

func (*conn) SetDeadline(_ time.Time) error {
	return gerrors.ErrUnsupportedOp
}

func (*conn) SetReadDeadline(_ time.Time) error {
	return gerrors.ErrUnsupportedOp
}

func (*conn) SetWriteDeadline(_ time.Time) error {
	return gerrors.ErrUnsupportedOp
}

func (c *conn) Context() interface{}       { return c.ctx }
func (c *conn) SetContext(ctx interface{}) { c.ctx = ctx }
func (c *conn) LocalAddr() net.Addr        { return c.localAddr }
func (c *conn) RemoteAddr() net.Addr       { return c.remoteAddr }

// Implementation of Socket interface

func (c *conn) Fd() int                        { return c.gfd.Fd() }
func (c *conn) Gfd() gfd.GFD                   { return c.gfd }
func (c *conn) Dup() (fd int, err error)       { fd, _, err = netpoll.Dup(c.gfd.Fd()); return }
func (c *conn) SetReadBuffer(bytes int) error  { return socket.SetRecvBuffer(c.gfd.Fd(), bytes) }
func (c *conn) SetWriteBuffer(bytes int) error { return socket.SetSendBuffer(c.gfd.Fd(), bytes) }
func (c *conn) SetLinger(sec int) error        { return socket.SetLinger(c.gfd.Fd(), sec) }
func (c *conn) SetNoDelay(noDelay bool) error {
	return socket.SetNoDelay(c.gfd.Fd(), bool2int(noDelay))
}
func (c *conn) SetKeepAlivePeriod(d time.Duration) error {
	return socket.SetKeepAlivePeriod(c.gfd.Fd(), int(d.Seconds()))
}

// ==================================== Concurrency-safe API's ====================================

func (c *conn) AsyncWrite(buf []byte, callback AsyncCallback) error {
	if c.isDatagram {
		defer func() {
			if callback != nil {
				_ = callback(nil, nil)
			}
		}()
		return c.sendTo(buf)
	}
	return c.loop.poller.Trigger(triggerTypeAsyncWrite, c.gfd, &asyncWriteHook{callback, buf})
}

func (c *conn) AsyncWritev(bs [][]byte, callback AsyncCallback) error {
	if c.isDatagram {
		return gerrors.ErrUnsupportedOp
	}
	return c.loop.poller.Trigger(triggerTypeAsyncWritev, c.gfd, &asyncWritevHook{callback, bs})
}

func (c *conn) Wake(callback AsyncCallback) error {
	return c.loop.poller.UrgentTrigger(triggerTypeWake, c.gfd, callback)
}

func (c *conn) CloseWithCallback(callback AsyncCallback) error {
	return c.loop.poller.Trigger(triggerTypeClose, c.gfd, callback)
}

func (c *conn) Close() error {
	return c.loop.poller.Trigger(triggerTypeClose, c.gfd, nil)
}
