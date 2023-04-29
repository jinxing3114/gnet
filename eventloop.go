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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/panjf2000/gnet/v2/internal/queue"
	"github.com/panjf2000/gnet/v2/pkg/gfd"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/panjf2000/gnet/v2/internal/io"
	"github.com/panjf2000/gnet/v2/internal/netpoll"
	gerrors "github.com/panjf2000/gnet/v2/pkg/errors"
	"github.com/panjf2000/gnet/v2/pkg/logging"
)

const (
	triggerTypeAsyncWrite = iota
	triggerTypeAsyncWritev
	triggerTypeWake
	triggerTypeClose
	triggerTypeShutdown
	triggerRegister
)

type eventloop struct {
	ln           *listener       // listener
	idx          int             // loop index in the engine loops list
	cache        bytes.Buffer    // temporary buffer for scattered bytes
	engine       *engine         // engine in loop
	poller       *netpoll.Poller // epoll or kqueue
	buffer       []byte          // read packet buffer whose capacity is set by user, default value is 64KB
	connCount    int32           // number of active connections in event-loop
	connections  map[int]*conn   // TCP connection map: fd -> GFD
	udpSockets   map[int]*conn   //
	eventHandler EventHandler    // user eventHandler
}

func (el *eventloop) getLogger() logging.Logger {
	return el.engine.opts.Logger
}

func (el *eventloop) addConn(delta int32) {
	atomic.AddInt32(&el.connCount, delta)
}

func (el *eventloop) loadConn() (ct int32) {
	return atomic.LoadInt32(&el.connCount)
}

func (el *eventloop) closeAllSockets() {
	// Close loops and all outstanding connections
	for _, c := range el.connections {
		_ = el.closeConn(c, nil)
	}

	for _, c := range el.udpSockets {
		_ = el.closeConn(c, nil)
	}
}

func (el *eventloop) test(c1 int) {
	for i := 0; i < c1; i++ {
		c := newTCPConn(i, el, nil, el.ln.addr, el.ln.addr)
		//c1 := &conn{
		//	gfd:        gfd.NewGFD(i, el.idx),
		//	loop:       el,
		//	localAddr:  el.ln.addr,
		//	remoteAddr: el.ln.addr,
		//}
		//ela, _ := elastic.New(el.engine.opts.WriteBufferCap)
		//c1.outboundBuffer = *ela
		//c1.pollAttachment = *netpoll.GetPollAttachment()
		//c1.pollAttachment.FD, c1.pollAttachment.Type = i, netpoll.PollAttachmentTCP
		//c1 := &conn{gfd: gfd.NewGFD(i, el.idx)}
		//el.storeConn(c1)
		el.connections[c.gfd.Fd()] = c
		el.addConn(1)
		//el.connections[el.connectionNAI] = c1
		//el.connectionNAI++
		//el.connections1 = append(el.connections1, conn{
		//	gfd: gfd.NewGFD(i, el.idx),
		//	loop: el,
		//	localAddr:  el.ln.addr,
		//	remoteAddr: el.ln.addr,
		//})
		//el.connections3 = append(el.connections3, conn1{
		//	gfd: gfd.NewGFD(i, el.idx),
		//loop: el,
		//localAddr:  el.ln.addr,
		//remoteAddr: el.ln.addr,
		//})
		//el.connections2 = append(el.connections2, gfd.NewGFD(i, el.idx))
	}
	//log.Println("el ", el.idx, cap(el.connections3), len(el.connections3))
}

func (el *eventloop) register(c *conn) error {
	if c.pollAttachment.FD == 0 { // UDP socket
		c.pollAttachment.FD, c.pollAttachment.Type = c.gfd.Fd(), netpoll.PollAttachmentUDP
		if err := el.poller.AddRead(&c.pollAttachment); err != nil {
			_ = unix.Close(c.gfd.Fd())
			c.releaseUDP()
			return err
		}
		el.udpSockets[c.gfd.Fd()] = c
		return nil
	}
	if err := el.poller.AddRead(&c.pollAttachment); err != nil {
		_ = unix.Close(c.gfd.Fd())
		c.releaseTCP()
		return err
	}

	el.connections[c.gfd.Fd()] = c
	return el.open(c)
}

func (el *eventloop) open(c *conn) error {
	el.addConn(1)
	c.opened = true

	out, action := el.eventHandler.OnOpen(c)
	if out != nil {
		if err := c.open(out); err != nil {
			return err
		}
	}

	if !c.outboundBuffer.IsEmpty() {
		if err := el.poller.AddWrite(&c.pollAttachment); err != nil {
			return err
		}
	}

	return el.handleAction(c, action)
}

func (el *eventloop) read(c *conn) error {
	n, err := unix.Read(c.gfd.Fd(), el.buffer)
	if err != nil || n == 0 {
		if err == unix.EAGAIN {
			return nil
		}
		if n == 0 {
			err = unix.ECONNRESET
		}
		return el.closeConn(c, os.NewSyscallError("read", err))
	}

	c.buffer = el.buffer[:n]
	action := el.eventHandler.OnTraffic(c)
	switch action {
	case None:
	case Close:
		return el.closeConn(c, nil)
	case Shutdown:
		return gerrors.ErrEngineShutdown
	}
	_, _ = c.inboundBuffer.Write(c.buffer)

	return nil
}

const iovMax = 1024

func (el *eventloop) write(c *conn) error {
	iov := c.outboundBuffer.Peek(-1)
	var (
		n   int
		err error
	)
	if len(iov) > 1 {
		if len(iov) > iovMax {
			iov = iov[:iovMax]
		}
		n, err = io.Writev(c.gfd.Fd(), iov)
	} else {
		n, err = unix.Write(c.gfd.Fd(), iov[0])
	}
	_, _ = c.outboundBuffer.Discard(n)
	switch err {
	case nil:
	case unix.EAGAIN:
		return nil
	default:
		return el.closeConn(c, os.NewSyscallError("write", err))
	}

	// All data have been drained, it's no need to monitor the writable events,
	// remove the writable event from poller to help the future event-loops.
	if c.outboundBuffer.IsEmpty() {
		_ = el.poller.ModRead(&c.pollAttachment)
	}

	return nil
}

func (el *eventloop) closeConn(c *conn, err error) (rerr error) {
	if addr := c.localAddr; addr != nil && strings.HasPrefix(c.localAddr.Network(), "udp") {
		rerr = el.poller.Delete(c.gfd.Fd())
		if c.gfd.Fd() != el.ln.fd {
			rerr = unix.Close(c.gfd.Fd())
			delete(el.udpSockets, c.gfd.Fd())
		}
		if el.eventHandler.OnClose(c, err) == Shutdown {
			return gerrors.ErrEngineShutdown
		}
		c.releaseUDP()
		return
	}

	if !c.opened {
		return
	}

	// Send residual data in buffer back to the peer before actually closing the connection.
	if !c.outboundBuffer.IsEmpty() {
		for !c.outboundBuffer.IsEmpty() {
			iov := c.outboundBuffer.Peek(0)
			if len(iov) > iovMax {
				iov = iov[:iovMax]
			}
			if n, e := io.Writev(c.gfd.Fd(), iov); e != nil {
				el.getLogger().Warnf("closeConn: error occurs when sending data back to peer, %v", e)
				break
			} else {
				_, _ = c.outboundBuffer.Discard(n)
			}
		}
	}

	err0, err1 := el.poller.Delete(c.gfd.Fd()), unix.Close(c.gfd.Fd())
	if err0 != nil {
		rerr = fmt.Errorf("failed to delete fd=%d from poller in event-loop(%d): %v", c.gfd.Fd(), el.idx, err0)
	}
	if err1 != nil {
		err1 = fmt.Errorf("failed to close fd=%d in event-loop(%d): %v", c.gfd.Fd(), el.idx, os.NewSyscallError("close", err1))
		if rerr != nil {
			rerr = errors.New(rerr.Error() + " & " + err1.Error())
		} else {
			rerr = err1
		}
	}

	delete(el.connections, c.gfd.Fd())
	el.addConn(-1)

	if el.eventHandler.OnClose(c, err) == Shutdown {
		rerr = gerrors.ErrEngineShutdown
	}
	c.releaseTCP()

	return
}

func (el *eventloop) wake(c *conn) error {
	action := el.eventHandler.OnTraffic(c)

	return el.handleAction(c, action)
}

func (el *eventloop) ticker(ctx context.Context) {
	if el == nil {
		return
	}
	var (
		action Action
		delay  time.Duration
		timer  *time.Timer
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		delay, action = el.eventHandler.OnTick()
		switch action {
		case None:
		case Shutdown:
			err := el.poller.UrgentTrigger(triggerTypeShutdown, gfd.GFD{}, nil)
			el.getLogger().Debugf("stopping ticker in event-loop(%d) from OnTick(), UrgentTrigger:%v", el.idx, err)
		}
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}
		select {
		case <-ctx.Done():
			el.getLogger().Debugf("stopping ticker in event-loop(%d) from Engine, error:%v", el.idx, ctx.Err())
			return
		case <-timer.C:
		}
	}
}

func (el *eventloop) pollCallback(poolType byte, fd int, e netpoll.IOEvent) (err error) {
	switch poolType {
	case netpoll.PollAttachmentMainAccept:
		return el.engine.accept(fd, e)
	case netpoll.PollAttachmentEventLoops:
		return el.accept(fd, e)
	case netpoll.PollAttachmentTCP:
		return el.handleEvents(fd, e)
	case netpoll.PollAttachmentUDP:
		return el.readUDP(fd, e)
	default:
		return
	}
}

func (el *eventloop) taskRun(task *queue.Task) (err error) {
	//非conn执行任务
	switch task.TaskType {
	case triggerTypeShutdown:
		return gerrors.ErrEngineShutdown
	case triggerRegister:
		return el.register(task.Arg.(*conn))
	}

	//需conn执行任务

	c, ok := el.connections[task.GFD.Fd()]
	if !ok || c.gfd.Fd() != task.GFD.Fd() {
		return
	}
	switch task.TaskType {
	case triggerTypeAsyncWrite:
		return c.asyncWrite(task.Arg.(*asyncWriteHook))
	case triggerTypeAsyncWritev:
		return c.asyncWritev(task.Arg.(*asyncWritevHook))
	case triggerTypeClose:
		err = el.closeConn(c, nil)
		if task.Arg != nil {
			if callback, ok := task.Arg.(AsyncCallback); ok && callback != nil {
				_ = callback(c, err)
			}
		}
		return
	case triggerTypeWake:
		err = el.wake(c)
		if task.Arg != nil {
			if callback, ok := task.Arg.(AsyncCallback); ok && callback != nil {
				_ = callback(c, err)
			}
		}
		return
	default:
		return
	}
}

func (el *eventloop) handleAction(c *conn, action Action) error {
	switch action {
	case None:
		return nil
	case Close:
		return el.closeConn(c, nil)
	case Shutdown:
		return gerrors.ErrEngineShutdown
	default:
		return nil
	}
}

func (el *eventloop) readUDP(fd int, _ netpoll.IOEvent) error {
	n, sa, err := unix.Recvfrom(fd, el.buffer, 0)
	if err != nil {
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return nil
		}
		return fmt.Errorf("failed to read UDP packet from fd=%d in event-loop(%d), %v",
			fd, el.idx, os.NewSyscallError("recvfrom", err))
	}
	var c *conn
	if fd == el.ln.fd {
		c = newUDPConn(fd, el, el.ln.addr, sa, false)
	} else {
		c = el.udpSockets[fd]
	}
	c.buffer = el.buffer[:n]
	action := el.eventHandler.OnTraffic(c)
	if c.peer != nil {
		c.releaseUDP()
	}
	if action == Shutdown {
		return gerrors.ErrEngineShutdown
	}
	return nil
}
