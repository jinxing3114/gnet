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
	ln             *listener             // listener
	idx            int                   // loop index in the engine loops list
	cache          bytes.Buffer          // temporary buffer for scattered bytes
	engine         *engine               // engine in loop
	poller         *netpoll.Poller       // epoll or kqueue
	buffer         []byte                // read packet buffer whose capacity is set by user, default value is 64KB
	connCounts     [gfd.Conn1Max]int32   // number of active connections in event-loop
	connectionNAI1 int                   // connections Next Available Index1
	connectionNAI2 int                   // connections Next Available Index2
	connectionMap  map[int]gfd.GFD       // TCP connection map: fd -> GFD
	connections    [gfd.Conn1Max][]*conn // TCP connection slice *conn
	udpSockets     map[int]*conn         //
	eventHandler   EventHandler          // user eventHandler
}

func (el *eventloop) getLogger() logging.Logger {
	return el.engine.opts.Logger
}

func (el *eventloop) addConn(i1 int, delta int32) {
	atomic.AddInt32(&el.connCounts[i1], delta)
}

func (el *eventloop) loadConn() (ct int32) {
	for i := 0; i < len(el.connCounts); i++ {
		ct += atomic.LoadInt32(&el.connCounts[i])
	}
	return
}

func (el *eventloop) closeAllSockets() {
	// Close loops and all outstanding connections
	for k, cl := range el.connections {
		if el.connCounts[k] == 0 {
			continue
		}
		for _, c := range cl {
			if c != nil {
				_ = el.closeConn(c, nil)
			}
		}
	}

	for _, c := range el.udpSockets {
		if c != nil {
			_ = el.closeConn(c, nil)
		}
	}
}

func (el *eventloop) test(c int) {
	//for i := 0; i < c; i++ {
	//	c1 := &conn{
	//		gfd:        gfd.NewGFD(i, el.idx),
	//		loop:       el,
	//		localAddr:  el.ln.addr,
	//		remoteAddr: el.ln.addr,
	//	}
	//	//ela, _ := elastic.New(el.engine.opts.WriteBufferCap)
	//	//c1.outboundBuffer = *ela
	//	//c1.pollAttachment = *netpoll.GetPollAttachment()
	//	//c1.pollAttachment.FD, c1.pollAttachment.Type = i, netpoll.PollAttachmentTCP
	//	//c1 := &conn{gfd: gfd.NewGFD(i, el.idx)}
	//	//el.storeConn(c1)
	//	el.connectionMap[c1.gfd.FD()] = c1.gfd
	//	el.addConn(el.connectionNAI1, 1)
	//	if el.connections[el.connectionNAI1] == nil {
	//		el.connections[el.connectionNAI1] = make([]*conn, gfd.Conn2Max)
	//	}
	//	el.connections[el.connectionNAI1][el.connectionNAI2] = c1
	//	el.connectionNAI2++
	//	if el.connectionNAI2 == gfd.Conn2Max {
	//		el.connectionNAI1++
	//		el.connectionNAI2 = 0
	//	}
	//}
}

func (el *eventloop) register(c *conn) error {
	if c.pollAttachment.FD == 0 { // UDP socket
		c.pollAttachment = *netpoll.GetPollAttachment()
		c.pollAttachment.FD = c.gfd.FD()
		c.pollAttachment.Type = netpoll.PollAttachmentUDP
		if err := el.poller.AddRead(&c.pollAttachment); err != nil {
			_ = unix.Close(c.gfd.FD())
			c.releaseUDP()
			return err
		}
		el.udpSockets[c.gfd.FD()] = c
		return nil
	}
	if err := el.poller.AddRead(&c.pollAttachment); err != nil {
		_ = unix.Close(c.gfd.FD())
		c.releaseTCP()
		return err
	}

	el.storeConn(c)
	return el.open(c)
}

func (el *eventloop) storeConn(c *conn) {
	if el.connectionNAI1 >= gfd.Conn1Max { //超过上限
		return
	}
	if el.connections[el.connectionNAI1] == nil { //申请空间
		el.connections[el.connectionNAI1] = make([]*conn, gfd.Conn2Max)
		el.connections[el.connectionNAI1][el.connectionNAI2] = c
	}

	el.connections[el.connectionNAI1][el.connectionNAI2] = c
	c.gfd.UpdateConnIndex(el.connectionNAI1, el.connectionNAI2)
	el.connectionMap[c.gfd.FD()] = c.gfd
	el.addConn(el.connectionNAI1, 1)

	//检查当前空间是否有剩余可用位置
	for i2 := el.connectionNAI2; i2 < gfd.Conn2Max; i2++ {
		if el.connections[el.connectionNAI1][i2] == nil {
			el.connectionNAI2 = i2
			return
		}
	}

	//检查已申请的其他空间
	//check if the space have applied for is available
	for i1 := 0; i1 < gfd.Conn1Max; i1++ {
		if el.connections[i1] != nil && el.connCounts[i1] < gfd.Conn2Max {
			for i2 := 0; i2 < gfd.Conn2Max; i2++ {
				if el.connections[i1][i2] == nil {
					el.connectionNAI1, el.connectionNAI2 = i1, i2
					return
				}
			}
		}
	}

	//insufficient space has been applied for, allocate a new space
	for i1 := 0; i1 < gfd.Conn1Max; i1++ {
		if el.connections[i1] == nil {
			el.connectionNAI1, el.connectionNAI2 = i1, 0
			return
		}
	}
}

func (el *eventloop) open(c *conn) error {
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
	n, err := unix.Read(c.gfd.FD(), el.buffer)
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
		n, err = io.Writev(c.gfd.FD(), iov)
	} else {
		n, err = unix.Write(c.gfd.FD(), iov[0])
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
		rerr = el.poller.Delete(c.gfd.FD())
		if c.gfd.FD() != el.ln.fd {
			rerr = unix.Close(c.gfd.FD())
			delete(el.udpSockets, c.gfd.FD())
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
			if n, e := io.Writev(c.gfd.FD(), iov); e != nil {
				el.getLogger().Warnf("closeConn: error occurs when sending data back to peer, %v", e)
				break
			} else {
				_, _ = c.outboundBuffer.Discard(n)
			}
		}
	}

	err0, err1 := el.poller.Delete(c.gfd.FD()), unix.Close(c.gfd.FD())
	if err0 != nil {
		rerr = fmt.Errorf("failed to delete fd=%d from poller in event-loop(%d): %v", c.gfd.FD(), el.idx, err0)
	}
	if err1 != nil {
		err1 = fmt.Errorf("failed to close fd=%d in event-loop(%d): %v", c.gfd.FD(), el.idx, os.NewSyscallError("close", err1))
		if rerr != nil {
			rerr = errors.New(rerr.Error() + " & " + err1.Error())
		} else {
			rerr = err1
		}
	}

	delete(el.connectionMap, c.gfd.FD())
	el.addConn(c.gfd.ConnIndex1(), -1)
	if el.connCounts[c.gfd.ConnIndex1()] == 0 {
		el.connections[c.gfd.ConnIndex1()] = nil
	} else {
		el.connections[c.gfd.ConnIndex1()][c.gfd.ConnIndex2()] = nil
	}

	if el.connectionNAI1 > c.gfd.ConnIndex1() || el.connectionNAI2 > c.gfd.ConnIndex2() {
		el.connectionNAI1, el.connectionNAI2 = c.gfd.ConnIndex1(), c.gfd.ConnIndex2()
	}

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
	if el.connections[task.GFD.ConnIndex1()] == nil {
		return
	}
	c := el.connections[task.GFD.ConnIndex1()][task.GFD.ConnIndex2()]
	if c == nil || c.gfd.FD() != task.GFD.FD() {
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
