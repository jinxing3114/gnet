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
	"log"
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
	ln            *listener       // listener
	idx           int             // loop index in the engine loops list
	cache         bytes.Buffer    // temporary buffer for scattered bytes
	engine        *engine         // engine in loop
	poller        *netpoll.Poller // epoll or kqueue
	buffer        []byte          // read packet buffer whose capacity is set by user, default value is 64KB
	connCount     int32           // number of active connections in event-loop
	connectionNAI int             //connections Next Available Index
	connectionMap map[int]GFD     //TCP connection map: fd -> GFD
	connections   []*conn         //TCP connection slice *conn
	eventHandler  EventHandler    // user eventHandler
}

func (el *eventloop) getLogger() logging.Logger {
	return el.engine.opts.Logger
}

func (el *eventloop) addConn(delta int32) {
	atomic.AddInt32(&el.connCount, delta)
}

func (el *eventloop) loadConn() int32 {
	return atomic.LoadInt32(&el.connCount)
}

func (el *eventloop) closeAllSockets() {
	// Close loops and all outstanding connections
	log.Println(len(el.connections), cap(el.connections))
	for i, c := range el.connections {
		log.Println(i, c == nil)
		if c != nil {
			_ = el.closeConn(c, nil)
		}
	}
}

func (el *eventloop) register(itf interface{}) error {
	c := itf.(*conn)
	if c.pollAttachment == nil { // UDP socket
		c.pollAttachment = netpoll.GetPollAttachment()
		c.pollAttachment.FD = c.gfd.FD()
		c.pollAttachment.Callback = el.readUDP
		if err := el.poller.AddRead(c.pollAttachment); err != nil {
			_ = unix.Close(c.gfd.FD())
			c.releaseUDP()
			return err
		}
		el.connsAdd(c)
		return nil
	}
	if err := el.poller.AddRead(c.pollAttachment); err != nil {
		_ = unix.Close(c.gfd.FD())
		c.releaseTCP()
		return err
	}

	el.connsAdd(c)
	return el.open(c)
}

func (el *eventloop) connsAdd(c *conn) {
	log.Println("conns add before:", c.gfd.FD(), el.connectionNAI, len(el.connections), cap(el.connections))
	if el.connectionNAI == -1 || el.connectionNAI >= len(el.connections) { //第一位或者已超过可用位置
		if cap(el.connections) == len(el.connections) { //需要扩容，可改为已用容量百分比判断
			el.connections = append(el.connections, make([]*conn, 10000)...)
			//有剩余容量，可直接追加
			c.gfd.updateConnIndex(el.connectionNAI)
			el.connections[el.connectionNAI] = c
			el.connectionNAI++
		} else {
			//有剩余容量，可直接追加
			c.gfd.updateConnIndex(len(el.connections))
			el.connections = append(el.connections, c)
			el.connectionNAI = len(el.connections)
		}
	} else {
		el.connections[el.connectionNAI] = c
		c.gfd.updateConnIndex(el.connectionNAI)
		for i := el.connectionNAI; i < len(el.connections); i++ {
			if el.connections[i] == nil {
				el.connectionNAI = i
				return
			}
		}
		el.connectionNAI = len(el.connections)
	}
	log.Println("conns add after:", c.gfd.FD(), el.connectionNAI, len(el.connections), cap(el.connections))
	el.connectionMap[c.gfd.FD()] = c.gfd
}

func (el *eventloop) releaseConns() {
	if cap(el.connections)-len(el.connections) > 20000 { //空余容量大于阈值2倍，则降低容量
		el.connections = el.connections[: len(el.connections) : cap(el.connections)-10000]
	}
}

func (el *eventloop) open(c *conn) error {
	c.opened = true
	el.addConn(1)

	out, action := el.eventHandler.OnOpen(c)
	if out != nil {
		if err := c.open(out); err != nil {
			return err
		}
	}

	if !c.outboundBuffer.IsEmpty() {
		if err := el.poller.AddWrite(c.pollAttachment); err != nil {
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
		_ = el.poller.ModRead(c.pollAttachment)
	}

	return nil
}

func (el *eventloop) closeConn(c *conn, err error) (rerr error) {
	log.Println("close fd:", c.Gfd().FD())
	if addr := c.localAddr; addr != nil && strings.HasPrefix(c.localAddr.Network(), "udp") {
		rerr = el.poller.Delete(c.gfd.FD())
		if c.gfd.FD() != el.ln.fd {
			rerr = unix.Close(c.gfd.FD())
			delete(el.connectionMap, c.gfd.FD())
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
	el.connections[c.gfd.connIndex()] = nil
	if el.connectionNAI > c.gfd.connIndex() {
		el.connectionNAI = c.gfd.connIndex()
	}
	el.releaseConns()

	el.addConn(-1)
	if el.eventHandler.OnClose(c, err) == Shutdown {
		rerr = gerrors.ErrEngineShutdown
	}
	c.releaseTCP()

	return
}

func (el *eventloop) wake(c *conn) error {
	if _, ok := el.connectionMap[c.gfd.FD()]; !ok || el.connections[c.gfd.connIndex()] != c {
		return nil // ignore stale wakes.
	}

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
			err := el.poller.UrgentTrigger(0, 0, triggerTypeShutdown, nil)
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

func (el *eventloop) taskFuncRun(task *queue.Task) (err error) {
	//非conn执行任务
	if task.Fd == 0 || task.ConnIndex == 0 {
		switch task.TaskType {
		case triggerTypeShutdown:
			return gerrors.ErrEngineShutdown
		case triggerRegister:
			return el.register(task.Arg)
		default:
			return
		}
	}

	//需conn执行任务
	if len(el.connections) <= task.ConnIndex {
		return
	}
	c := el.connections[task.ConnIndex]
	if c == nil || c.gfd.FD() != task.Fd {
		return
	}
	switch task.TaskType {
	case triggerTypeAsyncWrite:
		return c.asyncWrite(task.Arg)
	case triggerTypeAsyncWritev:
		return c.asyncWritev(task.Arg)
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
	//log.Println(reflect.TypeOf(sa), sa.(*unix.SockaddrInet4))
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
		c = el.connections[fd]
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
