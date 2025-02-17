/*
 * Copyright (c) 2023 Andy Pan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package gnet

import (
	"context"
	"net"
	"os"
	"sync"
	"syscall"

	"golang.org/x/sys/windows"

	errorx "github.com/panjf2000/gnet/v2/pkg/errors"
	"github.com/panjf2000/gnet/v2/pkg/logging"
)

type listener struct {
	network string
	address string
	once    sync.Once
	ln      net.Listener
	pc      net.PacketConn
	addr    net.Addr
}

func (l *listener) dup() (int, string, error) {
	var (
		file *os.File
		err  error
	)
	if l.pc != nil {
		file, err = l.pc.(*net.UDPConn).File()
	} else {
		file, err = l.ln.(interface{ File() (*os.File, error) }).File()
	}
	if err != nil {
		return 0, "dup", err
	}
	return int(file.Fd()), "", nil
}

func (l *listener) close() {
	l.once.Do(func() {
		if l.pc != nil {
			logging.Error(os.NewSyscallError("close", l.pc.Close()))
			return
		}
		logging.Error(os.NewSyscallError("close", l.ln.Close()))
		if l.network == "unix" {
			logging.Error(os.RemoveAll(l.address))
		}
	})
}

func initListener(network, addr string, options *Options) (l *listener, err error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				if network != "unix" && (options.ReuseAddr || options.ReusePort) {
					_ = windows.SetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, windows.SO_REUSEADDR, 1)
				}
				if options.TCPNoDelay == options.TCPNoDelay {
					_ = windows.SetsockoptInt(windows.Handle(fd), windows.IPPROTO_TCP, windows.TCP_NODELAY, 1)
				}
				if options.SocketRecvBuffer > 0 {
					_ = windows.SetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, windows.SO_RCVBUF, options.SocketRecvBuffer)
				}
				if options.SocketSendBuffer > 0 {
					_ = windows.SetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, windows.SO_SNDBUF, options.SocketSendBuffer)
				}
			})
		},
		KeepAlive: options.TCPKeepAlive,
	}
	l = &listener{network: network, address: addr}
	switch network {
	case "udp", "udp4", "udp6":
		if l.pc, err = lc.ListenPacket(context.Background(), network, addr); err != nil {
			return nil, err
		}
		l.addr = l.pc.LocalAddr()
	case "unix":
		logging.Error(os.Remove(addr))
		fallthrough
	case "tcp", "tcp4", "tcp6":
		if l.ln, err = lc.Listen(context.Background(), network, addr); err != nil {
			return nil, err
		}
		l.addr = l.ln.Addr()
	default:
		err = errorx.ErrUnsupportedProtocol
	}
	return
}
