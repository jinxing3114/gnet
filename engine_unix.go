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
	"context"
	"runtime"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/panjf2000/gnet/v2/internal/netpoll"
	"github.com/panjf2000/gnet/v2/pkg/errors"
)

type engine struct {
	ln         *listener    // the listener for accepting new connections
	lb         loadBalancer // event-loops for handling events
	opts       *Options     // options with engine
	mainLoop   *eventloop   // main event-loop for accepting connections
	inShutdown int32        // whether the engine is in shutdown
	ticker     struct {
		ctx    context.Context    // context for ticker
		cancel context.CancelFunc // function to stop the ticker
	}
	workerPool struct {
		*errgroup.Group

		shutdownCtx context.Context
		shutdown    context.CancelFunc
	}
	eventHandler EventHandler // user eventHandler
}

func (eng *engine) isInShutdown() bool {
	return atomic.LoadInt32(&eng.inShutdown) == 1
}

// shutdown signals the engine to shut down.
func (eng *engine) shutdown(err error) {
	if err != nil && err != errors.ErrEngineShutdown {
		eng.opts.Logger.Errorf("engine is being shutdown with error: %v", err)
	}

	eng.workerPool.shutdown()
}

func (eng *engine) startEventLoops() {
	eng.lb.iterate(func(i int, el *eventloop) bool {
		eng.workerPool.Go(el.run)
		return true
	})
}

func (eng *engine) closeEventLoops() {
	eng.lb.iterate(func(i int, el *eventloop) bool {
		_ = el.poller.Close()
		return true
	})
}

func (eng *engine) startSubReactors() {
	eng.lb.iterate(func(i int, el *eventloop) bool {
		eng.workerPool.Go(el.activateSubReactor)
		return true
	})
}

func (eng *engine) activateEventLoops(numEventLoop int) (err error) {
	network, address := eng.ln.network, eng.ln.address
	ln := eng.ln
	eng.ln = nil
	var striker *eventloop
	// Create loops locally and bind the listeners.
	for i := 0; i < numEventLoop; i++ {
		if i > 0 {
			if ln, err = initListener(network, address, eng.opts); err != nil {
				return
			}
		}
		var p *netpoll.Poller
		if p, err = netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			el.ln = ln
			el.engine = eng
			el.poller = p
			el.buffer = make([]byte, eng.opts.ReadBufferCap)
			el.connections = make(map[int]*conn)
			el.eventHandler = eng.eventHandler
			if err = el.poller.AddRead(el.ln.packPollAttachment(el.accept)); err != nil {
				return
			}
			eng.lb.register(el)

			// Start the ticker.
			if el.idx == 0 && eng.opts.Ticker {
				striker = el
			}
		} else {
			return
		}
	}

	// Start event-loops in background.
	eng.startEventLoops()

	eng.workerPool.Go(func() error {
		striker.ticker(eng.ticker.ctx)
		return nil
	})

	return
}

func (eng *engine) activateReactors(numEventLoop int) error {
	for i := 0; i < numEventLoop; i++ {
		if p, err := netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			el.ln = eng.ln
			el.engine = eng
			el.poller = p
			el.buffer = make([]byte, eng.opts.ReadBufferCap)
			el.connections = make(map[int]*conn)
			el.eventHandler = eng.eventHandler
			eng.lb.register(el)
		} else {
			return err
		}
	}

	// Start sub reactors in background.
	eng.startSubReactors()

	if p, err := netpoll.OpenPoller(); err == nil {
		el := new(eventloop)
		el.ln = eng.ln
		el.idx = -1
		el.engine = eng
		el.poller = p
		el.eventHandler = eng.eventHandler
		if err = el.poller.AddRead(eng.ln.packPollAttachment(eng.accept)); err != nil {
			return err
		}
		eng.mainLoop = el

		// Start main reactor in background.
		eng.workerPool.Go(el.activateMainReactor)
	} else {
		return err
	}

	// Start the ticker.
	if eng.opts.Ticker {
		eng.workerPool.Go(func() error {
			eng.mainLoop.ticker(eng.ticker.ctx)
			return nil
		})
	}

	return nil
}

func (eng *engine) start(numEventLoop int) error {
	if eng.opts.ReusePort || eng.ln.network == "udp" {
		return eng.activateEventLoops(numEventLoop)
	}

	return eng.activateReactors(numEventLoop)
}

func (eng *engine) stop(s Engine) {
	// Wait on a signal for shutdown
	<-eng.workerPool.shutdownCtx.Done()

	eng.eventHandler.OnShutdown(s)

	// Notify all loops to close by closing all listeners
	eng.lb.iterate(func(i int, el *eventloop) bool {
		err := el.poller.UrgentTrigger(func(_ interface{}) error { return errors.ErrEngineShutdown }, nil)
		if err != nil {
			eng.opts.Logger.Errorf("failed to call UrgentTrigger on sub event-loop when stopping engine: %v", err)
		}
		return true
	})

	if eng.mainLoop != nil {
		eng.ln.close()
		err := eng.mainLoop.poller.UrgentTrigger(func(_ interface{}) error { return errors.ErrEngineShutdown }, nil)
		if err != nil {
			eng.opts.Logger.Errorf("failed to call UrgentTrigger on main event-loop when stopping engine: %v", err)
		}
	}

	// Stop the ticker.
	if eng.ticker.cancel != nil {
		eng.ticker.cancel()
	}

	if err := eng.workerPool.Wait(); err != nil {
		eng.opts.Logger.Errorf("engine shutdown error: %v", err)
	}

	eng.closeEventLoops()

	if eng.mainLoop != nil {
		err := eng.mainLoop.poller.Close()
		if err != nil {
			eng.opts.Logger.Errorf("failed to close poller when stopping engine: %v", err)
		}
	}

	atomic.StoreInt32(&eng.inShutdown, 1)
}

func run(eventHandler EventHandler, listener *listener, options *Options, protoAddr string) error {
	// Figure out the proper number of event-loops/goroutines to run.
	numEventLoop := 1
	if options.Multicore {
		numEventLoop = runtime.NumCPU()
	}
	if options.NumEventLoop > 0 {
		numEventLoop = options.NumEventLoop
	}

	shutdownCtx, shutdown := context.WithCancel(context.Background())
	eng := engine{
		ln:   listener,
		opts: options,
		workerPool: struct {
			*errgroup.Group
			shutdownCtx context.Context
			shutdown    context.CancelFunc
		}{&errgroup.Group{}, shutdownCtx, shutdown},
		eventHandler: eventHandler,
	}
	switch options.LB {
	case RoundRobin:
		eng.lb = new(roundRobinLoadBalancer)
	case LeastConnections:
		eng.lb = new(leastConnectionsLoadBalancer)
	case SourceAddrHash:
		eng.lb = new(sourceAddrHashLoadBalancer)
	}

	if eng.opts.Ticker {
		eng.ticker.ctx, eng.ticker.cancel = context.WithCancel(context.Background())
	}

	e := Engine{&eng}
	switch eng.eventHandler.OnBoot(e) {
	case None:
	case Shutdown:
		return nil
	}

	if err := eng.start(numEventLoop); err != nil {
		eng.closeEventLoops()
		eng.opts.Logger.Errorf("gnet engine is stopping with error: %v", err)
		return err
	}
	defer eng.stop(e)

	allEngines.Store(protoAddr, &eng)

	return nil
}
