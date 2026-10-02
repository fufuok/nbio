// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package taskpool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/fufuok/nbio/logging"
)

// TaskPool .
type TaskPool struct {
	concurrent    int64
	maxConcurrent int64
	chQqueue      chan func()
	chSlot        chan struct{}
	chClose       chan struct{}
	stopOnce      sync.Once
	caller        func(f func())
}

// acquire tries to take a worker slot.
//
//go:norace
func (tp *TaskPool) acquire() bool {
	if atomic.AddInt64(&tp.concurrent, 1) <= tp.maxConcurrent {
		return true
	}
	atomic.AddInt64(&tp.concurrent, -1)
	return false
}

// release gives back a worker slot and wakes up the dispatcher if it's waiting.
//
//go:norace
func (tp *TaskPool) release() {
	atomic.AddInt64(&tp.concurrent, -1)
	select {
	case tp.chSlot <- struct{}{}:
	default:
	}
}

// fork .
//
//go:norace
func (tp *TaskPool) fork(f func()) bool {
	if !tp.acquire() {
		return false
	}
	go func() {
		defer tp.release()
		for {
			tp.caller(f)
			select {
			case f = <-tp.chQqueue:
			default:
				return
			}
		}
	}()
	return true
}

// dispatch moves queued tasks to workers. It never executes tasks itself,
// so a blocking task can't stop the queue from being consumed.
//
//go:norace
func (tp *TaskPool) dispatch() {
	for {
		select {
		case f := <-tp.chQqueue:
			for !tp.fork(f) {
				select {
				case <-tp.chSlot:
				case <-tp.chClose:
					return
				}
			}
		case <-tp.chClose:
			return
		}
	}
}

// Call .
//
//go:norace
func (tp *TaskPool) Call(f func()) {
	tp.caller(f)
}

// Go .
//
//go:norace
func (tp *TaskPool) Go(f func()) {
	if f == nil {
		return
	}

	// If current goroutine num is less than maxConcurrent,
	// creat a new goroutine to exec new task.
	if tp.fork(f) {
		return
	}

	// Else push the new task into chan/queue.
	select {
	case tp.chQqueue <- f:
	case <-tp.chClose:
	}
}

// Stop .
//
//go:norace
func (tp *TaskPool) Stop() {
	tp.stopOnce.Do(func() {
		atomic.AddInt64(&tp.concurrent, tp.maxConcurrent)
		close(tp.chClose)
	})
}

// New creates and returns a TaskPool.
//
//go:norace
func New(maxConcurrent int, chQqueueSize int, v ...interface{}) *TaskPool {
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	tp := &TaskPool{
		maxConcurrent: int64(maxConcurrent),
		chQqueue:      make(chan func(), chQqueueSize),
		chSlot:        make(chan struct{}, 1),
		chClose:       make(chan struct{}),
	}
	tp.caller = func(f func()) {
		defer func() {
			if err := recover(); err != nil {
				const size = 64 << 10
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				logging.Error("taskpool call failed: %v\n%v\n", err, *(*string)(unsafe.Pointer(&buf)))
			}
		}()
		f()
	}
	if len(v) > 0 {
		if caller, ok := v[0].(func(f func())); ok {
			tp.caller = caller
		}
	}
	go tp.dispatch()
	return tp
}
