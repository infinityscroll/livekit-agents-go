// SPDX-License-Identifier: Apache-2.0

// Package stream provides cancellation-aware, bounded streams used throughout
// the SDK. The core channel uses no helper goroutine and allocates its ring once.
package stream

import (
	"context"
	"errors"
	"io"
	"iter"
	"sync"
)

var ErrClosed = errors.New("stream closed")

type Reader[T any] interface {
	Recv(context.Context) (T, error)
}

type Writer[T any] interface {
	Send(context.Context, T) error
	Close() error
	Abort(error) error
}

// Channel is a bounded MPMC stream with explicit error termination.
type Channel[T any] struct {
	mu       sync.Mutex
	buf      []T
	head     int
	len      int
	closed   bool
	closeErr error
	readable chan struct{}
	writable chan struct{}
	done     chan struct{}
}

func NewChannel[T any](capacity int) *Channel[T] {
	if capacity <= 0 {
		capacity = 1
	}
	return &Channel[T]{
		buf:      make([]T, capacity),
		readable: make(chan struct{}, 1),
		writable: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

func (c *Channel[T]) Send(ctx context.Context, value T) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		c.mu.Lock()
		if c.closed {
			err := c.terminalErrorLocked()
			c.mu.Unlock()
			return err
		}
		if c.len < len(c.buf) {
			idx := (c.head + c.len) % len(c.buf)
			c.buf[idx] = value
			c.len++
			c.signal(c.readable)
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-c.done:
		case <-c.writable:
		}
	}
}

func (c *Channel[T]) TrySend(value T) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.len == len(c.buf) {
		return false
	}
	idx := (c.head + c.len) % len(c.buf)
	c.buf[idx] = value
	c.len++
	c.signal(c.readable)
	return true
}

func (c *Channel[T]) Recv(ctx context.Context) (T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		c.mu.Lock()
		if c.len != 0 {
			value := c.buf[c.head]
			var zero T
			c.buf[c.head] = zero
			c.head = (c.head + 1) % len(c.buf)
			c.len--
			c.signal(c.writable)
			c.mu.Unlock()
			return value, nil
		}
		if c.closed {
			err := c.terminalErrorLocked()
			c.mu.Unlock()
			var zero T
			return zero, err
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			var zero T
			return zero, context.Cause(ctx)
		case <-c.done:
		case <-c.readable:
		}
	}
}

func (c *Channel[T]) TryRecv() (value T, ok bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.len != 0 {
		value = c.buf[c.head]
		var zero T
		c.buf[c.head] = zero
		c.head = (c.head + 1) % len(c.buf)
		c.len--
		c.signal(c.writable)
		return value, true, nil
	}
	if c.closed {
		return value, false, c.terminalErrorLocked()
	}
	return value, false, nil
}

func (c *Channel[T]) Close() error { return c.closeWith(nil) }

func (c *Channel[T]) Abort(err error) error {
	if err == nil {
		err = ErrClosed
	}
	return c.closeWith(err)
}

func (c *Channel[T]) closeWith(err error) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.closeErr = err
	close(c.done)
	c.mu.Unlock()
	return nil
}

func (c *Channel[T]) Closed() bool {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	return closed
}

func (c *Channel[T]) Len() int {
	c.mu.Lock()
	n := c.len
	c.mu.Unlock()
	return n
}

func (c *Channel[T]) Cap() int { return len(c.buf) }

func (c *Channel[T]) Range(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for {
			value, err := c.Recv(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					var zero T
					yield(zero, err)
				}
				return
			}
			if !yield(value, nil) {
				return
			}
		}
	}
}

func (c *Channel[T]) terminalErrorLocked() error {
	if c.closeErr != nil {
		return c.closeErr
	}
	return io.EOF
}

func (c *Channel[T]) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
