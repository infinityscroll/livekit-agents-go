// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
)

// realtimeQueue is a fixed-allocation queue for RTC callback paths. Writers
// never block: when full, the oldest media item is discarded so latency stays
// bounded. Control events use RTCBridge instead and fail on overflow.
type realtimeQueue[T any] struct {
	mu       sync.Mutex
	buf      []T
	head     int
	len      int
	closed   bool
	closeErr error
	readable chan struct{}
	done     chan struct{}
	dropped  atomic.Uint64
}

func newRealtimeQueue[T any](capacity int) *realtimeQueue[T] {
	if capacity <= 0 {
		capacity = 1
	}
	return &realtimeQueue[T]{
		buf:      make([]T, capacity),
		readable: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

func (q *realtimeQueue[T]) Push(value T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	dropped := false
	if q.len == len(q.buf) {
		var zero T
		q.buf[q.head] = zero
		q.head = (q.head + 1) % len(q.buf)
		q.len--
		q.dropped.Add(1)
		dropped = true
	}
	index := (q.head + q.len) % len(q.buf)
	q.buf[index] = value
	q.len++
	select {
	case q.readable <- struct{}{}:
	default:
	}
	return !dropped
}

func (q *realtimeQueue[T]) Recv(ctx context.Context) (T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		q.mu.Lock()
		if q.len != 0 {
			value := q.buf[q.head]
			var zero T
			q.buf[q.head] = zero
			q.head = (q.head + 1) % len(q.buf)
			q.len--
			q.mu.Unlock()
			return value, nil
		}
		if q.closed {
			err := q.closeErr
			q.mu.Unlock()
			var zero T
			if err == nil {
				err = io.EOF
			}
			return zero, err
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			var zero T
			return zero, context.Cause(ctx)
		case <-q.done:
		case <-q.readable:
		}
	}
}

func (q *realtimeQueue[T]) Close(err error) {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.closeErr = err
		close(q.done)
	}
	q.mu.Unlock()
}

func (q *realtimeQueue[T]) Dropped() uint64 { return q.dropped.Load() }
