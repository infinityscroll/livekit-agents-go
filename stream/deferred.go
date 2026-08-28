// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"sync"
)

var ErrSourceAlreadySet = errors.New("stream source already set")

// Deferred presents a Reader before its actual source is available.
type Deferred[T any] struct {
	out      *Channel[T]
	mu       sync.Mutex
	set      bool
	cancel   context.CancelCauseFunc
	pumpDone chan struct{}
}

func NewDeferred[T any](capacity int) *Deferred[T] {
	return &Deferred[T]{out: NewChannel[T](capacity), pumpDone: make(chan struct{})}
}

func (d *Deferred[T]) Recv(ctx context.Context) (T, error) { return d.out.Recv(ctx) }

func (d *Deferred[T]) SetSource(parent context.Context, source Reader[T]) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.set {
		return ErrSourceAlreadySet
	}
	d.set = true
	ctx, cancel := context.WithCancelCause(parent)
	d.cancel = cancel
	go d.pump(ctx, source)
	return nil
}

func (d *Deferred[T]) Detach(cause error) {
	d.mu.Lock()
	cancel := d.cancel
	d.cancel = nil
	d.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

func (d *Deferred[T]) Wait() { <-d.pumpDone }

func (d *Deferred[T]) pump(ctx context.Context, source Reader[T]) {
	defer close(d.pumpDone)
	for {
		value, err := source.Recv(ctx)
		if err != nil {
			if context.Cause(ctx) != nil {
				_ = d.out.Close()
			} else {
				_ = d.out.Abort(err)
			}
			return
		}
		if err := d.out.Send(ctx, value); err != nil {
			return
		}
	}
}
