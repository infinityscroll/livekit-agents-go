// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// MultiInput fans dynamically added readers into one bounded output. A failed
// input is removed without terminating other inputs.
type MultiInput[T any] struct {
	out    *Channel[T]
	ctx    context.Context
	cancel context.CancelCauseFunc

	mu     sync.Mutex
	nextID uint64
	closed bool
	inputs map[string]context.CancelCauseFunc
	wg     sync.WaitGroup
	onErr  func(string, error)
}

func NewMultiInput[T any](parent context.Context, capacity int, onError func(string, error)) *MultiInput[T] {
	ctx, cancel := context.WithCancelCause(parent)
	return &MultiInput[T]{
		out:    NewChannel[T](capacity),
		ctx:    ctx,
		cancel: cancel,
		inputs: make(map[string]context.CancelCauseFunc),
		onErr:  onError,
	}
}

func (m *MultiInput[T]) Recv(ctx context.Context) (T, error) { return m.out.Recv(ctx) }

func (m *MultiInput[T]) Add(source Reader[T]) (string, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return "", ErrClosed
	}
	id := fmt.Sprintf("input-%d", m.nextID)
	m.nextID++
	ctx, cancel := context.WithCancelCause(m.ctx)
	m.inputs[id] = cancel
	m.wg.Add(1)
	m.mu.Unlock()
	go m.pump(ctx, id, source)
	return id, nil
}

func (m *MultiInput[T]) Remove(id string) {
	m.mu.Lock()
	cancel := m.inputs[id]
	delete(m.inputs, id)
	m.mu.Unlock()
	if cancel != nil {
		cancel(ErrClosed)
	}
}

func (m *MultiInput[T]) InputCount() int {
	m.mu.Lock()
	n := len(m.inputs)
	m.mu.Unlock()
	return n
}

func (m *MultiInput[T]) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	for id, cancel := range m.inputs {
		cancel(ErrClosed)
		delete(m.inputs, id)
	}
	m.cancel(ErrClosed)
	m.mu.Unlock()
	m.wg.Wait()
	return m.out.Close()
}

func (m *MultiInput[T]) pump(ctx context.Context, id string, source Reader[T]) {
	defer m.wg.Done()
	defer func() {
		m.mu.Lock()
		delete(m.inputs, id)
		m.mu.Unlock()
	}()
	for {
		value, err := source.Recv(ctx)
		if err != nil {
			if context.Cause(ctx) == nil && !errors.Is(err, io.EOF) && m.onErr != nil {
				m.onErr(id, err)
			}
			return
		}
		if err := m.out.Send(ctx, value); err != nil {
			return
		}
	}
}
