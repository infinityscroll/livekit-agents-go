// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"log/slog"
	"sync"
)

// EventEmitter is a small synchronous typed emitter. Callbacks execute in
// registration order and outside the emitter lock. Subscribe and unsubscribe
// are safe to call concurrently with Emit.
type EventEmitter[T any] struct {
	mu     sync.RWMutex
	nextID uint64
	subs   []subscription[T]
}

type subscription[T any] struct {
	id uint64
	fn func(T)
}

func (e *EventEmitter[T]) Subscribe(fn func(T)) (unsubscribe func()) {
	if fn == nil {
		return func() {}
	}
	e.mu.Lock()
	id := e.nextID
	e.nextID++
	e.subs = append(e.subs, subscription[T]{id: id, fn: fn})
	e.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Lock()
			for i := range e.subs {
				if e.subs[i].id == id {
					copy(e.subs[i:], e.subs[i+1:])
					e.subs[len(e.subs)-1] = subscription[T]{}
					e.subs = e.subs[:len(e.subs)-1]
					break
				}
			}
			e.mu.Unlock()
		})
	}
}

func (e *EventEmitter[T]) Emit(event T) {
	e.mu.RLock()
	listeners := make([]func(T), len(e.subs))
	for i := range e.subs {
		listeners[i] = e.subs[i].fn
	}
	e.mu.RUnlock()
	for _, fn := range listeners {
		emitSafely(fn, event)
	}
}

func emitSafely[T any](fn func(T), event T) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("event subscriber panicked", "panic", recovered)
		}
	}()
	fn(event)
}

func (e *EventEmitter[T]) Len() int {
	e.mu.RLock()
	n := len(e.subs)
	e.mu.RUnlock()
	return n
}
