// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

type ConnectionPoolOptions[T comparable] struct {
	MaxSessionDuration time.Duration
	MarkRefreshedOnGet bool
	Connect            func(context.Context) (T, error)
	Close              func(context.Context, T) error
	ConnectTimeout     time.Duration
}

// ConnectionPool serializes establishment, reuses connections in insertion
// order, and never returns an expired/failed connection.
type ConnectionPool[T comparable] struct {
	options ConnectionPoolOptions[T]
	gate    chan struct{}

	mu          sync.Mutex
	connections map[T]time.Time
	available   []T
	toClose     []T
	closed      bool
	prewarming  bool
	prewarmDone chan struct{}
}

func NewConnectionPool[T comparable](options ConnectionPoolOptions[T]) (*ConnectionPool[T], error) {
	if options.Connect == nil {
		return nil, errors.New("connection pool connect callback is required")
	}
	if options.ConnectTimeout <= 0 {
		options.ConnectTimeout = 10 * time.Second
	}
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &ConnectionPool[T]{options: options, gate: gate, connections: make(map[T]time.Time)}, nil
}

func (p *ConnectionPool[T]) lock(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-p.gate:
		return nil
	}
}
func (p *ConnectionPool[T]) unlock() { p.gate <- struct{}{} }

func (p *ConnectionPool[T]) Get(ctx context.Context) (T, error) {
	var zero T
	if err := p.lock(ctx); err != nil {
		return zero, err
	}
	defer p.unlock()
	if err := p.drainLocked(ctx); err != nil {
		return zero, err
	}
	now := time.Now()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return zero, errors.New("connection pool closed")
		}
		if len(p.available) == 0 {
			p.mu.Unlock()
			break
		}
		conn := p.available[0]
		copy(p.available, p.available[1:])
		p.available[len(p.available)-1] = zero
		p.available = p.available[:len(p.available)-1]
		created, exists := p.connections[conn]
		expired := exists && p.options.MaxSessionDuration > 0 && now.Sub(created) > p.options.MaxSessionDuration
		if expired {
			delete(p.connections, conn)
		}
		if exists && !expired && p.options.MarkRefreshedOnGet {
			p.connections[conn] = now
		}
		p.mu.Unlock()
		if !exists {
			continue
		}
		if expired {
			if err := p.closeOne(ctx, conn); err != nil {
				return zero, err
			}
			continue
		}
		return conn, nil
	}
	connectCtx, cancel := context.WithTimeout(ctx, p.options.ConnectTimeout)
	defer cancel()
	conn, err := p.options.Connect(connectCtx)
	if err != nil {
		return zero, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = p.closeOne(ctx, conn)
		return zero, errors.New("connection pool closed")
	}
	p.connections[conn] = time.Now()
	p.mu.Unlock()
	return conn, nil
}

func (p *ConnectionPool[T]) Put(conn T) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	if _, exists := p.connections[conn]; !exists {
		return false
	}
	if slices.Contains(p.available, conn) {
		return true
	}
	p.available = append(p.available, conn)
	return true
}

func (p *ConnectionPool[T]) Remove(ctx context.Context, conn T) error {
	if err := p.lock(ctx); err != nil {
		return err
	}
	defer p.unlock()
	p.mu.Lock()
	_, exists := p.connections[conn]
	delete(p.connections, conn)
	p.available = slices.DeleteFunc(p.available, func(candidate T) bool { return candidate == conn })
	p.toClose = slices.DeleteFunc(p.toClose, func(candidate T) bool { return candidate == conn })
	p.mu.Unlock()
	if exists {
		return p.closeOne(ctx, conn)
	}
	return nil
}

func (p *ConnectionPool[T]) Invalidate() {
	p.mu.Lock()
	for conn := range p.connections {
		p.toClose = append(p.toClose, conn)
	}
	clear(p.connections)
	p.available = p.available[:0]
	p.mu.Unlock()
}

func (p *ConnectionPool[T]) WithConnection(ctx context.Context, fn func(context.Context, T) error) error {
	conn, err := p.Get(ctx)
	if err != nil {
		return err
	}
	if err := fn(ctx, conn); err != nil {
		return errors.Join(err, p.Remove(context.WithoutCancel(ctx), conn))
	}
	if !p.Put(conn) {
		return p.closeOne(context.WithoutCancel(ctx), conn)
	}
	return nil
}

func WithConnectionResult[T comparable, R any](ctx context.Context, pool *ConnectionPool[T], fn func(context.Context, T) (R, error)) (R, error) {
	var zero R
	conn, err := pool.Get(ctx)
	if err != nil {
		return zero, err
	}
	result, err := fn(ctx, conn)
	if err != nil {
		return zero, errors.Join(err, pool.Remove(context.WithoutCancel(ctx), conn))
	}
	if !pool.Put(conn) {
		return zero, pool.closeOne(context.WithoutCancel(ctx), conn)
	}
	return result, nil
}

func (p *ConnectionPool[T]) Prewarm(ctx context.Context) {
	p.mu.Lock()
	if p.closed || p.prewarming || len(p.connections) != 0 {
		p.mu.Unlock()
		return
	}
	p.prewarming = true
	done := make(chan struct{})
	p.prewarmDone = done
	p.mu.Unlock()
	go func() {
		defer close(done)
		conn, err := p.Get(ctx)
		if err == nil {
			p.Put(conn)
		}
		p.mu.Lock()
		p.prewarming = false
		p.mu.Unlock()
	}()
}

func (p *ConnectionPool[T]) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		done := p.prewarmDone
		p.mu.Unlock()
		if done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		return nil
	}
	p.closed = true
	for conn := range p.connections {
		p.toClose = append(p.toClose, conn)
	}
	clear(p.connections)
	p.available = nil
	done := p.prewarmDone
	p.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	if err := p.lock(ctx); err != nil {
		return err
	}
	defer p.unlock()
	return p.drainLocked(ctx)
}

func (p *ConnectionPool[T]) drainLocked(ctx context.Context) error {
	p.mu.Lock()
	pending := slices.Clone(p.toClose)
	p.toClose = p.toClose[:0]
	p.mu.Unlock()
	var errs []error
	for _, conn := range pending {
		if err := p.closeOne(ctx, conn); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *ConnectionPool[T]) closeOne(ctx context.Context, conn T) error {
	if p.options.Close == nil {
		return nil
	}
	if err := p.options.Close(ctx, conn); err != nil {
		return fmt.Errorf("close pooled connection: %w", err)
	}
	return nil
}

func (p *ConnectionPool[T]) Len() int { p.mu.Lock(); n := len(p.connections); p.mu.Unlock(); return n }
