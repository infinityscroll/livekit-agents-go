// SPDX-License-Identifier: Apache-2.0

// Package ipc contains the public cross-process inference contract. Worker
// supervision and wire framing remain internal; applications depend only on
// this context-aware interface and can supply in-process, subprocess, or remote
// implementations without coupling models to a transport.
package ipc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

var ErrInferenceExecutorClosed = errors.New("ipc: inference executor is closed")

// InferenceExecutor is the Go equivalent of agents-js doInference(method,
// data). Context cancellation is mandatory in Go so a dead child process or
// native model can never pin job shutdown indefinitely.
type InferenceExecutor interface {
	DoInference(context.Context, string, any) (any, error)
}

// InferenceExecutorFunc adapts a function to InferenceExecutor.
type InferenceExecutorFunc func(context.Context, string, any) (any, error)

func (f InferenceExecutorFunc) DoInference(ctx context.Context, method string, data any) (any, error) {
	if f == nil {
		return nil, errors.New("ipc: inference executor function is nil")
	}
	return f(ctx, method, data)
}

// Request is a transport-neutral inference request. ID is optional for direct
// executors and required by multiplexed transports.
type Request struct {
	ID     string `json:"id,omitempty"`
	Method string `json:"method"`
	Data   any    `json:"data,omitempty"`
}

// Response carries either Data or Err. Transport implementations should map
// remote error codes to a typed error before returning from DoInference.
type Response struct {
	ID   string `json:"id,omitempty"`
	Data any    `json:"data,omitempty"`
	Err  error  `json:"-"`
}

// Dispatcher provides a deterministic, allocation-light method registry for
// local and subprocess executors. Registration is intended for startup; Seal
// turns the registry immutable and enables lock-free dispatch reads.
type Dispatcher struct {
	mu      sync.Mutex
	mutable map[string]InferenceExecutorFunc
	sealed  atomic.Pointer[map[string]InferenceExecutorFunc]
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{mutable: make(map[string]InferenceExecutorFunc)}
}

func (d *Dispatcher) Register(method string, handler InferenceExecutorFunc) error {
	if d == nil {
		return errors.New("ipc: inference dispatcher is nil")
	}
	if method == "" {
		return errors.New("ipc: inference method must not be empty")
	}
	if handler == nil {
		return errors.New("ipc: inference handler must not be nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sealed.Load() != nil {
		return errors.New("ipc: inference dispatcher is sealed")
	}
	if _, exists := d.mutable[method]; exists {
		return fmt.Errorf("ipc: inference method %q is already registered", method)
	}
	d.mutable[method] = handler
	return nil
}

func (d *Dispatcher) Seal() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sealed.Load() != nil {
		return
	}
	handlers := make(map[string]InferenceExecutorFunc, len(d.mutable))
	for method, handler := range d.mutable {
		handlers[method] = handler
	}
	d.sealed.CompareAndSwap(nil, &handlers)
}

func (d *Dispatcher) DoInference(ctx context.Context, method string, data any) (any, error) {
	if d == nil {
		return nil, errors.New("ipc: inference dispatcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	handlers := d.sealed.Load()
	if handlers == nil {
		return nil, errors.New("ipc: inference dispatcher must be sealed before use")
	}
	handler, exists := (*handlers)[method]
	if !exists {
		return nil, &UnknownMethodError{Method: method}
	}
	return handler(ctx, method, data)
}

type UnknownMethodError struct{ Method string }

func (e *UnknownMethodError) Error() string {
	if e == nil {
		return "ipc: unknown inference method"
	}
	return fmt.Sprintf("ipc: unknown inference method %q", e.Method)
}

// IsInferenceExecutor reports whether value is a non-nil executor without
// invoking it. It safely handles typed nil interfaces.
func IsInferenceExecutor(value any) bool {
	executor, ok := value.(InferenceExecutor)
	if !ok || executor == nil {
		return false
	}
	reflected := reflect.ValueOf(executor)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !reflected.IsNil()
	default:
		return true
	}
}
