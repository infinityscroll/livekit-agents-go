// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sync"
)

var (
	ErrInferenceRunnerExists  = errors.New("agents: inference runner is already registered")
	ErrInferenceRunnerUnknown = errors.New("agents: inference runner is not registered")
)

// InferenceRunner is the context-aware Go contract for a local model runner.
// A runner instance is owned by one inference executor and must tolerate an
// Initialize/Close pair even when no Run call succeeds.
type InferenceRunner interface {
	Initialize(context.Context) error
	Run(context.Context, any) (any, error)
	Close(context.Context) error
}

// InferenceRunnerFactory constructs a fresh runner in the process that will
// execute it. Factories replace JavaScript import paths because Go links the
// implementation into both the parent and supervised child binary.
type InferenceRunnerFactory func() (InferenceRunner, error)

// InferenceRunnerFuncs adapts three functions to InferenceRunner.
type InferenceRunnerFuncs struct {
	InitializeFunc func(context.Context) error
	RunFunc        func(context.Context, any) (any, error)
	CloseFunc      func(context.Context) error
}

func (r InferenceRunnerFuncs) Initialize(ctx context.Context) error {
	if r.InitializeFunc == nil {
		return nil
	}
	return r.InitializeFunc(contextOrBackground(ctx))
}

func (r InferenceRunnerFuncs) Run(ctx context.Context, input any) (any, error) {
	if r.RunFunc == nil {
		return nil, errors.New("agents: inference runner has no Run function")
	}
	return r.RunFunc(contextOrBackground(ctx), input)
}

func (r InferenceRunnerFuncs) Close(ctx context.Context) error {
	if r.CloseFunc == nil {
		return nil
	}
	return r.CloseFunc(contextOrBackground(ctx))
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

var inferenceRunnerRegistry struct {
	sync.RWMutex
	factories map[string]InferenceRunnerFactory
}

// RegisterInferenceRunner registers a local inference method. Registration is
// concurrency-safe, rejects duplicates, and allocates the registry only after
// the first feature package is actually imported.
//
// Register every production method before the worker executor pool starts.
// Process mode re-executes the same binary and requires the factory to be
// linked and registered along the child startup path as well; a runtime-only
// closure cannot be serialized into that process. ExecutorModeInProcess is the
// explicit development escape hatch for parent-only dynamic registrations.
func RegisterInferenceRunner(method string, factory InferenceRunnerFactory) error {
	if method == "" {
		return errors.New("agents: inference runner method must not be empty")
	}
	if factory == nil {
		return errors.New("agents: inference runner factory must not be nil")
	}
	inferenceRunnerRegistry.Lock()
	defer inferenceRunnerRegistry.Unlock()
	if inferenceRunnerRegistry.factories == nil {
		inferenceRunnerRegistry.factories = make(map[string]InferenceRunnerFactory)
	}
	if _, exists := inferenceRunnerRegistry.factories[method]; exists {
		return fmt.Errorf("%w: %s", ErrInferenceRunnerExists, method)
	}
	inferenceRunnerRegistry.factories[method] = factory
	return nil
}

// MustRegisterInferenceRunner is intended for package-level plugin
// registration before the executor pool starts and panics on a duplicate or
// invalid method. Package initialization naturally runs in the same-binary
// inference child required by process mode.
func MustRegisterInferenceRunner(method string, factory InferenceRunnerFactory) {
	if err := RegisterInferenceRunner(method, factory); err != nil {
		panic(err)
	}
}

// RegisteredInferenceRunners returns a defensive snapshot. An empty registry
// returns nil without allocating, keeping core-only process startup lean.
func RegisteredInferenceRunners() map[string]InferenceRunnerFactory {
	inferenceRunnerRegistry.RLock()
	if len(inferenceRunnerRegistry.factories) == 0 {
		inferenceRunnerRegistry.RUnlock()
		return nil
	}
	result := maps.Clone(inferenceRunnerRegistry.factories)
	inferenceRunnerRegistry.RUnlock()
	return result
}

// NewInferenceRunner creates one registered runner without holding the
// registry lock across user code.
func NewInferenceRunner(method string) (InferenceRunner, error) {
	inferenceRunnerRegistry.RLock()
	factory := inferenceRunnerRegistry.factories[method]
	inferenceRunnerRegistry.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("%w: %s", ErrInferenceRunnerUnknown, method)
	}
	runner, err := factory()
	if err != nil {
		return nil, fmt.Errorf("agents: construct inference runner %s: %w", method, err)
	}
	if nilInferenceRunner(runner) {
		return nil, fmt.Errorf("agents: construct inference runner %s: nil runner", method)
	}
	return runner, nil
}

func nilInferenceRunner(runner InferenceRunner) bool {
	if runner == nil {
		return true
	}
	value := reflect.ValueOf(runner)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ InferenceRunner = InferenceRunnerFuncs{}
