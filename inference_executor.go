// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/infinityscroll/livekit-agents-go/ipc"
)

const DefaultInferenceInitializeTimeout = 5 * time.Minute

var ErrInferenceExecutorClosed = errors.New("agents: inference executor is closed")

// LocalInferenceExecutor owns one instance of every registered inference
// runner. A worker shares this executor across jobs so a native model is loaded
// once per worker host rather than once per job process.
//
// Initialize eagerly constructs all registered runners. DoInference also
// initializes a runner lazily, which keeps standalone and console use useful
// without imposing import-time work. Close is idempotent and waits for accepted
// initialization and inference calls before closing runners.
type LocalInferenceExecutor struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	mu        sync.Mutex
	factories map[string]InferenceRunnerFactory
	slots     map[string]*inferenceRunnerSlot
	closed    bool
	ops       sync.WaitGroup

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

type inferenceRunnerSlot struct {
	method  string
	factory InferenceRunnerFactory

	mu           sync.Mutex
	initializing bool
	ready        chan struct{}
	initialized  bool
	runner       InferenceRunner
	err          error
}

// NewLocalInferenceExecutor snapshots factories. A nil map uses the process
// registry; an explicitly empty map creates a valid executor that reports
// unknown methods without allocating runner state.
func NewLocalInferenceExecutor(factories map[string]InferenceRunnerFactory) (*LocalInferenceExecutor, error) {
	if factories == nil {
		factories = RegisteredInferenceRunners()
	} else {
		factories = maps.Clone(factories)
	}
	for method, factory := range factories {
		if method == "" {
			return nil, errors.New("agents: inference runner method must not be empty")
		}
		if factory == nil {
			return nil, fmt.Errorf("agents: inference runner factory %q must not be nil", method)
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	executor := &LocalInferenceExecutor{
		ctx: ctx, cancel: cancel, factories: factories,
		closeDone: make(chan struct{}),
	}
	if len(factories) != 0 {
		executor.slots = make(map[string]*inferenceRunnerSlot, len(factories))
		for method, factory := range factories {
			executor.slots[method] = &inferenceRunnerSlot{method: method, factory: factory}
		}
	}
	return executor, nil
}

// Initialize eagerly initializes all runners concurrently. It is safe to call
// repeatedly and concurrently. Partial initialization is retained so Close can
// release every successfully initialized runner; callers should close the
// executor when Initialize returns an error.
func (e *LocalInferenceExecutor) Initialize(ctx context.Context) error {
	if e == nil {
		return errors.New("agents: inference executor is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := e.startOperation(); err != nil {
		return err
	}
	defer e.ops.Done()

	e.mu.Lock()
	methods := make([]string, 0, len(e.slots))
	for method := range e.slots {
		methods = append(methods, method)
	}
	e.mu.Unlock()
	sort.Strings(methods)

	errorsOut := make(chan error, len(methods))
	var wait sync.WaitGroup
	for _, method := range methods {
		slot := e.slot(method)
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := e.ensureRunner(ctx, slot); err != nil {
				errorsOut <- err
			}
		}()
	}
	wait.Wait()
	close(errorsOut)
	var result error
	for err := range errorsOut {
		result = errors.Join(result, err)
	}
	return result
}

// DoInference dispatches one request to a registered runner. Runner calls may
// execute concurrently, matching the TypeScript/Python shared executor. The
// request context is propagated through initialization and Run.
func (e *LocalInferenceExecutor) DoInference(ctx context.Context, method string, data any) (result any, err error) {
	if e == nil {
		return nil, errors.New("agents: inference executor is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if method == "" {
		return nil, errors.New("agents: inference method must not be empty")
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if err := e.startOperation(); err != nil {
		return nil, err
	}
	defer e.ops.Done()

	slot := e.slot(method)
	if slot == nil {
		return nil, &ipc.UnknownMethodError{Method: method}
	}
	runner, err := e.ensureRunner(ctx, slot)
	if err != nil {
		return nil, err
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	stopRun := context.AfterFunc(e.ctx, func() { cancelRun(ErrInferenceExecutorClosed) })
	defer func() {
		stopRun()
		cancelRun(nil)
	}()
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("agents: inference runner %q panicked: %v", method, recovered)
		}
	}()
	result, err = runner.Run(runCtx, data)
	if err != nil {
		return nil, fmt.Errorf("agents: inference of %s failed: %w", method, err)
	}
	return result, nil
}

func (e *LocalInferenceExecutor) slot(method string) *inferenceRunnerSlot {
	e.mu.Lock()
	slot := e.slots[method]
	e.mu.Unlock()
	return slot
}

func (e *LocalInferenceExecutor) startOperation() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrInferenceExecutorClosed
	}
	e.ops.Add(1)
	return nil
}

func (e *LocalInferenceExecutor) ensureRunner(ctx context.Context, slot *inferenceRunnerSlot) (InferenceRunner, error) {
	if slot == nil {
		return nil, errors.New("agents: inference runner slot is nil")
	}
	for {
		slot.mu.Lock()
		if slot.initialized {
			runner, err := slot.runner, slot.err
			slot.mu.Unlock()
			return runner, err
		}
		if slot.initializing {
			ready := slot.ready
			slot.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			case <-e.ctx.Done():
				return nil, ErrInferenceExecutorClosed
			case <-ready:
			}
			continue
		}
		slot.initializing = true
		slot.ready = make(chan struct{})
		ready := slot.ready
		slot.mu.Unlock()

		initializeCtx, cancelInitialize := context.WithCancelCause(ctx)
		stopInitialize := context.AfterFunc(e.ctx, func() {
			cancelInitialize(ErrInferenceExecutorClosed)
		})
		runner, err := constructInferenceRunner(slot.method, slot.factory)
		if err == nil {
			err = initializeInferenceRunner(initializeCtx, slot.method, runner)
		}
		stopInitialize()
		cancelInitialize(nil)
		// A canceled caller must not permanently poison a lazily initialized
		// slot. A worker's eager Initialize has its own long startup deadline and
		// will retain non-context failures as deterministic configuration errors.
		cacheFailure := err == nil || !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
		if err != nil && runner != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), DefaultShutdownProcessTimeout)
			_ = closeInferenceRunner(closeCtx, slot.method, runner)
			cancel()
			runner = nil
		}

		slot.mu.Lock()
		if cacheFailure {
			slot.runner, slot.err, slot.initialized = runner, err, true
		}
		slot.initializing = false
		close(ready)
		slot.mu.Unlock()
		return runner, err
	}
}

func constructInferenceRunner(method string, factory InferenceRunnerFactory) (runner InferenceRunner, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			runner = nil
			err = fmt.Errorf("agents: construct inference runner %s panicked: %v", method, recovered)
		}
	}()
	runner, err = factory()
	if err != nil {
		return nil, fmt.Errorf("agents: construct inference runner %s: %w", method, err)
	}
	if nilInferenceRunner(runner) {
		return nil, fmt.Errorf("agents: construct inference runner %s: nil runner", method)
	}
	return runner, nil
}

func initializeInferenceRunner(ctx context.Context, method string, runner InferenceRunner) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("agents: initialize inference runner %s panicked: %v", method, recovered)
		}
	}()
	if err := runner.Initialize(ctx); err != nil {
		return fmt.Errorf("agents: initialize inference runner %s: %w", method, err)
	}
	return nil
}

func closeInferenceRunner(ctx context.Context, method string, runner InferenceRunner) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("agents: close inference runner %s panicked: %v", method, recovered)
		}
	}()
	if err := runner.Close(ctx); err != nil {
		return fmt.Errorf("agents: close inference runner %s: %w", method, err)
	}
	return nil
}

// Close rejects new work, cancels initialization waiters, waits for accepted
// calls, then closes initialized runners concurrently. A caller deadline is
// reported, while cleanup continues and a later Close observes the final error.
func (e *LocalInferenceExecutor) Close(ctx context.Context) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		e.cancel(ErrInferenceExecutorClosed)
		e.mu.Unlock()
		go e.closeAsync()
	})
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-e.closeDone:
		return e.closeErr
	}
}

func (e *LocalInferenceExecutor) closeAsync() {
	e.ops.Wait()
	e.mu.Lock()
	slots := make([]*inferenceRunnerSlot, 0, len(e.slots))
	for _, slot := range e.slots {
		slots = append(slots, slot)
	}
	e.mu.Unlock()
	sort.Slice(slots, func(i, j int) bool { return slots[i].method < slots[j].method })

	errorsOut := make(chan error, len(slots))
	var wait sync.WaitGroup
	for _, slot := range slots {
		slot.mu.Lock()
		runner := slot.runner
		slot.runner = nil
		slot.mu.Unlock()
		if nilInferenceRunner(runner) {
			continue
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			closeCtx, cancel := context.WithTimeout(context.Background(), DefaultShutdownProcessTimeout)
			defer cancel()
			if err := closeInferenceRunner(closeCtx, slot.method, runner); err != nil {
				errorsOut <- err
			}
		}()
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		e.closeErr = errors.Join(e.closeErr, err)
	}
	close(e.closeDone)
}

// HasInferenceRunners reports whether the executor has any configured method
// without initializing it.
func (e *LocalInferenceExecutor) HasInferenceRunners() bool {
	if e == nil {
		return false
	}
	e.mu.Lock()
	has := len(e.factories) != 0
	e.mu.Unlock()
	return has
}

// Healthy reports deterministic initialization/closure failures without
// triggering lazy initialization. Workers include it in readiness checks.
func (e *LocalInferenceExecutor) Healthy() error {
	if e == nil {
		return errors.New("agents: inference executor is nil")
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrInferenceExecutorClosed
	}
	slots := make([]*inferenceRunnerSlot, 0, len(e.slots))
	for _, slot := range e.slots {
		slots = append(slots, slot)
	}
	e.mu.Unlock()
	for _, slot := range slots {
		slot.mu.Lock()
		initialized, err := slot.initialized, slot.err
		slot.mu.Unlock()
		if initialized && err != nil {
			return err
		}
	}
	return nil
}

func isNilInferenceExecutor(executor ipc.InferenceExecutor) bool {
	if executor == nil {
		return true
	}
	value := reflect.ValueOf(executor)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ ipc.InferenceExecutor = (*LocalInferenceExecutor)(nil)
