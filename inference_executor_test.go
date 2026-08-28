// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/agents-go/ipc"
)

type executorTestRunner struct {
	initialized atomic.Int64
	runs        atomic.Int64
	closed      atomic.Int64
	initBlock   <-chan struct{}
	runBlock    <-chan struct{}
}

func (r *executorTestRunner) Initialize(ctx context.Context) error {
	r.initialized.Add(1)
	if r.initBlock != nil {
		select {
		case <-r.initBlock:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

func (r *executorTestRunner) Run(ctx context.Context, input any) (any, error) {
	r.runs.Add(1)
	if r.runBlock != nil {
		select {
		case <-r.runBlock:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	return input, nil
}

func (r *executorTestRunner) Close(context.Context) error {
	r.closed.Add(1)
	return nil
}

func TestLocalInferenceExecutorInitializesOnceAndDispatchesConcurrently(t *testing.T) {
	runner := new(executorTestRunner)
	var constructed atomic.Int64
	executor, err := NewLocalInferenceExecutor(map[string]InferenceRunnerFactory{
		"echo": func() (InferenceRunner, error) {
			constructed.Add(1)
			return runner, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = executor.Close(context.Background()) })

	const requests = 32
	var wait sync.WaitGroup
	errorsOut := make(chan error, requests)
	for value := range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			got, err := executor.DoInference(context.Background(), "echo", value)
			if err != nil {
				errorsOut <- err
				return
			}
			if got != value {
				errorsOut <- errors.New("inference result mismatch")
			}
		}()
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
	if got := constructed.Load(); got != 1 {
		t.Fatalf("constructed = %d, want 1", got)
	}
	if got := runner.initialized.Load(); got != 1 {
		t.Fatalf("initialized = %d, want 1", got)
	}
	if got := runner.runs.Load(); got != requests {
		t.Fatalf("runs = %d, want %d", got, requests)
	}
	if err := executor.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := runner.closed.Load(); got != 1 {
		t.Fatalf("closed = %d, want 1", got)
	}
	if err := executor.Healthy(); !errors.Is(err, ErrInferenceExecutorClosed) {
		t.Fatalf("Healthy after Close = %v", err)
	}
}

func TestLocalInferenceExecutorInitializeAllAndUnknownMethod(t *testing.T) {
	first, second := new(executorTestRunner), new(executorTestRunner)
	executor, err := NewLocalInferenceExecutor(map[string]InferenceRunnerFactory{
		"first":  func() (InferenceRunner, error) { return first, nil },
		"second": func() (InferenceRunner, error) { return second, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first.initialized.Load() != 1 || second.initialized.Load() != 1 {
		t.Fatal("Initialize did not initialize every runner")
	}
	_, err = executor.DoInference(context.Background(), "missing", nil)
	var unknown *ipc.UnknownMethodError
	if !errors.As(err, &unknown) || unknown.Method != "missing" {
		t.Fatalf("unknown method error = %v", err)
	}
	if err := executor.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLocalInferenceExecutorCanceledInitializationCanRetry(t *testing.T) {
	block := make(chan struct{})
	runner := &executorTestRunner{initBlock: block}
	var constructed atomic.Int64
	executor, err := NewLocalInferenceExecutor(map[string]InferenceRunnerFactory{
		"slow": func() (InferenceRunner, error) {
			constructed.Add(1)
			return runner, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := executor.DoInference(ctx, "slow", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first inference error = %v", err)
	}
	close(block)
	if _, err := executor.DoInference(context.Background(), "slow", "ok"); err != nil {
		t.Fatalf("retry inference: %v", err)
	}
	if got := constructed.Load(); got != 2 {
		t.Fatalf("constructed = %d, want 2 after canceled initialization", got)
	}
	if err := executor.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLocalInferenceExecutorCloseCancelsAcceptedRunAndRejectsNewWork(t *testing.T) {
	block := make(chan struct{})
	runner := &executorTestRunner{runBlock: block}
	executor, err := NewLocalInferenceExecutor(map[string]InferenceRunnerFactory{
		"slow": func() (InferenceRunner, error) { return runner, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, err := executor.DoInference(context.Background(), "slow", nil)
		runDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for runner.runs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- executor.Close(context.Background()) }()
	deadline = time.Now().Add(time.Second)
	for {
		executor.mu.Lock()
		closed := executor.closed
		executor.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("executor did not reject work after Close")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := executor.DoInference(context.Background(), "slow", nil); !errors.Is(err, ErrInferenceExecutorClosed) {
		t.Fatalf("inference after Close = %v", err)
	}
	if err := <-runDone; !errors.Is(err, ErrInferenceExecutorClosed) {
		t.Fatalf("accepted inference error = %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	close(block)
	if runner.closed.Load() != 1 {
		t.Fatalf("runner close count = %d", runner.closed.Load())
	}
}

func TestLocalInferenceExecutorPanicIsolation(t *testing.T) {
	executor, err := NewLocalInferenceExecutor(map[string]InferenceRunnerFactory{
		"panic": func() (InferenceRunner, error) {
			return InferenceRunnerFuncs{RunFunc: func(context.Context, any) (any, error) {
				panic("boom")
			}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.DoInference(context.Background(), "panic", nil); err == nil {
		t.Fatal("panic was not converted to an error")
	}
	if err := executor.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
