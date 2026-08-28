// SPDX-License-Identifier: Apache-2.0

package ipc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestDispatcherLifecycleAndConcurrentDispatch(t *testing.T) {
	dispatcher := NewDispatcher()
	var calls atomic.Int64
	if err := dispatcher.Register("eot", func(ctx context.Context, method string, data any) (any, error) {
		calls.Add(1)
		if method != "eot" || data.(int) < 0 {
			t.Fatalf("request = %q/%v", method, data)
		}
		return data.(int) * 2, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.DoInference(context.Background(), "eot", 1); err == nil {
		t.Fatal("unsealed dispatcher accepted work")
	}
	dispatcher.Seal()
	if err := dispatcher.Register("late", func(context.Context, string, any) (any, error) { return nil, nil }); err == nil {
		t.Fatal("sealed dispatcher accepted registration")
	}
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			result, err := dispatcher.DoInference(context.Background(), "eot", index)
			if err != nil || result != index*2 {
				t.Errorf("dispatch %d = %v, %v", index, result, err)
			}
		}(index)
	}
	wait.Wait()
	if calls.Load() != 64 {
		t.Fatalf("calls = %d", calls.Load())
	}
	var unknown *UnknownMethodError
	if _, err := dispatcher.DoInference(context.Background(), "missing", nil); !errors.As(err, &unknown) || unknown.Method != "missing" {
		t.Fatalf("unknown method error = %v", err)
	}
}

func TestDispatcherHonorsCancellationBeforeHandler(t *testing.T) {
	dispatcher := NewDispatcher()
	called := false
	if err := dispatcher.Register("method", func(context.Context, string, any) (any, error) {
		called = true
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	dispatcher.Seal()
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("stop")
	cancel(want)
	if _, err := dispatcher.DoInference(ctx, "method", nil); !errors.Is(err, want) {
		t.Fatalf("cancellation = %v", err)
	}
	if called {
		t.Fatal("handler ran after cancellation")
	}
}

func TestInferenceExecutorFuncAndTypedNil(t *testing.T) {
	var function InferenceExecutorFunc
	if IsInferenceExecutor(function) {
		t.Fatal("typed nil function reported as executor")
	}
	if _, err := function.DoInference(context.Background(), "x", nil); err == nil {
		t.Fatal("nil function did not fail")
	}
	function = func(context.Context, string, any) (any, error) { return "ok", nil }
	if !IsInferenceExecutor(function) {
		t.Fatal("function executor was not recognized")
	}
	if result, err := function.DoInference(context.Background(), "x", nil); err != nil || result != "ok" {
		t.Fatalf("function result = %v, %v", result, err)
	}
}
