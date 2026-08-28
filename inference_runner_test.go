// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestInferenceRunnerRegistryAndLifecycle(t *testing.T) {
	method := "test/" + t.Name()
	t.Cleanup(func() {
		inferenceRunnerRegistry.Lock()
		delete(inferenceRunnerRegistry.factories, method)
		inferenceRunnerRegistry.Unlock()
	})
	var initialized, closed atomic.Int32
	err := RegisterInferenceRunner(method, func() (InferenceRunner, error) {
		return InferenceRunnerFuncs{
			InitializeFunc: func(context.Context) error { initialized.Add(1); return nil },
			RunFunc:        func(_ context.Context, input any) (any, error) { return fmt.Sprint(input) + "!", nil },
			CloseFunc:      func(context.Context) error { closed.Add(1); return nil },
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterInferenceRunner(method, func() (InferenceRunner, error) { return nil, nil }); !errors.Is(err, ErrInferenceRunnerExists) {
		t.Fatalf("duplicate error=%v", err)
	}
	runner, err := NewInferenceRunner(method)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := runner.Run(context.Background(), "ready"); err != nil || got != "ready!" {
		t.Fatalf("Run=%v err=%v", got, err)
	}
	if err := runner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if initialized.Load() != 1 || closed.Load() != 1 {
		t.Fatalf("lifecycle initialize=%d close=%d", initialized.Load(), closed.Load())
	}
	registered := RegisteredInferenceRunners()
	delete(registered, method)
	if _, err := NewInferenceRunner(method); err != nil {
		t.Fatalf("registry snapshot mutated source: %v", err)
	}
}

func TestInferenceRunnerConcurrentRegistration(t *testing.T) {
	prefix := "concurrent/" + t.Name() + "/"
	t.Cleanup(func() {
		inferenceRunnerRegistry.Lock()
		for method := range inferenceRunnerRegistry.factories {
			if len(method) >= len(prefix) && method[:len(prefix)] == prefix {
				delete(inferenceRunnerRegistry.factories, method)
			}
		}
		inferenceRunnerRegistry.Unlock()
	})
	const count = 32
	var wait sync.WaitGroup
	wait.Add(count)
	errorsFound := make(chan error, count)
	for index := range count {
		go func() {
			defer wait.Done()
			method := fmt.Sprintf("%s%d", prefix, index)
			if err := RegisterInferenceRunner(method, func() (InferenceRunner, error) { return InferenceRunnerFuncs{}, nil }); err != nil {
				errorsFound <- err
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	if _, err := NewInferenceRunner(prefix + "missing"); !errors.Is(err, ErrInferenceRunnerUnknown) {
		t.Fatalf("missing error=%v", err)
	}
}
