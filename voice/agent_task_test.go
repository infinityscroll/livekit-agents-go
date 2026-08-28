// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestAgentTaskOneShotAndRepeatedWait(t *testing.T) {
	task, err := NewAgentTask[string](AgentTaskOptions[struct{}]{})
	if err != nil {
		t.Fatal(err)
	}
	run := make(chan string, 1)
	go func() {
		result, err := task.Run(context.Background())
		if err != nil {
			t.Error(err)
		}
		run <- result
	}()
	if err := task.Complete("done"); err != nil {
		t.Fatal(err)
	}
	if result := <-run; result != "done" {
		t.Fatalf("run result = %q", result)
	}
	if result, err := task.Wait(context.Background()); err != nil || result != "done" {
		t.Fatalf("repeat wait = %q, %v", result, err)
	}
	if _, err := task.Run(context.Background()); !errors.Is(err, ErrAgentTaskAlreadyStarted) {
		t.Fatalf("second run = %v", err)
	}
	if err := task.Complete("again"); !errors.Is(err, ErrAgentTaskAlreadyComplete) {
		t.Fatalf("second completion = %v", err)
	}
}

func TestAgentTaskCallbacksAreAsyncOrderedAndPanicIsolated(t *testing.T) {
	t.Parallel()
	task, err := NewAgentTask[int](AgentTaskOptions[struct{}]{})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var order []int
	task.OnDone(func(*AgentTask[int, struct{}]) {
		close(started)
		<-release
		mu.Lock()
		order = append(order, 1)
		mu.Unlock()
	})
	task.OnDone(func(*AgentTask[int, struct{}]) { panic("callback bug") })
	done := make(chan struct{})
	task.OnDone(func(*AgentTask[int, struct{}]) {
		mu.Lock()
		order = append(order, 3)
		mu.Unlock()
		close(done)
	})
	if err := task.Complete(42); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first callback did not start")
	}
	select {
	case <-done:
		t.Fatal("callbacks were not serialized")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("later callback did not run after panic")
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []int{1, 3}; !reflect.DeepEqual(order, want) {
		t.Fatalf("callback order = %v, want %v", order, want)
	}
}

func TestAgentTaskWaitCancellationDoesNotComplete(t *testing.T) {
	task, err := NewAgentTask[int](AgentTaskOptions[struct{}]{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := task.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait = %v", err)
	}
	if task.Done() {
		t.Fatal("cancelled observer completed the task")
	}
}
