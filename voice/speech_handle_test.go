// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSpeechHandleWaitAndCallbacks(t *testing.T) {
	handle := NewSpeechHandle(SpeechHandleOptions{})
	var called atomic.Int32
	done := make(chan struct{})
	handle.AddDoneCallback(func(received *SpeechHandle) {
		if received != handle {
			t.Error("wrong callback handle")
		}
		called.Add(1)
		close(done)
	})
	want := errors.New("playout failed")
	handle.MarkDone(want)
	if err := handle.Wait(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Wait error = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("completion callback did not run")
	}
	if called.Load() != 1 {
		t.Fatalf("callback count = %d", called.Load())
	}
	late := make(chan struct{})
	handle.AddDoneCallback(func(*SpeechHandle) { close(late) })
	select {
	case <-late:
	case <-time.After(time.Second):
		t.Fatal("late callback did not run")
	}
}

func TestSpeechHandleCircularWait(t *testing.T) {
	handle := NewSpeechHandle(SpeechHandleOptions{})
	ctx := WithFunctionCallContext(context.Background(), handle, "lookup", false)
	var circular *SpeechHandleCircularWaitError
	if err := handle.Wait(ctx); !errors.As(err, &circular) || circular.FunctionName != "lookup" {
		t.Fatalf("error = %#v", err)
	}
	nonBlocking, cancel := context.WithTimeout(WithFunctionCallContext(context.Background(), handle, "lookup", true), time.Second)
	defer cancel()
	handle.MarkDone(nil)
	if err := handle.Wait(nonBlocking); err != nil {
		t.Fatal(err)
	}
}

func TestSpeechHandleInterruptionWatchdog(t *testing.T) {
	handle := NewSpeechHandle(SpeechHandleOptions{InterruptTimeout: 10 * time.Millisecond})
	owned, unregister := handle.OwnContext(context.Background())
	defer unregister()
	if err := handle.Interrupt(false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-owned.Done():
	case <-time.After(time.Second):
		t.Fatal("owned context was not cancelled")
	}
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("watchdog completion = %v", err)
	}
}

func TestSpeechHandleGenerationAuthorization(t *testing.T) {
	handle := NewSpeechHandle(SpeechHandleOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready := make(chan struct{})
	go func() {
		if err := handle.WaitForAuthorization(ctx); err != nil {
			t.Error(err)
		}
		close(ready)
	}()
	index := handle.AuthorizeGeneration()
	<-ready
	if index != 0 {
		t.Fatalf("generation index = %d", index)
	}
	waited := make(chan struct{})
	go func() {
		if err := handle.WaitForGeneration(ctx, index); err != nil {
			t.Error(err)
		}
		close(waited)
	}()
	if err := handle.MarkGenerationDone(); err != nil {
		t.Fatal(err)
	}
	<-waited
}

func TestSpeechHandleWaitForScheduledAndSteps(t *testing.T) {
	handle := NewSpeechHandle(SpeechHandleOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- handle.WaitForScheduled(ctx) }()
	handle.MarkScheduled()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Marking scheduled twice is deliberately idempotent.
	handle.MarkScheduled()
	if got := handle.IncrementSteps(); got != 2 || handle.NumSteps() != 2 {
		t.Fatalf("steps = %d / %d", got, handle.NumSteps())
	}
}

func TestSpeechQueuePriorityAndFIFO(t *testing.T) {
	queue := NewSpeechQueue(8)
	lowA := NewSpeechHandle(SpeechHandleOptions{})
	highA := NewSpeechHandle(SpeechHandleOptions{})
	highB := NewSpeechHandle(SpeechHandleOptions{})
	lowB := NewSpeechHandle(SpeechHandleOptions{})
	for _, item := range []struct {
		h *SpeechHandle
		p int
	}{{lowA, 0}, {highA, 10}, {highB, 10}, {lowB, 0}} {
		if err := queue.Push(context.Background(), item.h, item.p); err != nil {
			t.Fatal(err)
		}
	}
	want := []*SpeechHandle{highA, highB, lowA, lowB}
	if got := queue.Ordered(); len(got) != len(want) {
		t.Fatalf("snapshot length = %d", len(got))
	} else {
		for index := range want {
			if got[index] != want[index] {
				t.Fatalf("snapshot[%d] wrong handle", index)
			}
		}
	}
	for index := range want {
		got, err := queue.Pop(context.Background())
		if err != nil || got != want[index] {
			t.Fatalf("pop[%d] = %p, %v", index, got, err)
		}
	}
}

func TestSpeechQueueConcurrentProducers(t *testing.T) {
	queue := NewSpeechQueue(16)
	const count = 100
	var wg sync.WaitGroup
	for producer := 0; producer < 4; producer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := 0; index < count/4; index++ {
				if err := queue.Push(context.Background(), NewSpeechHandle(SpeechHandleOptions{}), SpeechPriorityNormal); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	consumed := make(chan struct{})
	go func() {
		for index := 0; index < count; index++ {
			if _, err := queue.Pop(context.Background()); err != nil {
				t.Error(err)
			}
		}
		close(consumed)
	}()
	wg.Wait()
	select {
	case <-consumed:
	case <-time.After(time.Second):
		t.Fatal("consumer stalled")
	}
}
