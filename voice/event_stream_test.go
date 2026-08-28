// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestEventBusPreservesOrder(t *testing.T) {
	bus := NewEventBus(context.Background(), EventBusOptions{QueueCapacity: 4})
	defer bus.Close()
	sub, err := bus.Subscribe(EventSubscriptionOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	events := []Event{
		NewUserStateChangedEvent(UserStateListening, UserStateSpeaking, time.UnixMilli(1)),
		NewAgentStateChangedEvent(AgentStateIdle, AgentStateThinking, time.UnixMilli(2)),
		NewCloseEvent(CloseReasonUserInitiated, nil, time.UnixMilli(3)),
	}
	for _, event := range events {
		if err := bus.Publish(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	for index, want := range events {
		got, err := sub.Recv(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got.Type() != want.Type() || !got.Time().Equal(want.Time()) {
			t.Fatalf("event %d = %s at %s", index, got.Type(), got.Time())
		}
	}
}

func TestEventBusDropsOnlyTelemetry(t *testing.T) {
	bus := NewEventBus(context.Background(), EventBusOptions{QueueCapacity: 4})
	defer bus.Close()
	sub, err := bus.Subscribe(EventSubscriptionOptions{Capacity: 1, DropTelemetry: true})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	first := DebugMessageEvent{EventBase: newEventBase(EventDebugMessage, time.Now())}
	second := DebugMessageEvent{EventBase: newEventBase(EventDebugMessage, time.Now())}
	if err := bus.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for sub.Dropped() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if sub.Dropped() != 1 {
		t.Fatalf("dropped = %d", sub.Dropped())
	}

	// A close event remains lossless and waits until the subscriber drains.
	done := make(chan error, 1)
	go func() {
		done <- bus.Publish(context.Background(), NewCloseEvent(CloseReasonUserInitiated, nil, time.Now()))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		// Publish may return once accepted by the global queue; dispatch still
		// applies subscriber backpressure.
	case <-time.After(time.Second):
		t.Fatal("publish did not enqueue")
	}
	if _, err := sub.Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := sub.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Type() != EventClose {
		t.Fatalf("lossless event = %s", got.Type())
	}
}

func TestEventBusRecoversCallbackPanic(t *testing.T) {
	panicReported := make(chan error, 1)
	bus := NewEventBus(context.Background(), EventBusOptions{OnCallbackError: func(err error) { panicReported <- err }})
	var mu sync.Mutex
	seen := 0
	unsubscribe, err := bus.OnEvent(func(Event) {
		mu.Lock()
		seen++
		current := seen
		mu.Unlock()
		if current == 1 {
			panic("boom")
		}
	}, EventSubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	if err := bus.Publish(context.Background(), NewUserStateChangedEvent(UserStateListening, UserStateSpeaking, time.Now())); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(context.Background(), NewUserStateChangedEvent(UserStateSpeaking, UserStateListening, time.Now())); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-panicReported:
		if !errors.Is(err, ErrEventCallbackPanic) {
			t.Fatalf("panic error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("callback panic was not reported")
	}
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		count := seen
		mu.Unlock()
		if count == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("callback stopped after panic; seen = %d", count)
		}
		time.Sleep(time.Millisecond)
	}
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEventSubscriptionCloseUnblocksRecv(t *testing.T) {
	bus := NewEventBus(context.Background(), EventBusOptions{})
	sub, err := bus.Subscribe(EventSubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Recv(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("recv after close = %v", err)
	}
	_ = bus.Close()
}

func TestEventBusPublishAndWaitEstablishesCloseBarrier(t *testing.T) {
	bus := NewEventBus(context.Background(), EventBusOptions{QueueCapacity: 4})
	sub, err := bus.Subscribe(EventSubscriptionOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := NewUserStateChangedEvent(UserStateListening, UserStateSpeaking, time.Now())
	if err := bus.PublishAndWait(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	closeEvent := NewCloseEvent(CloseReasonUserInitiated, nil, time.Now())
	dispatched := make(chan error, 1)
	go func() { dispatched <- bus.PublishAndWait(context.Background(), closeEvent) }()
	select {
	case err := <-dispatched:
		t.Fatalf("close passed a full subscriber before it drained: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if got, err := sub.Recv(context.Background()); err != nil || got.Type() != EventUserStateChanged {
		t.Fatalf("first event = %T, %v", got, err)
	}
	select {
	case err := <-dispatched:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close event was not acknowledged")
	}
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := sub.Recv(context.Background())
	if err != nil || got.Type() != EventClose {
		t.Fatalf("terminal event after bus close = %T, %v", got, err)
	}
}
