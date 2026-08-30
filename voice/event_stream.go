// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"io"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/infinityscroll/livekit-agents-go/stream"
)

const (
	DefaultEventQueueCapacity      = 128
	DefaultEventSubscriberCapacity = 32
)

var ErrEventCallbackPanic = errors.New("voice event callback panicked")

// EventCallbackError reports a recovered callback panic. A user callback can
// therefore never take down a media or session goroutine.
type EventCallbackError struct {
	Panic any
	Stack []byte
}

func (e *EventCallbackError) Error() string { return "voice event callback panicked" }
func (e *EventCallbackError) Unwrap() error { return ErrEventCallbackPanic }

type EventBusOptions struct {
	QueueCapacity int
	// OnCallbackError runs on the affected callback's pump goroutine.
	OnCallbackError func(error)
}

type EventSubscriptionOptions struct {
	Capacity int
	// DropTelemetry permits dropping only coalescible telemetry events when
	// this subscriber is full. Conversation, state, speech, error, and close
	// events always apply backpressure and remain lossless.
	DropTelemetry bool
}

// EventSubscription is an explicitly closable, bounded event stream.
type EventSubscription struct {
	out     *stream.Channel[Event]
	bus     *EventBus
	id      uint64
	dropped atomic.Uint64
	once    sync.Once
}

func (s *EventSubscription) Recv(ctx context.Context) (Event, error) {
	return s.out.Recv(ctx)
}

func (s *EventSubscription) Dropped() uint64 { return s.dropped.Load() }

func (s *EventSubscription) Close() error {
	s.once.Do(func() { s.bus.remove(s.id) })
	return nil
}

type eventSubscriber struct {
	sub           *EventSubscription
	dropTelemetry bool
}

type eventEnvelope struct {
	event Event
	ack   chan error
}

// EventBus serializes all session events through one bounded dispatcher. It
// owns a single goroutine regardless of the number of stream subscribers;
// callback subscriptions add one isolated pump apiece.
type EventBus struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	in     *stream.Channel[eventEnvelope]

	mu            sync.RWMutex
	subs          map[uint64]*eventSubscriber
	nextID        uint64
	closed        bool
	callbackError func(error)

	dispatchDone chan struct{}
	callbackWG   sync.WaitGroup
	closeOnce    sync.Once
}

func NewEventBus(parent context.Context, options EventBusOptions) *EventBus {
	if parent == nil {
		parent = context.Background()
	}
	capacity := options.QueueCapacity
	if capacity <= 0 {
		capacity = DefaultEventQueueCapacity
	}
	ctx, cancel := context.WithCancelCause(parent)
	bus := &EventBus{
		ctx: ctx, cancel: cancel, in: stream.NewChannel[eventEnvelope](capacity),
		subs: make(map[uint64]*eventSubscriber), callbackError: options.OnCallbackError,
		dispatchDone: make(chan struct{}),
	}
	go bus.dispatch()
	return bus
}

// Publish enqueues an event in global session order and applies bounded
// backpressure. It never invokes user code on the caller's goroutine.
func (b *EventBus) Publish(ctx context.Context, event Event) error {
	if event == nil {
		return errors.New("voice event must not be nil")
	}
	return b.in.Send(ctx, eventEnvelope{event: event})
}

// PublishAndWait applies backpressure until event has been dispatched to every
// active subscriber queue. It is used for terminal/control events that must be
// visible before the producer closes the bus; callbacks still run on their
// isolated subscription goroutines.
func (b *EventBus) PublishAndWait(ctx context.Context, event Event) error {
	if event == nil {
		return errors.New("voice event must not be nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ack := make(chan error, 1)
	if err := b.in.Send(ctx, eventEnvelope{event: event, ack: ack}); err != nil {
		return err
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// TryPublish is intended for metrics producers that must not block. It returns
// false when the global queue is full or the bus is closed.
func (b *EventBus) TryPublish(event Event) bool {
	return event != nil && b.in.TrySend(eventEnvelope{event: event})
}

func (b *EventBus) Subscribe(options EventSubscriptionOptions) (*EventSubscription, error) {
	capacity := options.Capacity
	if capacity <= 0 {
		capacity = DefaultEventSubscriberCapacity
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, stream.ErrClosed
	}
	id := b.nextID
	b.nextID++
	sub := &EventSubscription{out: stream.NewChannel[Event](capacity), bus: b, id: id}
	b.subs[id] = &eventSubscriber{sub: sub, dropTelemetry: options.DropTelemetry}
	b.mu.Unlock()
	return sub, nil
}

// OnEvent runs fn in registration order for its own subscription, never on a
// producer or dispatcher goroutine. The returned function is idempotent.
func (b *EventBus) OnEvent(fn func(Event), options EventSubscriptionOptions) (func(), error) {
	if fn == nil {
		return func() {}, errors.New("voice event callback must not be nil")
	}
	sub, err := b.Subscribe(options)
	if err != nil {
		return nil, err
	}
	b.callbackWG.Add(1)
	go func() {
		defer b.callbackWG.Done()
		defer sub.Close()
		for {
			event, err := sub.Recv(b.ctx)
			if err != nil {
				return
			}
			b.invokeCallback(fn, event)
		}
	}()
	return func() { _ = sub.Close() }, nil
}

func (b *EventBus) invokeCallback(fn func(Event), event Event) {
	defer func() {
		if recovered := recover(); recovered != nil && b.callbackError != nil {
			b.callbackError(&EventCallbackError{Panic: recovered, Stack: debug.Stack()})
		}
	}()
	fn(event)
}

func (b *EventBus) dispatch() {
	defer close(b.dispatchDone)
	for {
		envelope, err := b.in.Recv(b.ctx)
		if err != nil {
			return
		}
		event := envelope.event
		var dispatchErr error
		b.mu.RLock()
		subs := make([]*eventSubscriber, 0, len(b.subs))
		for _, sub := range b.subs {
			subs = append(subs, sub)
		}
		b.mu.RUnlock()
		for _, target := range subs {
			if target.dropTelemetry && eventCanDrop(event.Type()) {
				if !target.sub.out.TrySend(event) {
					target.sub.dropped.Add(1)
				}
				continue
			}
			if err := target.sub.out.Send(b.ctx, event); err != nil && !errors.Is(err, stream.ErrClosed) && !errors.Is(err, io.EOF) {
				if context.Cause(b.ctx) != nil {
					dispatchErr = context.Cause(b.ctx)
					break
				}
			}
		}
		if envelope.ack != nil {
			envelope.ack <- dispatchErr
		}
		if dispatchErr != nil {
			return
		}
	}
}

func eventCanDrop(kind EventType) bool {
	switch kind {
	case EventMetricsCollected, EventSessionUsageUpdated, EventDebugMessage, EventEOTPrediction:
		return true
	default:
		return false
	}
}

func (b *EventBus) remove(id uint64) {
	b.mu.Lock()
	target := b.subs[id]
	delete(b.subs, id)
	b.mu.Unlock()
	if target != nil {
		_ = target.sub.out.Close()
	}
}

func (b *EventBus) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		_ = b.in.Close()
		b.cancel(stream.ErrClosed)
		<-b.dispatchDone

		b.mu.Lock()
		subs := b.subs
		b.subs = make(map[uint64]*eventSubscriber)
		b.mu.Unlock()
		for _, target := range subs {
			_ = target.sub.out.Close()
		}
		b.callbackWG.Wait()
	})
	return nil
}
