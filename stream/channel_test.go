// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestChannelBackpressureAndClose(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := NewChannel[int](1)
	if err := ch.Send(ctx, 1); err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() { sent <- ch.Send(ctx, 2) }()
	select {
	case err := <-sent:
		t.Fatalf("second send completed before receive: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if got, err := ch.Recv(ctx); err != nil || got != 1 {
		t.Fatalf("Recv() = %d, %v", got, err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := ch.Recv(ctx); err != nil || got != 2 {
		t.Fatalf("draining Recv() = %d, %v", got, err)
	}
	if _, err := ch.Recv(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal Recv() error = %v", err)
	}
}

func TestChannelAbortWakesAllWaiters(t *testing.T) {
	t.Parallel()
	ch := NewChannel[int](1)
	want := errors.New("provider failed")
	const readers = 8
	var wg sync.WaitGroup
	wg.Add(readers)
	errs := make(chan error, readers)
	for range readers {
		go func() {
			defer wg.Done()
			_, err := ch.Recv(context.Background())
			errs <- err
		}()
	}
	time.Sleep(time.Millisecond)
	_ = ch.Abort(want)
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, want) {
			t.Fatalf("waiter error = %v", err)
		}
	}
}

func TestChannelContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("stop")
	cancel(want)
	_, err := NewChannel[int](1).Recv(ctx)
	if !errors.Is(err, want) {
		t.Fatalf("Recv() error = %v", err)
	}
}

func BenchmarkChannelRoundTrip(b *testing.B) {
	ctx := context.Background()
	ch := NewChannel[int](1)
	b.ReportAllocs()
	for b.Loop() {
		if !ch.TrySend(1) {
			b.Fatal("send")
		}
		if _, err := ch.Recv(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
