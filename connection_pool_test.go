// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionPoolReuseAndFailureRemoval(t *testing.T) {
	t.Parallel()
	var connects, closes atomic.Int64
	pool, err := NewConnectionPool(ConnectionPoolOptions[int]{
		Connect: func(context.Context) (int, error) { return int(connects.Add(1)), nil },
		Close:   func(context.Context, int) error { closes.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !pool.Put(first) {
		t.Fatal("Put failed")
	}
	second, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second != first || connects.Load() != 1 {
		t.Fatalf("second=%d first=%d connects=%d", second, first, connects.Load())
	}
	if err := pool.Remove(ctx, second); err != nil {
		t.Fatal(err)
	}
	if closes.Load() != 1 {
		t.Fatalf("closes=%d", closes.Load())
	}
	if err := pool.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionPoolConnectTimeout(t *testing.T) {
	t.Parallel()
	pool, _ := NewConnectionPool(ConnectionPoolOptions[int]{
		ConnectTimeout: 5 * time.Millisecond,
		Connect:        func(ctx context.Context) (int, error) { <-ctx.Done(); return 0, context.Cause(ctx) },
	})
	if _, err := pool.Get(context.Background()); err == nil {
		t.Fatal("Get succeeded")
	}
}
