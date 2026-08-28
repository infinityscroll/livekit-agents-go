// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"io"
	"testing"
)

func TestRealtimeQueueDropsOldest(t *testing.T) {
	t.Parallel()
	queue := newRealtimeQueue[int](2)
	if !queue.Push(1) || !queue.Push(2) {
		t.Fatal("unexpected drop before queue was full")
	}
	if queue.Push(3) {
		t.Fatal("full push should report a dropped item")
	}
	for _, want := range []int{2, 3} {
		got, err := queue.Recv(context.Background())
		if err != nil || got != want {
			t.Fatalf("Recv() = %d, %v; want %d", got, err, want)
		}
	}
	if queue.Dropped() != 1 {
		t.Fatalf("Dropped() = %d", queue.Dropped())
	}
	queue.Close(nil)
	if _, err := queue.Recv(context.Background()); err != io.EOF {
		t.Fatalf("close error = %v", err)
	}
}
