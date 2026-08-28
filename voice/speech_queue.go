// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"container/heap"
	"context"
	"errors"
	"sync"
)

var ErrSpeechQueueClosed = errors.New("speech queue closed")

type queuedSpeech struct {
	handle   *SpeechHandle
	priority int
	sequence uint64
	index    int
}

type speechHeap []*queuedSpeech

func (h speechHeap) Len() int { return len(h) }
func (h speechHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].sequence < h[j].sequence
}
func (h speechHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *speechHeap) Push(value any) {
	item := value.(*queuedSpeech)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *speechHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	old[len(old)-1] = nil
	item.index = -1
	*h = old[:len(old)-1]
	return item
}

// SpeechQueue is a stable, bounded max-priority queue. It exposes no channel,
// so callers cannot bypass queue ordering or close ownership.
type SpeechQueue struct {
	mu       sync.Mutex
	items    speechHeap
	next     uint64
	capacity int
	closed   bool
	changed  chan struct{}
}

func NewSpeechQueue(capacity int) *SpeechQueue {
	if capacity <= 0 {
		capacity = 64
	}
	return &SpeechQueue{capacity: capacity, changed: make(chan struct{})}
}

func (q *SpeechQueue) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func (q *SpeechQueue) Push(ctx context.Context, handle *SpeechHandle, priority int) error {
	if handle == nil {
		return errors.New("speech handle is required")
	}
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrSpeechQueueClosed
		}
		if len(q.items) < q.capacity {
			heap.Push(&q.items, &queuedSpeech{handle: handle, priority: priority, sequence: q.next})
			q.next++
			q.notifyLocked()
			q.mu.Unlock()
			return nil
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}

func (q *SpeechQueue) Pop(ctx context.Context) (*SpeechHandle, error) {
	for {
		q.mu.Lock()
		if len(q.items) != 0 {
			item := heap.Pop(&q.items).(*queuedSpeech)
			q.notifyLocked()
			q.mu.Unlock()
			return item.handle, nil
		}
		if q.closed {
			q.mu.Unlock()
			return nil, ErrSpeechQueueClosed
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-changed:
		}
	}
}

// Ordered returns a playout-order snapshot. Heap-array order is intentionally
// never exposed because it is not a valid interruption traversal order.
func (q *SpeechQueue) Ordered() []*SpeechHandle {
	q.mu.Lock()
	copy := make(speechHeap, len(q.items))
	for index, item := range q.items {
		clone := *item
		copy[index] = &clone
	}
	q.mu.Unlock()
	heap.Init(&copy)
	result := make([]*SpeechHandle, 0, len(copy))
	for len(copy) != 0 {
		result = append(result, heap.Pop(&copy).(*queuedSpeech).handle)
	}
	return result
}

func (q *SpeechQueue) Len() int { q.mu.Lock(); length := len(q.items); q.mu.Unlock(); return length }

func (q *SpeechQueue) Close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.notifyLocked()
	}
	q.mu.Unlock()
}
