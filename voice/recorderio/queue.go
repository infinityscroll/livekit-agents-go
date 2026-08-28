// SPDX-License-Identifier: Apache-2.0

package recorderio

import (
	"context"
	"io"
	"sync"
)

type batchQueue struct {
	mu        sync.Mutex
	items     []recordBatch
	head      int
	length    int
	bytes     int
	frames    int
	closed    bool
	closeErr  error
	readable  chan struct{}
	writable  chan struct{}
	done      chan struct{}
	maxBytes  int
	maxFrames int
}

func newBatchQueue(capacity, maxBytes, maxFrames int) *batchQueue {
	return &batchQueue{
		items: make([]recordBatch, capacity), readable: make(chan struct{}, 1),
		writable: make(chan struct{}, 1), done: make(chan struct{}), maxBytes: maxBytes, maxFrames: maxFrames,
	}
}

func (q *batchQueue) Send(ctx context.Context, batch recordBatch) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if batch.bytes < 0 || batch.bytes > q.maxBytes || batch.frames < 0 || batch.frames > q.maxFrames {
		return ErrBufferLimit
	}
	for {
		q.mu.Lock()
		if q.closed {
			err := q.terminalLocked()
			q.mu.Unlock()
			return err
		}
		if q.length < len(q.items) && q.bytes <= q.maxBytes-batch.bytes && q.frames <= q.maxFrames-batch.frames {
			index := (q.head + q.length) % len(q.items)
			q.items[index] = batch
			q.length++
			q.bytes += batch.bytes
			q.frames += batch.frames
			signal(q.readable)
			q.mu.Unlock()
			return nil
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-q.done:
		case <-q.writable:
		}
	}
}

func (q *batchQueue) Recv(ctx context.Context) (recordBatch, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		q.mu.Lock()
		if q.length != 0 {
			batch := q.items[q.head]
			q.items[q.head] = recordBatch{}
			q.head = (q.head + 1) % len(q.items)
			q.length--
			q.bytes -= batch.bytes
			q.frames -= batch.frames
			signal(q.writable)
			q.mu.Unlock()
			return batch, nil
		}
		if q.closed {
			err := q.terminalLocked()
			q.mu.Unlock()
			return recordBatch{}, err
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return recordBatch{}, context.Cause(ctx)
		case <-q.done:
		case <-q.readable:
		}
	}
}

func (q *batchQueue) Close() error { return q.closeWith(nil) }
func (q *batchQueue) Abort(err error) error {
	if err == nil {
		err = ErrClosed
	}
	return q.closeWith(err)
}
func (q *batchQueue) closeWith(err error) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed, q.closeErr = true, err
	if err != nil {
		clear(q.items)
		q.head, q.length, q.bytes, q.frames = 0, 0, 0, 0
	}
	close(q.done)
	signal(q.readable)
	signal(q.writable)
	q.mu.Unlock()
	return nil
}
func (q *batchQueue) terminalLocked() error {
	if q.closeErr != nil {
		return q.closeErr
	}
	return io.EOF
}

func signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}
