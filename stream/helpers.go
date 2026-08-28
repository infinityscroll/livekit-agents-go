// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"io"
)

type SliceReader[T any] struct {
	values []T
	index  int
}

func FromSlice[T any](values []T) *SliceReader[T] {
	return &SliceReader[T]{values: values}
}

func (r *SliceReader[T]) Recv(ctx context.Context) (T, error) {
	if err := context.Cause(ctx); err != nil {
		var zero T
		return zero, err
	}
	if r.index == len(r.values) {
		var zero T
		return zero, io.EOF
	}
	value := r.values[r.index]
	r.index++
	return value, nil
}
