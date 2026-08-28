// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"testing"
	"time"
)

func TestSummarizePreservesRawSamples(t *testing.T) {
	values := []time.Duration{40 * time.Millisecond, 10 * time.Millisecond, 30 * time.Millisecond, 20 * time.Millisecond}
	got := summarize(values, 7)
	if got.Samples != 4 || got.Warmups != 7 || got.MeanNS != int64(25*time.Millisecond) {
		t.Fatalf("unexpected counts or mean: %+v", got)
	}
	if got.MinNS != int64(10*time.Millisecond) || got.P50NS != int64(20*time.Millisecond) ||
		got.P95NS != int64(40*time.Millisecond) || got.P99NS != int64(40*time.Millisecond) ||
		got.MaxNS != int64(40*time.Millisecond) {
		t.Fatalf("unexpected distribution: %+v", got)
	}
	wantRaw := []int64{
		int64(40 * time.Millisecond), int64(10 * time.Millisecond),
		int64(30 * time.Millisecond), int64(20 * time.Millisecond),
	}
	if !reflect.DeepEqual(got.SamplesNS, wantRaw) {
		t.Fatalf("raw samples changed order: got %v, want %v", got.SamplesNS, wantRaw)
	}
	if !reflect.DeepEqual(values, []time.Duration{40 * time.Millisecond, 10 * time.Millisecond, 30 * time.Millisecond, 20 * time.Millisecond}) {
		t.Fatalf("summarize mutated its input: %v", values)
	}
}
