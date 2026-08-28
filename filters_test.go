// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"math"
	"testing"
	"time"
)

func TestExpFilter(t *testing.T) {
	t.Parallel()
	minimum, maximum := 0.0, 10.0
	filter, err := NewExpFilter(ExpFilterOptions{Alpha: 0.5, Min: &minimum, Max: &maximum})
	if err != nil {
		t.Fatal(err)
	}
	if got := filter.Apply(1, 8); got != 8 {
		t.Fatalf("first = %v", got)
	}
	if got := filter.Apply(1, 4); got != 6 {
		t.Fatalf("second = %v", got)
	}
	if got := filter.Apply(2, 20); got != 10 {
		t.Fatalf("clamped = %v", got)
	}
	filter.Reset(nil)
	if _, ok := filter.Value(); ok {
		t.Fatal("Reset(nil) retained a value")
	}
	if _, err := NewExpFilter(ExpFilterOptions{Alpha: math.NaN()}); err == nil {
		t.Fatal("NaN alpha accepted")
	}
}

func TestAudioEnergyFilterCooldown(t *testing.T) {
	t.Parallel()
	filter := NewAudioEnergyFilter(40 * time.Millisecond)
	silent := AudioFrame{Data: make([]int16, 480), SampleRate: 48_000, Channels: 1, SamplesPerChannel: 480}
	loud := silent
	loud.Data = make([]int16, 480)
	for i := range loud.Data {
		loud.Data[i] = 1_000
	}
	if !filter.PushFrame(loud) {
		t.Fatal("loud frame was rejected")
	}
	for range 3 {
		if !filter.PushFrame(silent) {
			t.Fatal("filter ended before cooldown")
		}
	}
	if filter.PushFrame(silent) {
		t.Fatal("filter remained active after cooldown")
	}
}

func BenchmarkAudioEnergyFilter(b *testing.B) {
	filter := NewAudioEnergyFilter(time.Second)
	frame := AudioFrame{Data: make([]int16, 480), SampleRate: 48_000, Channels: 1, SamplesPerChannel: 480}
	b.ReportAllocs()
	for b.Loop() {
		filter.PushFrame(frame)
	}
}
