// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"errors"
	"math"
	"time"
)

type ExpFilterOptions struct {
	Alpha   float64
	Initial *float64
	Min     *float64
	Max     *float64
}

// ExpFilter performs exponentially weighted smoothing. It is intended to be
// owned by one actor/goroutine, avoiding a lock on load/audio hot paths.
type ExpFilter struct {
	alpha    float64
	min      float64
	max      float64
	hasMin   bool
	hasMax   bool
	filtered float64
	hasValue bool
}

func NewExpFilter(options ExpFilterOptions) (*ExpFilter, error) {
	if options.Alpha <= 0 || options.Alpha > 1 || math.IsNaN(options.Alpha) {
		return nil, errors.New("alpha must be in (0, 1]")
	}
	filter := &ExpFilter{alpha: options.Alpha}
	if options.Initial != nil {
		filter.filtered, filter.hasValue = *options.Initial, true
	}
	if options.Min != nil {
		filter.min, filter.hasMin = *options.Min, true
	}
	if options.Max != nil {
		filter.max, filter.hasMax = *options.Max, true
	}
	if filter.hasMin && filter.hasMax && filter.min > filter.max {
		return nil, errors.New("minimum filter value cannot exceed maximum")
	}
	if filter.hasValue {
		filter.filtered = filter.clamp(filter.filtered)
	}
	return filter, nil
}

func (f *ExpFilter) Apply(exponent, sample float64) float64 {
	if f.hasValue {
		alpha := math.Pow(f.alpha, exponent)
		f.filtered = alpha*f.filtered + (1-alpha)*sample
	} else {
		f.filtered, f.hasValue = sample, true
	}
	f.filtered = f.clamp(f.filtered)
	return f.filtered
}

func (f *ExpFilter) Value() (float64, bool) {
	if f == nil {
		return 0, false
	}
	return f.filtered, f.hasValue
}

func (f *ExpFilter) Reset(initial *float64) {
	if initial == nil {
		f.filtered, f.hasValue = 0, false
		return
	}
	f.filtered, f.hasValue = f.clamp(*initial), true
}

func (f *ExpFilter) SetAlpha(alpha float64) error {
	if alpha <= 0 || alpha > 1 || math.IsNaN(alpha) {
		return errors.New("alpha must be in (0, 1]")
	}
	f.alpha = alpha
	return nil
}

func (f *ExpFilter) clamp(value float64) float64 {
	if f.hasMax && value > f.max {
		value = f.max
	}
	if f.hasMin && value < f.min {
		value = f.min
	}
	return value
}

const DefaultAudioEnergyThreshold = 0.004

// AudioEnergyFilter holds speech-active state for Cooldown after the most
// recent frame whose normalized PCM RMS exceeds Threshold. PushFrame performs
// no allocations and is intended for one audio actor/goroutine.
type AudioEnergyFilter struct {
	Cooldown  time.Duration
	Threshold float64
	remaining time.Duration
}

func NewAudioEnergyFilter(cooldown time.Duration) *AudioEnergyFilter {
	if cooldown == 0 {
		cooldown = time.Second
	}
	return &AudioEnergyFilter{Cooldown: cooldown, Threshold: DefaultAudioEnergyThreshold, remaining: cooldown}
}

func (f *AudioEnergyFilter) Reset() {
	if f.Cooldown <= 0 {
		f.Cooldown = time.Second
	}
	f.remaining = f.Cooldown
}

func (f *AudioEnergyFilter) PushFrame(frame AudioFrame) bool {
	if f == nil || len(frame.Data) == 0 || frame.SampleRate <= 0 || frame.Channels <= 0 {
		return false
	}
	threshold := f.Threshold
	if threshold <= 0 {
		threshold = DefaultAudioEnergyThreshold
	}
	var energy float64
	for _, sample := range frame.Data {
		normalized := float64(sample) / 32768
		energy += normalized * normalized
	}
	rms := math.Sqrt(energy / float64(len(frame.Data)))
	if rms > threshold {
		if f.Cooldown <= 0 {
			f.Cooldown = time.Second
		}
		f.remaining = f.Cooldown
		return true
	}
	f.remaining -= frame.Duration()
	return f.remaining > 0
}
