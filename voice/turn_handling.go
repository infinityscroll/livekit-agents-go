// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"errors"
	"fmt"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/inference"
)

type TurnDetectionMode string

const (
	TurnDetectionSTT         TurnDetectionMode = "stt"
	TurnDetectionVAD         TurnDetectionMode = "vad"
	TurnDetectionRealtimeLLM TurnDetectionMode = "realtime_llm"
	TurnDetectionManual      TurnDetectionMode = "manual"
)

// TurnDetection selects either one of the built-in turn-boundary strategies or
// a concrete streaming audio end-of-turn detector. The interface is sealed so
// invalid implementations cannot enter a running session; use
// UseTurnDetector for inference.TurnDetector instances.
type TurnDetection interface {
	turnDetectionSelection()
}

func (TurnDetectionMode) turnDetectionSelection() {}

type streamingTurnDetection struct {
	detector *inference.TurnDetector
}

func (streamingTurnDetection) turnDetectionSelection() {}

// UseTurnDetectionMode creates a concrete mode override. The zero value of
// TurnDetectionUpdate inherits the session setting and agents.Disable opts out
// explicitly.
func UseTurnDetectionMode(mode TurnDetectionMode) TurnDetectionUpdate {
	return agents.Use[TurnDetection](mode)
}

// UseTurnDetector selects a caller-owned streaming audio turn detector.
func UseTurnDetector(detector *inference.TurnDetector) TurnDetectionUpdate {
	return agents.Use[TurnDetection](streamingTurnDetection{detector: detector})
}

func turnDetectionSelection(value TurnDetection) (TurnDetectionMode, *inference.TurnDetector, error) {
	switch selected := value.(type) {
	case TurnDetectionMode:
		switch selected {
		case TurnDetectionSTT, TurnDetectionVAD, TurnDetectionRealtimeLLM, TurnDetectionManual:
			return selected, nil, nil
		default:
			return "", nil, fmt.Errorf("unknown turn detection mode %q", selected)
		}
	case streamingTurnDetection:
		if selected.detector == nil {
			return "", nil, errors.New("streaming turn detector must not be nil")
		}
		return TurnDetectionVAD, selected.detector, nil
	case nil:
		return "", nil, errors.New("turn detection selection must not be nil")
	default:
		return "", nil, fmt.Errorf("unsupported turn detection selection %T", value)
	}
}

type InterruptionMode string

const (
	InterruptionAdaptive InterruptionMode = "adaptive"
	InterruptionVAD      InterruptionMode = "vad"
)

type BackchannelBoundary struct {
	Start time.Duration
	End   time.Duration
}

type InterruptionOptions struct {
	Enabled bool
	Mode    InterruptionMode
	// Detector optionally supplies a caller-owned adaptive detector. When nil,
	// adaptive mode lazily constructs the production inference detector.
	Detector                      *inference.AdaptiveInterruptionDetector
	DiscardAudioIfUninterruptible bool
	MinDuration                   time.Duration
	MinWords                      int
	FalseInterruptionTimeout      *time.Duration
	ResumeFalseInterruption       bool
	BackchannelBoundary           *BackchannelBoundary
}

func DefaultInterruptionOptions() InterruptionOptions {
	timeout := 2 * time.Second
	return InterruptionOptions{
		Enabled: true, DiscardAudioIfUninterruptible: true, MinDuration: 500 * time.Millisecond,
		MinWords: 0, FalseInterruptionTimeout: &timeout, ResumeFalseInterruption: true,
		BackchannelBoundary: &BackchannelBoundary{Start: time.Second, End: time.Second},
	}
}

func (o InterruptionOptions) Validate() error {
	if o.Mode != "" && o.Mode != InterruptionAdaptive && o.Mode != InterruptionVAD {
		return errors.New("unknown interruption mode")
	}
	if o.MinDuration < 0 || o.MinWords < 0 {
		return errors.New("interruption minimums must not be negative")
	}
	if o.FalseInterruptionTimeout != nil && *o.FalseInterruptionTimeout < 0 {
		return errors.New("false interruption timeout must not be negative")
	}
	if o.BackchannelBoundary != nil && (o.BackchannelBoundary.Start < 0 || o.BackchannelBoundary.End < 0) {
		return errors.New("backchannel boundary must not be negative")
	}
	if o.Detector != nil && o.Mode == InterruptionVAD {
		return errors.New("adaptive interruption detector cannot be used with VAD-only interruption mode")
	}
	return nil
}

type PreemptiveGenerationOptions struct {
	Enabled           bool
	PreemptiveTTS     bool
	MaxSpeechDuration time.Duration
	MaxRetries        int
}

var DefaultPreemptiveGenerationOptions = PreemptiveGenerationOptions{
	Enabled: true, PreemptiveTTS: false, MaxSpeechDuration: 10 * time.Second, MaxRetries: 3,
}

func (o PreemptiveGenerationOptions) Validate() error {
	if o.MaxSpeechDuration < 0 || o.MaxRetries < 0 {
		return errors.New("preemptive generation limits must not be negative")
	}
	return nil
}

type UserTurnLimitOptions struct {
	MaxWords    *int
	MaxDuration *time.Duration
}

func (o UserTurnLimitOptions) Validate() error {
	if o.MaxWords != nil && *o.MaxWords < 0 {
		return errors.New("user turn max words must not be negative")
	}
	if o.MaxDuration != nil && *o.MaxDuration < 0 {
		return errors.New("user turn max duration must not be negative")
	}
	return nil
}

// TurnDetectionUpdate preserves omitted, explicit disable, a concrete mode,
// and a concrete streaming detector.
type TurnDetectionUpdate = agents.Override[TurnDetection]

type TurnHandlingOptions struct {
	TurnDetection        TurnDetectionUpdate
	Endpointing          EndpointingOptions
	Interruption         InterruptionOptions
	PreemptiveGeneration PreemptiveGenerationOptions
	UserTurnLimit        UserTurnLimitOptions
}

func DefaultTurnHandlingOptions(streamingDetector bool) TurnHandlingOptions {
	endpointing := DefaultEndpointingOptions
	if streamingDetector {
		endpointing = StreamingEndpointingOptions
	}
	return TurnHandlingOptions{
		Endpointing: endpointing, Interruption: DefaultInterruptionOptions(),
		PreemptiveGeneration: DefaultPreemptiveGenerationOptions,
	}
}

func (o TurnHandlingOptions) Validate() error {
	if selected, set := o.TurnDetection.Value(); set {
		if _, _, err := turnDetectionSelection(selected); err != nil {
			return err
		}
	}
	if err := validateEndpointing(o.Endpointing); err != nil {
		return err
	}
	if err := o.Interruption.Validate(); err != nil {
		return err
	}
	if err := o.PreemptiveGeneration.Validate(); err != nil {
		return err
	}
	return o.UserTurnLimit.Validate()
}

type UserTurnAccumulator struct {
	startedAt  time.Time
	transcript string
	words      int
}

func (a *UserTurnAccumulator) Add(now time.Time, transcript string, wordCount int) {
	if a.startedAt.IsZero() {
		a.startedAt = now
	}
	if a.transcript != "" && transcript != "" {
		a.transcript += " "
	}
	a.transcript += transcript
	a.words += wordCount
}
func (a *UserTurnAccumulator) Reset() { *a = UserTurnAccumulator{} }
func (a *UserTurnAccumulator) Exceeded(now time.Time, options UserTurnLimitOptions) bool {
	return (options.MaxWords != nil && a.words >= *options.MaxWords) ||
		(options.MaxDuration != nil && !a.startedAt.IsZero() && now.Sub(a.startedAt) >= *options.MaxDuration)
}
func (a *UserTurnAccumulator) Snapshot(now time.Time) (string, int, time.Duration) {
	if a.startedAt.IsZero() {
		return a.transcript, a.words, 0
	}
	return a.transcript, a.words, max(0, now.Sub(a.startedAt))
}
