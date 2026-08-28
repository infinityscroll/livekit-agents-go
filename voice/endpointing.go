// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"errors"
	"math"
	"sync"
	"time"
)

type EndpointingMode string

const (
	EndpointingFixed   EndpointingMode = "fixed"
	EndpointingDynamic EndpointingMode = "dynamic"
)

type EndpointingOptions struct {
	Mode     EndpointingMode
	MinDelay time.Duration
	MaxDelay time.Duration
	Alpha    float64
}

var (
	DefaultEndpointingOptions   = EndpointingOptions{Mode: EndpointingFixed, MinDelay: 500 * time.Millisecond, MaxDelay: 3 * time.Second, Alpha: 0.9}
	StreamingEndpointingOptions = EndpointingOptions{Mode: EndpointingFixed, MinDelay: 300 * time.Millisecond, MaxDelay: 2500 * time.Millisecond, Alpha: 0.9}
)

const agentSpeechLeadingSilenceGracePeriod = 250 * time.Millisecond

type EndpointingUpdate struct {
	MinDelay *time.Duration
	MaxDelay *time.Duration
	Alpha    *float64
}

type Endpointing interface {
	MinDelay() time.Duration
	MaxDelay() time.Duration
	Overlapping() bool
	OnStartOfSpeech(time.Time, bool)
	OnEndOfSpeech(time.Time, bool)
	OnStartOfAgentSpeech(time.Time)
	OnEndOfAgentSpeech(time.Time)
	Update(EndpointingUpdate) error
}

func validateEndpointing(options EndpointingOptions) error {
	if options.MinDelay < 0 || options.MaxDelay < 0 {
		return errors.New("endpointing delays must not be negative")
	}
	if math.IsNaN(options.Alpha) || options.Alpha < 0 || options.Alpha > 1 {
		return errors.New("endpointing alpha must be in [0, 1]")
	}
	return nil
}

func NewEndpointing(options EndpointingOptions) (Endpointing, error) {
	if options.Mode == "" {
		options.Mode = EndpointingFixed
	}
	if err := validateEndpointing(options); err != nil {
		return nil, err
	}
	if options.Mode == EndpointingDynamic {
		return NewDynamicEndpointing(options)
	}
	if options.Mode != EndpointingFixed {
		return nil, errors.New("unknown endpointing mode")
	}
	return &FixedEndpointing{minDelay: options.MinDelay, maxDelay: options.MaxDelay}, nil
}

type FixedEndpointing struct {
	mu          sync.RWMutex
	minDelay    time.Duration
	maxDelay    time.Duration
	overlapping bool
}

func (e *FixedEndpointing) MinDelay() time.Duration {
	e.mu.RLock()
	value := e.minDelay
	e.mu.RUnlock()
	return value
}
func (e *FixedEndpointing) MaxDelay() time.Duration {
	e.mu.RLock()
	value := e.maxDelay
	e.mu.RUnlock()
	return value
}
func (e *FixedEndpointing) Overlapping() bool {
	e.mu.RLock()
	value := e.overlapping
	e.mu.RUnlock()
	return value
}
func (e *FixedEndpointing) OnStartOfSpeech(_ time.Time, overlapping bool) {
	e.mu.Lock()
	e.overlapping = overlapping
	e.mu.Unlock()
}
func (e *FixedEndpointing) OnEndOfSpeech(_ time.Time, _ bool) {
	e.mu.Lock()
	e.overlapping = false
	e.mu.Unlock()
}
func (*FixedEndpointing) OnStartOfAgentSpeech(time.Time) {}
func (*FixedEndpointing) OnEndOfAgentSpeech(time.Time)   {}
func (e *FixedEndpointing) Update(update EndpointingUpdate) error {
	if update.MinDelay != nil && *update.MinDelay < 0 {
		return errors.New("endpointing min delay must not be negative")
	}
	if update.MaxDelay != nil && *update.MaxDelay < 0 {
		return errors.New("endpointing max delay must not be negative")
	}
	if update.Alpha != nil && (math.IsNaN(*update.Alpha) || *update.Alpha < 0 || *update.Alpha > 1) {
		return errors.New("endpointing alpha must be in [0, 1]")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if update.MinDelay != nil {
		e.minDelay = *update.MinDelay
	}
	if update.MaxDelay != nil {
		e.maxDelay = *update.MaxDelay
	}
	return nil
}

type DynamicEndpointing struct {
	mu sync.RWMutex

	minDelay, maxDelay time.Duration
	alpha              float64
	filtered           time.Duration
	overlapping        bool
	utteranceStartedAt *time.Time
	utteranceEndedAt   *time.Time
	agentStartedAt     *time.Time
	agentEndedAt       *time.Time
	speaking           bool
}

func NewDynamicEndpointing(options EndpointingOptions) (*DynamicEndpointing, error) {
	if options.Alpha == 0 {
		// Zero is a valid explicit coefficient. Callers wanting the cross-SDK
		// default should pass DefaultEndpointingOptions.Alpha.
	}
	if err := validateEndpointing(options); err != nil {
		return nil, err
	}
	return &DynamicEndpointing{minDelay: options.MinDelay, maxDelay: options.MaxDelay, alpha: options.Alpha, filtered: options.MinDelay}, nil
}

func (e *DynamicEndpointing) MinDelay() time.Duration {
	e.mu.RLock()
	value := min(e.filtered, e.maxDelay)
	e.mu.RUnlock()
	return value
}
func (e *DynamicEndpointing) MaxDelay() time.Duration {
	e.mu.RLock()
	value := e.maxDelay
	e.mu.RUnlock()
	return value
}
func (e *DynamicEndpointing) Overlapping() bool {
	e.mu.RLock()
	value := e.overlapping
	e.mu.RUnlock()
	return value
}

func (e *DynamicEndpointing) BetweenUtteranceDelay() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.betweenUtteranceDelayLocked()
}
func (e *DynamicEndpointing) betweenUtteranceDelayLocked() time.Duration {
	if e.utteranceStartedAt == nil || e.utteranceEndedAt == nil {
		return 0
	}
	return max(0, e.utteranceStartedAt.Sub(*e.utteranceEndedAt))
}
func (e *DynamicEndpointing) betweenTurnDelayLocked() time.Duration {
	if e.agentStartedAt == nil || e.utteranceEndedAt == nil {
		return 0
	}
	return max(0, e.agentStartedAt.Sub(*e.utteranceEndedAt))
}
func (e *DynamicEndpointing) immediateInterruptionDelayLocked() (time.Duration, time.Duration) {
	if e.utteranceStartedAt == nil || e.agentStartedAt == nil {
		return 0, 0
	}
	turn := e.betweenTurnDelayLocked()
	return turn, absDuration(e.betweenUtteranceDelayLocked() - turn)
}

func (e *DynamicEndpointing) OnStartOfAgentSpeech(started time.Time) {
	e.mu.Lock()
	e.agentStartedAt = timePointer(started)
	e.agentEndedAt = nil
	e.overlapping = e.speaking
	e.mu.Unlock()
}

func (e *DynamicEndpointing) OnEndOfAgentSpeech(ended time.Time) {
	e.mu.Lock()
	if e.agentStartedAt != nil && (e.agentEndedAt == nil || e.agentEndedAt.Before(*e.agentStartedAt)) {
		e.agentEndedAt = timePointer(ended)
	}
	e.overlapping = false
	e.mu.Unlock()
}

func (e *DynamicEndpointing) OnStartOfSpeech(started time.Time, overlapping bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.overlapping {
		return
	}
	if e.utteranceStartedAt != nil && e.utteranceEndedAt != nil && e.agentStartedAt != nil &&
		e.utteranceEndedAt.Before(*e.utteranceStartedAt) && overlapping {
		adjusted := e.agentStartedAt.Add(-time.Millisecond)
		e.utteranceEndedAt = &adjusted
	}
	e.utteranceStartedAt = timePointer(started)
	e.overlapping = overlapping
	e.speaking = true
}

func (e *DynamicEndpointing) OnEndOfSpeech(ended time.Time, shouldIgnore bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if shouldIgnore && e.overlapping {
		if e.utteranceStartedAt == nil || e.agentStartedAt == nil || absDuration(e.utteranceStartedAt.Sub(*e.agentStartedAt)) >= agentSpeechLeadingSilenceGracePeriod {
			e.overlapping = false
			e.speaking = false
			e.utteranceStartedAt = nil
			e.utteranceEndedAt = nil
			return
		}
	}
	agentStillSpeaking := e.agentStartedAt != nil && e.agentEndedAt == nil
	betweenUtterances := e.betweenUtteranceDelayLocked()
	if e.overlapping || agentStillSpeaking {
		turnDelay, interruptionDelay := e.immediateInterruptionDelayLocked()
		learnedMin := min(e.filtered, e.maxDelay)
		if interruptionDelay > 0 && interruptionDelay <= learnedMin && turnDelay > 0 && turnDelay <= e.maxDelay && betweenUtterances > 0 {
			e.applyLocked(betweenUtterances)
		}
	} else if betweenUtterances > 0 && e.agentEndedAt == nil && e.agentStartedAt == nil {
		e.applyLocked(betweenUtterances)
	}
	e.utteranceEndedAt = timePointer(ended)
	e.agentStartedAt = nil
	e.agentEndedAt = nil
	e.speaking = false
	e.overlapping = false
}

func (e *DynamicEndpointing) applyLocked(sample time.Duration) {
	filtered := e.alpha*float64(e.filtered) + (1-e.alpha)*float64(sample)
	e.filtered = time.Duration(filtered)
	if e.filtered > e.maxDelay {
		e.filtered = e.maxDelay
	}
	if e.filtered < e.minDelay {
		e.filtered = e.minDelay
	}
}

func (e *DynamicEndpointing) Update(update EndpointingUpdate) error {
	if update.MinDelay != nil && *update.MinDelay < 0 {
		return errors.New("endpointing min delay must not be negative")
	}
	if update.MaxDelay != nil && *update.MaxDelay < 0 {
		return errors.New("endpointing max delay must not be negative")
	}
	if update.Alpha != nil && (math.IsNaN(*update.Alpha) || *update.Alpha < 0 || *update.Alpha > 1) {
		return errors.New("endpointing alpha must be in [0, 1]")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if update.MinDelay != nil {
		e.minDelay, e.filtered = *update.MinDelay, *update.MinDelay
	}
	if update.MaxDelay != nil {
		e.maxDelay = *update.MaxDelay
		if e.filtered > e.maxDelay {
			e.filtered = e.maxDelay
		}
	}
	if update.Alpha != nil {
		e.alpha = *update.Alpha
	}
	return nil
}

func timePointer(value time.Time) *time.Time { copy := value; return &copy }
func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
