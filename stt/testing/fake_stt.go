// SPDX-License-Identifier: Apache-2.0

// Package stttest contains deterministic STT test doubles. Nothing in this
// package performs network I/O or starts work before FakeSTT.Stream is called.
package stttest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/stt"
)

const (
	DefaultObservationCapacity = 128
	DefaultStreamCapacity      = 64
)

var ErrFakeSTTClosed = errors.New("fake STT is closed")

// FakeUserSpeech describes a transcript scheduled relative to the first audio
// frame pushed into a stream. Interim text is emitted at EndTime+STTDelay/2 and
// final text at EndTime+STTDelay. StartTime is used to validate non-overlap.
type FakeUserSpeech struct {
	StartTime  time.Duration
	EndTime    time.Duration
	Transcript string
	STTDelay   time.Duration
	Final      *bool
}

// SpeedUpFakeUserSpeech scales all scheduled durations. A factor above one
// makes a fixture faster. Invalid factors return the original value.
func SpeedUpFakeUserSpeech(speech FakeUserSpeech, factor float64) FakeUserSpeech {
	if factor <= 0 {
		return speech
	}
	speech.StartTime = time.Duration(float64(speech.StartTime) / factor)
	speech.EndTime = time.Duration(float64(speech.EndTime) / factor)
	speech.STTDelay = time.Duration(float64(speech.STTDelay) / factor)
	return speech
}

// RecognizeSentinel is emitted once for every batch Recognize call.
type RecognizeSentinel struct{}

type FakeSTTOptions struct {
	Label            string
	Provider         string
	Model            string
	FakeException    error
	FakeTranscript   *string
	FakeTimeout      time.Duration
	FakeUserSpeeches []FakeUserSpeech
	FakeRequireAudio bool
	Capabilities     *stt.Capabilities

	ObservationCapacity int
	StreamCapacity      int
}

// FakeSTTUpdateOptions uses Override so tests can distinguish “leave this
// knob unchanged”, “clear it”, and “set it (including to a zero value)”.
type FakeSTTUpdateOptions struct {
	FakeException  agents.Override[error]
	FakeTranscript agents.Override[string]
	FakeTimeout    agents.Override[time.Duration]
}

type fakeState struct {
	exception  error
	transcript *string
	timeout    time.Duration
	speeches   []FakeUserSpeech
	require    bool
}

// FakeSTT is a concurrency-safe scripted STT for fallback, recognition, and
// full agent-session tests. Observation streams are bounded and never block a
// model call; DroppedObservations reports saturation rather than hiding it.
type FakeSTT struct {
	*stt.Base

	mu          sync.RWMutex
	state       fakeState
	streams     map[*FakeRecognizeStream]struct{}
	streamEvent chan struct{}
	closed      bool
	capacity    int

	recognize *stream.Channel[RecognizeSentinel]
	created   *stream.Channel[*FakeRecognizeStream]
	dropped   atomic.Uint64

	speechesDone     chan struct{}
	speechesDoneOnce sync.Once
	closeOnce        sync.Once
	closeErr         error
}

func NewFakeSTT(options FakeSTTOptions) (*FakeSTT, error) {
	if options.FakeTimeout < 0 {
		return nil, errors.New("fake STT timeout must not be negative")
	}
	speeches := slices.Clone(options.FakeUserSpeeches)
	slices.SortStableFunc(speeches, func(left, right FakeUserSpeech) int {
		if left.StartTime < right.StartTime {
			return -1
		}
		if left.StartTime > right.StartTime {
			return 1
		}
		return 0
	})
	for index, speech := range speeches {
		if speech.StartTime < 0 || speech.EndTime < speech.StartTime || speech.STTDelay < 0 {
			return nil, fmt.Errorf("fake user speech %d has invalid timing", index)
		}
		if index != 0 && speeches[index-1].EndTime > speech.StartTime {
			return nil, errors.New("fake user speeches overlap")
		}
	}
	label := options.Label
	if label == "" {
		label = "fake-stt"
	}
	provider := options.Provider
	if provider == "" {
		provider = "test"
	}
	model := options.Model
	if model == "" {
		model = "fake"
	}
	capabilities := stt.Capabilities{Streaming: true}
	if options.Capabilities != nil {
		capabilities = *options.Capabilities
	}
	observationCapacity := options.ObservationCapacity
	if observationCapacity <= 0 {
		observationCapacity = DefaultObservationCapacity
	}
	streamCapacity := options.StreamCapacity
	if streamCapacity <= 0 {
		streamCapacity = DefaultStreamCapacity
	}
	value := &FakeSTT{
		Base: stt.NewBase(label, provider, model, capabilities), capacity: streamCapacity,
		state:        fakeState{exception: options.FakeException, transcript: cloneString(options.FakeTranscript), timeout: options.FakeTimeout, speeches: speeches, require: options.FakeRequireAudio},
		streams:      make(map[*FakeRecognizeStream]struct{}),
		streamEvent:  make(chan struct{}, 1),
		recognize:    stream.NewChannel[RecognizeSentinel](observationCapacity),
		created:      stream.NewChannel[*FakeRecognizeStream](observationCapacity),
		speechesDone: make(chan struct{}),
	}
	if len(speeches) == 0 {
		value.markSpeechesDone()
	}
	return value, nil
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (f *FakeSTT) UpdateOptions(options FakeSTTUpdateOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrFakeSTTClosed
	}
	if value, ok := options.FakeException.Value(); ok {
		f.state.exception = value
	} else if options.FakeException.IsDisabled() {
		f.state.exception = nil
	}
	if value, ok := options.FakeTranscript.Value(); ok {
		f.state.transcript = cloneString(&value)
	} else if options.FakeTranscript.IsDisabled() {
		f.state.transcript = nil
	}
	if value, ok := options.FakeTimeout.Value(); ok {
		if value < 0 {
			return errors.New("fake STT timeout must not be negative")
		}
		f.state.timeout = value
	} else if options.FakeTimeout.IsDisabled() {
		f.state.timeout = 0
	}
	return nil
}

func (f *FakeSTT) snapshot() fakeState {
	f.mu.RLock()
	state := f.state
	state.transcript = cloneString(state.transcript)
	state.speeches = slices.Clone(state.speeches)
	f.mu.RUnlock()
	return state
}

func (f *FakeSTT) RecognizeCalls() stream.Reader[RecognizeSentinel] { return f.recognize }
func (f *FakeSTT) Streams() stream.Reader[*FakeRecognizeStream]     { return f.created }
func (f *FakeSTT) FakeUserSpeechesDone() <-chan struct{}            { return f.speechesDone }
func (f *FakeSTT) DroppedObservations() uint64                      { return f.dropped.Load() }

func (f *FakeSTT) Recognize(ctx context.Context, _ []agents.AudioFrame, _ stt.RecognizeOptions) (stt.SpeechEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !f.recognize.TrySend(RecognizeSentinel{}) {
		f.dropped.Add(1)
	}
	f.mu.RLock()
	closed := f.closed
	f.mu.RUnlock()
	if closed {
		return stt.SpeechEvent{}, ErrFakeSTTClosed
	}
	state := f.snapshot()
	if err := wait(ctx, state.timeout); err != nil {
		return stt.SpeechEvent{}, err
	}
	if state.exception != nil {
		return stt.SpeechEvent{}, state.exception
	}
	text := ""
	if state.transcript != nil {
		text = *state.transcript
	}
	return transcriptEvent(text, true), nil
}

func wait(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return context.Cause(ctx)
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func (f *FakeSTT) Stream(ctx context.Context, _ stt.StreamOptions) (stt.SpeechStream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, ErrFakeSTTClosed
	}
	value := &FakeRecognizeStream{BaseStream: stt.NewBaseStream(ctx, f.capacity), fake: f, state: f.snapshotLocked()}
	f.streams[value] = struct{}{}
	f.mu.Unlock()
	if !f.created.TrySend(value) {
		f.dropped.Add(1)
	}
	go value.run()
	return value, nil
}

func (f *FakeSTT) snapshotLocked() fakeState {
	state := f.state
	state.transcript = cloneString(state.transcript)
	state.speeches = slices.Clone(state.speeches)
	return state
}

func (f *FakeSTT) unregister(value *FakeRecognizeStream) {
	f.mu.Lock()
	delete(f.streams, value)
	f.mu.Unlock()
	select {
	case f.streamEvent <- struct{}{}:
	default:
	}
}

func (f *FakeSTT) markSpeechesDone() { f.speechesDoneOnce.Do(func() { close(f.speechesDone) }) }

func (f *FakeSTT) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed = true
		streams := make([]*FakeRecognizeStream, 0, len(f.streams))
		for value := range f.streams {
			streams = append(streams, value)
		}
		f.mu.Unlock()
		for _, value := range streams {
			_ = value.Close()
		}
		for {
			f.mu.RLock()
			remaining := len(f.streams)
			f.mu.RUnlock()
			if remaining == 0 {
				break
			}
			select {
			case <-f.streamEvent:
			case <-ctx.Done():
				f.closeErr = context.Cause(ctx)
			}
			if f.closeErr != nil {
				break
			}
		}
		_ = f.recognize.Close()
		_ = f.created.Close()
		f.markSpeechesDone()
	})
	return f.closeErr
}

// FakeRecognizeStream is returned by FakeSTT.Stream. SendFakeTranscript can be
// used to inject additional output while the scripted stream is active.
type FakeRecognizeStream struct {
	*stt.BaseStream
	fake    *FakeSTT
	state   fakeState
	attempt atomic.Int64
}

func (s *FakeRecognizeStream) Attempt() int64 { return s.attempt.Load() }

func (s *FakeRecognizeStream) SendFakeTranscript(ctx context.Context, transcript string, final bool) error {
	return s.Emit(ctx, transcriptEvent(transcript, final))
}

func transcriptEvent(text string, final bool) stt.SpeechEvent {
	typeValue := stt.InterimTranscript
	if final {
		typeValue = stt.FinalTranscript
	}
	return stt.SpeechEvent{Type: typeValue, Alternatives: []stt.SpeechData{{Text: text, Confidence: 1}}}
}

func (s *FakeRecognizeStream) run() {
	s.attempt.Add(1)
	defer s.fake.unregister(s)
	if err := wait(s.Context(), s.state.timeout); err != nil {
		s.Finish(err)
		return
	}
	if s.state.require {
		s.runAudioRequired()
		return
	}
	if s.state.transcript != nil {
		if err := s.SendFakeTranscript(s.Context(), *s.state.transcript, true); err != nil {
			s.Finish(err)
			return
		}
	}
	if err := s.runScheduled(); err != nil {
		s.Finish(err)
		return
	}
	if s.state.exception != nil {
		s.Finish(s.state.exception)
		return
	}
	s.Finish(nil)
}

func (s *FakeRecognizeStream) runAudioRequired() {
	gotAudio := false
	for {
		input, err := s.Inputs().Recv(s.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.Finish(err)
			return
		}
		if input.Frame != nil {
			gotAudio = true
		}
		if input.Flush {
			if gotAudio && s.state.transcript != nil {
				if err := s.SendFakeTranscript(s.Context(), *s.state.transcript, true); err != nil {
					s.Finish(err)
					return
				}
			}
			gotAudio = false
		}
	}
	if s.state.exception != nil {
		s.Finish(s.state.exception)
		return
	}
	s.Finish(nil)
}

func (s *FakeRecognizeStream) runScheduled() error {
	if len(s.state.speeches) == 0 {
		return s.drainInput()
	}
	first, err := s.Inputs().Recv(s.Context())
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	_ = first
	drained := make(chan error, 1)
	go func() { drained <- s.drainInput() }()
	started := time.Now()
	for _, speech := range s.state.speeches {
		if speech.Transcript == "" {
			if err := waitUntil(s.Context(), started.Add(speech.EndTime+speech.STTDelay)); err != nil {
				return err
			}
			continue
		}
		if err := waitUntil(s.Context(), started.Add(speech.EndTime+speech.STTDelay/2)); err != nil {
			return err
		}
		if err := s.SendFakeTranscript(s.Context(), firstWords(speech.Transcript, 2), false); err != nil {
			return err
		}
		if err := waitUntil(s.Context(), started.Add(speech.EndTime+speech.STTDelay)); err != nil {
			return err
		}
		if speech.Final == nil || *speech.Final {
			if err := s.SendFakeTranscript(s.Context(), speech.Transcript, true); err != nil {
				return err
			}
		}
	}
	s.fake.markSpeechesDone()
	select {
	case err := <-drained:
		return err
	case <-s.Context().Done():
		return context.Cause(s.Context())
	}
}

func (s *FakeRecognizeStream) drainInput() error {
	for {
		_, err := s.Inputs().Recv(s.Context())
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	duration := time.Until(deadline)
	if duration <= 0 {
		return context.Cause(ctx)
	}
	return wait(ctx, duration)
}

func firstWords(text string, count int) string {
	fields := splitFields(text)
	if len(fields) > count {
		fields = fields[:count]
	}
	result := ""
	for index, field := range fields {
		if index != 0 {
			result += " "
		}
		result += field
	}
	return result
}

func splitFields(text string) []string {
	var fields []string
	start := -1
	for index, value := range text {
		if value == ' ' || value == '\t' || value == '\n' || value == '\r' {
			if start >= 0 {
				fields = append(fields, text[start:index])
				start = -1
			}
		} else if start < 0 {
			start = index
		}
	}
	if start >= 0 {
		fields = append(fields, text[start:])
	}
	return fields
}

// EmptyAudioFrame is a zero-sample PCM frame suitable for batch fake calls.
func EmptyAudioFrame() agents.AudioFrame {
	return agents.AudioFrame{SampleRate: 16_000, Channels: 1}
}

var _ stt.STT = (*FakeSTT)(nil)
var _ stt.SpeechStream = (*FakeRecognizeStream)(nil)
