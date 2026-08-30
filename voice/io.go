// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stream"
)

type Attachable interface {
	SetAttached(bool)
	OnAttached()
	OnDetached()
}

type AudioInput interface {
	stream.Reader[agents.AudioFrame]
	Attachable
	Close() error
}

// BaseAudioInput combines dynamically attached bounded frame sources.
type BaseAudioInput struct {
	inputs   *stream.MultiInput[agents.AudioFrame]
	attached atomic.Bool
}

func NewBaseAudioInput(parent context.Context, capacity int, onError func(string, error)) *BaseAudioInput {
	return &BaseAudioInput{inputs: stream.NewMultiInput[agents.AudioFrame](parent, capacity, onError)}
}
func (i *BaseAudioInput) Recv(ctx context.Context) (agents.AudioFrame, error) {
	return i.inputs.Recv(ctx)
}
func (i *BaseAudioInput) Add(source stream.Reader[agents.AudioFrame]) (string, error) {
	return i.inputs.Add(source)
}
func (i *BaseAudioInput) Remove(id string)          { i.inputs.Remove(id) }
func (i *BaseAudioInput) SetAttached(attached bool) { i.attached.Store(attached) }
func (i *BaseAudioInput) Attached() bool            { return i.attached.Load() }
func (*BaseAudioInput) OnAttached()                 {}
func (*BaseAudioInput) OnDetached()                 {}
func (i *BaseAudioInput) Close() error              { return i.inputs.Close() }

type AudioOutputCapabilities struct {
	Pause bool
}

type PlaybackStartedEvent struct {
	CreatedAt time.Time `json:"created_at"`
}

type PlaybackFinishedEvent struct {
	PlaybackPosition       time.Duration `json:"playback_position"`
	Interrupted            bool          `json:"interrupted"`
	SynchronizedTranscript *string       `json:"synchronized_transcript,omitempty"`
}

type AudioOutput interface {
	CaptureFrame(context.Context, agents.AudioFrame) error
	Flush(context.Context) error
	ClearBuffer(context.Context) error
	WaitForPlayout(context.Context) (PlaybackFinishedEvent, error)
	Pause(context.Context) error
	Resume(context.Context) error
	CanPause() bool
	SampleRate() int
	Attachable
	OnPlaybackStarted(func(PlaybackStartedEvent)) func()
	OnPlaybackFinished(func(PlaybackFinishedEvent)) func()
	PendingPlayoutSegments() uint64
	CapturedPlayoutSegments() uint64
}

var ErrUnexpectedPlaybackFinished = errors.New("playback finished without a pending segment")

type audioOutputState struct {
	mu          sync.Mutex
	open        bool
	captured    uint64
	finished    uint64
	last        PlaybackFinishedEvent
	changed     chan struct{}
	started     agents.EventEmitter[PlaybackStartedEvent]
	completed   agents.EventEmitter[PlaybackFinishedEvent]
	next        AudioOutput
	unsubscribe []func()
}

func newAudioOutputState(next AudioOutput) *audioOutputState {
	state := &audioOutputState{changed: make(chan struct{}), next: next}
	if next != nil {
		state.unsubscribe = append(state.unsubscribe,
			next.OnPlaybackStarted(func(event PlaybackStartedEvent) { state.notifyStarted(event) }),
			next.OnPlaybackFinished(func(event PlaybackFinishedEvent) { _ = state.notifyFinished(event) }),
		)
	}
	return state
}

func (s *audioOutputState) beginFrame() bool {
	s.mu.Lock()
	opened := false
	if !s.open {
		s.open = true
		s.captured++
		opened = true
		s.signalLocked()
	}
	s.mu.Unlock()
	return opened
}
func (s *audioOutputState) rollbackFrame(opened bool) {
	s.mu.Lock()
	s.open = false
	if opened && s.captured > s.finished {
		s.captured--
		s.signalLocked()
	}
	s.mu.Unlock()
}
func (s *audioOutputState) flush()                                   { s.mu.Lock(); s.open = false; s.mu.Unlock() }
func (s *audioOutputState) abandon()                                 { s.mu.Lock(); s.open = false; s.mu.Unlock() }
func (s *audioOutputState) signalLocked()                            { close(s.changed); s.changed = make(chan struct{}) }
func (s *audioOutputState) notifyStarted(event PlaybackStartedEvent) { s.started.Emit(event) }
func (s *audioOutputState) notifyFinished(event PlaybackFinishedEvent) error {
	s.mu.Lock()
	if s.finished >= s.captured {
		s.mu.Unlock()
		return ErrUnexpectedPlaybackFinished
	}
	s.last = event
	s.finished++
	s.signalLocked()
	s.mu.Unlock()
	s.completed.Emit(event)
	return nil
}
func (s *audioOutputState) wait(ctx context.Context) (PlaybackFinishedEvent, error) {
	s.mu.Lock()
	target := s.captured
	for s.finished < target {
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return PlaybackFinishedEvent{}, context.Cause(ctx)
		case <-changed:
		}
		s.mu.Lock()
	}
	last := s.last
	s.mu.Unlock()
	return last, nil
}
func (s *audioOutputState) counts() (uint64, uint64) {
	s.mu.Lock()
	captured, finished := s.captured, s.finished
	s.mu.Unlock()
	return captured, finished
}

type AudioOutputOptions struct {
	SampleRate   int
	Capabilities AudioOutputCapabilities
	Next         AudioOutput
	Capture      func(context.Context, agents.AudioFrame) error
	Flush        func(context.Context) error
	ClearBuffer  func(context.Context) error
	Pause        func(context.Context) error
	Resume       func(context.Context) error
	Attached     func()
	Detached     func()
}

// ManagedAudioOutput is the production helper for custom sinks. It owns
// segment accounting and forwards through an optional output chain.
type ManagedAudioOutput struct {
	options  AudioOutputOptions
	state    *audioOutputState
	attached atomic.Bool
}

func NewManagedAudioOutput(options AudioOutputOptions) (*ManagedAudioOutput, error) {
	if options.SampleRate < 0 {
		return nil, errors.New("audio output sample rate must not be negative")
	}
	return &ManagedAudioOutput{options: options, state: newAudioOutputState(options.Next)}, nil
}
func (o *ManagedAudioOutput) SampleRate() int { return o.options.SampleRate }
func (o *ManagedAudioOutput) CanPause() bool {
	return o.options.Capabilities.Pause && (o.options.Next == nil || o.options.Next.CanPause())
}
func (o *ManagedAudioOutput) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	opened := o.state.beginFrame()
	if o.options.Capture != nil {
		if err := o.options.Capture(ctx, frame); err != nil {
			o.state.rollbackFrame(opened)
			return err
		}
	}
	if o.options.Next != nil {
		if err := o.options.Next.CaptureFrame(ctx, frame); err != nil {
			o.state.rollbackFrame(opened)
			return err
		}
	}
	return nil
}
func (o *ManagedAudioOutput) Flush(ctx context.Context) error {
	o.state.flush()
	if o.options.Flush != nil {
		if err := o.options.Flush(ctx); err != nil {
			return err
		}
	}
	if o.options.Next != nil {
		return o.options.Next.Flush(ctx)
	}
	return nil
}
func (o *ManagedAudioOutput) ClearBuffer(ctx context.Context) error {
	o.state.abandon()
	if o.options.ClearBuffer == nil && o.options.Next == nil {
		return errors.New("audio output does not implement ClearBuffer")
	}
	if o.options.ClearBuffer != nil {
		if err := o.options.ClearBuffer(ctx); err != nil {
			return err
		}
	}
	if o.options.Next != nil {
		return o.options.Next.ClearBuffer(ctx)
	}
	return nil
}
func (o *ManagedAudioOutput) Pause(ctx context.Context) error {
	if !o.CanPause() {
		return errors.New("audio output chain does not support pause")
	}
	if o.options.Pause != nil {
		if err := o.options.Pause(ctx); err != nil {
			return err
		}
	}
	if o.options.Next != nil {
		return o.options.Next.Pause(ctx)
	}
	return nil
}
func (o *ManagedAudioOutput) Resume(ctx context.Context) error {
	if !o.CanPause() {
		return errors.New("audio output chain does not support resume")
	}
	if o.options.Resume != nil {
		if err := o.options.Resume(ctx); err != nil {
			return err
		}
	}
	if o.options.Next != nil {
		return o.options.Next.Resume(ctx)
	}
	return nil
}
func (o *ManagedAudioOutput) WaitForPlayout(ctx context.Context) (PlaybackFinishedEvent, error) {
	return o.state.wait(ctx)
}
func (o *ManagedAudioOutput) NotifyPlaybackStarted(createdAt time.Time) {
	o.state.notifyStarted(PlaybackStartedEvent{CreatedAt: createdAt})
}
func (o *ManagedAudioOutput) NotifyPlaybackFinished(event PlaybackFinishedEvent) error {
	return o.state.notifyFinished(event)
}
func (o *ManagedAudioOutput) OnPlaybackStarted(fn func(PlaybackStartedEvent)) func() {
	return o.state.started.Subscribe(fn)
}
func (o *ManagedAudioOutput) OnPlaybackFinished(fn func(PlaybackFinishedEvent)) func() {
	return o.state.completed.Subscribe(fn)
}
func (o *ManagedAudioOutput) PendingPlayoutSegments() uint64 {
	captured, finished := o.state.counts()
	return captured - finished
}
func (o *ManagedAudioOutput) CapturedPlayoutSegments() uint64 {
	captured, _ := o.state.counts()
	return captured
}
func (o *ManagedAudioOutput) SetAttached(attached bool) { o.attached.Store(attached) }
func (o *ManagedAudioOutput) OnAttached() {
	o.attached.Store(true)
	if o.options.Attached != nil {
		o.options.Attached()
	}
	if o.options.Next != nil {
		o.options.Next.SetAttached(true)
		o.options.Next.OnAttached()
	}
}
func (o *ManagedAudioOutput) OnDetached() {
	o.attached.Store(false)
	if o.options.Detached != nil {
		o.options.Detached()
	}
	if o.options.Next != nil {
		o.options.Next.SetAttached(false)
		o.options.Next.OnDetached()
	}
}
func (o *ManagedAudioOutput) Close() {
	for _, unsubscribe := range o.state.unsubscribe {
		unsubscribe()
	}
	o.state.unsubscribe = nil
}

type TextOutput interface {
	CaptureText(context.Context, agents.TimedString) error
	Flush(context.Context) error
	Attachable
}

type TextOutputOptions struct {
	Next     TextOutput
	Capture  func(context.Context, agents.TimedString) error
	Flush    func(context.Context) error
	Attached func()
	Detached func()
}

type ManagedTextOutput struct {
	options  TextOutputOptions
	attached atomic.Bool
}

func NewManagedTextOutput(options TextOutputOptions) *ManagedTextOutput {
	return &ManagedTextOutput{options: options}
}
func (o *ManagedTextOutput) CaptureText(ctx context.Context, text agents.TimedString) error {
	if o.options.Capture != nil {
		if err := o.options.Capture(ctx, text); err != nil {
			return err
		}
	}
	if o.options.Next != nil {
		return o.options.Next.CaptureText(ctx, text)
	}
	return nil
}
func (o *ManagedTextOutput) Flush(ctx context.Context) error {
	if o.options.Flush != nil {
		if err := o.options.Flush(ctx); err != nil {
			return err
		}
	}
	if o.options.Next != nil {
		return o.options.Next.Flush(ctx)
	}
	return nil
}
func (o *ManagedTextOutput) SetAttached(attached bool) { o.attached.Store(attached) }
func (o *ManagedTextOutput) OnAttached() {
	o.attached.Store(true)
	if o.options.Attached != nil {
		o.options.Attached()
	}
	if o.options.Next != nil {
		o.options.Next.SetAttached(true)
		o.options.Next.OnAttached()
	}
}
func (o *ManagedTextOutput) OnDetached() {
	o.attached.Store(false)
	if o.options.Detached != nil {
		o.options.Detached()
	}
	if o.options.Next != nil {
		o.options.Next.SetAttached(false)
		o.options.Next.OnDetached()
	}
}

type AgentInput struct {
	mu        sync.RWMutex
	audio     AudioInput
	enabled   bool
	onChanged func()
	onEnabled func(bool)
}

func NewAgentInput(onChanged func(), onEnabled func(bool)) *AgentInput {
	return &AgentInput{enabled: true, onChanged: onChanged, onEnabled: onEnabled}
}
func (i *AgentInput) Audio() AudioInput { i.mu.RLock(); value := i.audio; i.mu.RUnlock(); return value }
func (i *AgentInput) AudioEnabled() bool {
	i.mu.RLock()
	value := i.enabled
	i.mu.RUnlock()
	return value
}
func (i *AgentInput) SetAudioEnabled(enabled bool) {
	i.mu.Lock()
	if i.enabled == enabled {
		i.mu.Unlock()
		return
	}
	i.enabled = enabled
	audio, callback := i.audio, i.onEnabled
	i.mu.Unlock()
	if callback != nil {
		callback(enabled)
	}
	applyAttach(audio, enabled)
}
func (i *AgentInput) SetAudio(audio AudioInput) {
	i.mu.Lock()
	if sameInterface(i.audio, audio) {
		i.mu.Unlock()
		return
	}
	old, enabled, callback := i.audio, i.enabled, i.onChanged
	i.audio = audio
	i.mu.Unlock()
	applyAttach(old, false)
	if callback != nil {
		callback()
	}
	applyAttach(audio, enabled)
}

type AgentOutput struct {
	mu                            sync.RWMutex
	audio                         AudioOutput
	text                          TextOutput
	audioEnabled, textEnabled     bool
	onAudioChanged, onTextChanged func()
}

func NewAgentOutput(onAudioChanged, onTextChanged func()) *AgentOutput {
	return &AgentOutput{audioEnabled: true, textEnabled: true, onAudioChanged: onAudioChanged, onTextChanged: onTextChanged}
}
func (o *AgentOutput) Audio() AudioOutput {
	o.mu.RLock()
	value := o.audio
	o.mu.RUnlock()
	return value
}
func (o *AgentOutput) Transcription() TextOutput {
	o.mu.RLock()
	value := o.text
	o.mu.RUnlock()
	return value
}
func (o *AgentOutput) AudioEnabled() bool {
	o.mu.RLock()
	value := o.audioEnabled
	o.mu.RUnlock()
	return value
}
func (o *AgentOutput) TranscriptionEnabled() bool {
	o.mu.RLock()
	value := o.textEnabled
	o.mu.RUnlock()
	return value
}
func (o *AgentOutput) SetAudioEnabled(enabled bool) {
	o.mu.Lock()
	if o.audioEnabled == enabled {
		o.mu.Unlock()
		return
	}
	o.audioEnabled = enabled
	audio := o.audio
	o.mu.Unlock()
	applyAttach(audio, enabled)
}
func (o *AgentOutput) SetTranscriptionEnabled(enabled bool) {
	o.mu.Lock()
	if o.textEnabled == enabled {
		o.mu.Unlock()
		return
	}
	o.textEnabled = enabled
	text := o.text
	o.mu.Unlock()
	applyAttach(text, enabled)
}
func (o *AgentOutput) SetAudio(audio AudioOutput) {
	o.mu.Lock()
	if sameInterface(o.audio, audio) {
		o.mu.Unlock()
		return
	}
	old, enabled, callback := o.audio, o.audioEnabled, o.onAudioChanged
	o.audio = audio
	o.mu.Unlock()
	applyAttach(old, false)
	if callback != nil {
		callback()
	}
	applyAttach(audio, enabled)
}
func (o *AgentOutput) SetTranscription(text TextOutput) {
	o.mu.Lock()
	if sameInterface(o.text, text) {
		o.mu.Unlock()
		return
	}
	old, enabled, callback := o.text, o.textEnabled, o.onTextChanged
	o.text = text
	o.mu.Unlock()
	applyAttach(old, false)
	if callback != nil {
		callback()
	}
	applyAttach(text, enabled)
}

func applyAttach(value Attachable, attached bool) {
	if value == nil {
		return
	}
	value.SetAttached(attached)
	if attached {
		value.OnAttached()
	} else {
		value.OnDetached()
	}
}

func sameInterface(left, right any) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	lv, rv := reflect.ValueOf(left), reflect.ValueOf(right)
	if lv.Type() != rv.Type() {
		return false
	}
	if lv.Type().Comparable() {
		return lv.Interface() == rv.Interface()
	}
	return false
}
