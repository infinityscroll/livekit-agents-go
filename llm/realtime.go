// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
)

type RealtimeCapabilities struct {
	MessageTruncation            bool
	TurnDetection                bool
	UserTranscription            bool
	AutoToolReplyGeneration      bool
	AudioOutput                  bool
	ManualFunctionCalls          bool
	MidSessionChatContextUpdate  bool
	MidSessionInstructionsUpdate bool
	MidSessionToolsUpdate        bool
	PerResponseToolChoice        bool
	NativeTranscriptSync         bool // Deprecated: retained for provider compatibility.
}

type RealtimeError struct {
	Operation string
	Err       error
}

func (e *RealtimeError) Error() string {
	if e == nil {
		return "realtime operation failed"
	}
	if e.Operation == "" {
		return fmt.Sprintf("realtime operation failed: %v", e.Err)
	}
	return fmt.Sprintf("realtime %s failed: %v", e.Operation, e.Err)
}

func (e *RealtimeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type RealtimeModelError struct {
	Type        string
	Timestamp   time.Time
	Label       string
	Err         error
	Recoverable bool
}

func (e RealtimeModelError) Error() string {
	if e.Err == nil {
		return "realtime model error"
	}
	return e.Err.Error()
}

func (e RealtimeModelError) Unwrap() error { return e.Err }

type InputSpeechStartedEvent struct{}

type InputSpeechStoppedEvent struct {
	UserTranscriptionEnabled bool
}

type InputTranscriptionCompletedEvent struct {
	ItemID        string
	Transcript    string
	IsFinal       bool
	TurnStartedAt *time.Time
}

type RealtimeSessionReconnectedEvent struct{}

// RealtimeText represents the string | TimedString union exposed by the
// TypeScript SDK without resorting to any. Timed is non-nil for aligned text.
type RealtimeText struct {
	Text  string
	Timed *agents.TimedString
}

func PlainRealtimeText(text string) RealtimeText { return RealtimeText{Text: text} }

func TimedRealtimeText(value agents.TimedString) RealtimeText {
	return RealtimeText{Text: value.Text, Timed: &value}
}

type MessageGeneration struct {
	MessageID   string
	TextStream  stream.Reader[RealtimeText]
	AudioStream stream.Reader[agents.AudioFrame]
	// Modalities is nil when the provider does not expose the asynchronous
	// modality result. Providers must return a fresh slice from the callback.
	Modalities func(context.Context) ([]Modality, error)
}

type GenerationCreatedEvent struct {
	MessageStream  stream.Reader[MessageGeneration]
	FunctionStream stream.Reader[*FunctionCall]
	UserInitiated  bool
	ResponseID     string
}

type RealtimeUpdateOptions struct {
	// ToolChoice uses Override so Unset means no change and Null explicitly
	// resets the provider's tool choice.
	ToolChoice agents.Override[ToolChoice]
}

type GenerateRealtimeReplyOptions struct {
	Instructions string
}

type TruncateRealtimeMessageOptions struct {
	MessageID       string
	AudioEnd        time.Duration
	Modalities      []Modality
	AudioTranscript string
}

type RealtimeModel interface {
	Capabilities() RealtimeCapabilities
	Model() string
	Provider() string
	Label() string
	Session(context.Context) (RealtimeSession, error)
	Close(context.Context) error
}

type RealtimeSession interface {
	RealtimeModel() RealtimeModel
	ChatContext() *ChatContext
	Tools() *Context
	UpdateInstructions(context.Context, string) error
	UpdateChatContext(context.Context, *ChatContext) error
	UpdateTools(context.Context, *Context) error
	UpdateOptions(context.Context, RealtimeUpdateOptions) error
	PushAudio(context.Context, agents.AudioFrame) error
	GenerateReply(context.Context, GenerateRealtimeReplyOptions) (GenerationCreatedEvent, error)
	CommitAudio(context.Context) error
	ClearAudio(context.Context) error
	Interrupt(context.Context) error
	Truncate(context.Context, TruncateRealtimeMessageOptions) error
	StartUserActivity()
	SetInputAudioStream(context.Context, stream.Reader[agents.AudioFrame]) error
	Close(context.Context) error

	OnInputSpeechStarted(func(InputSpeechStartedEvent)) func()
	OnInputSpeechStopped(func(InputSpeechStoppedEvent)) func()
	OnInputTranscriptionCompleted(func(InputTranscriptionCompletedEvent)) func()
	OnGenerationCreated(func(GenerationCreatedEvent)) func()
	OnMetrics(func(metrics.Realtime)) func()
	OnError(func(RealtimeModelError)) func()
	OnReconnected(func(RealtimeSessionReconnectedEvent)) func()
}

// RealtimeModelBase supplies the immutable provider metadata and capabilities
// for concrete realtime model implementations.
type RealtimeModelBase struct {
	capabilities RealtimeCapabilities
	label        string
	provider     string
	mu           sync.RWMutex
	model        string
}

func NewRealtimeModelBase(capabilities RealtimeCapabilities, label, provider, model string) *RealtimeModelBase {
	if label == "" {
		label = "RealtimeModel"
	}
	if provider == "" {
		provider = "unknown"
	}
	if model == "" {
		model = "unknown"
	}
	return &RealtimeModelBase{capabilities: capabilities, label: label, provider: provider, model: model}
}

func (b *RealtimeModelBase) Capabilities() RealtimeCapabilities { return b.capabilities }
func (b *RealtimeModelBase) Label() string                      { return b.label }
func (b *RealtimeModelBase) Provider() string                   { return b.provider }
func (b *RealtimeModelBase) Model() string {
	b.mu.RLock()
	model := b.model
	b.mu.RUnlock()
	return model
}
func (b *RealtimeModelBase) SetModel(model string) {
	if model == "" {
		model = "unknown"
	}
	b.mu.Lock()
	b.model = model
	b.mu.Unlock()
}

// RealtimeSessionEvents is intended to be embedded in provider sessions. It
// provides ordered typed subscriptions and isolates callback panics.
type RealtimeSessionEvents struct {
	inputStarted  agents.EventEmitter[InputSpeechStartedEvent]
	inputStopped  agents.EventEmitter[InputSpeechStoppedEvent]
	transcription agents.EventEmitter[InputTranscriptionCompletedEvent]
	generation    agents.EventEmitter[GenerationCreatedEvent]
	metrics       agents.EventEmitter[metrics.Realtime]
	errors        agents.EventEmitter[RealtimeModelError]
	reconnected   agents.EventEmitter[RealtimeSessionReconnectedEvent]
}

func (e *RealtimeSessionEvents) OnInputSpeechStarted(fn func(InputSpeechStartedEvent)) func() {
	return e.inputStarted.Subscribe(fn)
}
func (e *RealtimeSessionEvents) OnInputSpeechStopped(fn func(InputSpeechStoppedEvent)) func() {
	return e.inputStopped.Subscribe(fn)
}
func (e *RealtimeSessionEvents) OnInputTranscriptionCompleted(fn func(InputTranscriptionCompletedEvent)) func() {
	return e.transcription.Subscribe(fn)
}
func (e *RealtimeSessionEvents) OnGenerationCreated(fn func(GenerationCreatedEvent)) func() {
	return e.generation.Subscribe(fn)
}
func (e *RealtimeSessionEvents) OnMetrics(fn func(metrics.Realtime)) func() {
	return e.metrics.Subscribe(fn)
}
func (e *RealtimeSessionEvents) OnError(fn func(RealtimeModelError)) func() {
	return e.errors.Subscribe(fn)
}
func (e *RealtimeSessionEvents) OnReconnected(fn func(RealtimeSessionReconnectedEvent)) func() {
	return e.reconnected.Subscribe(fn)
}

func (e *RealtimeSessionEvents) EmitInputSpeechStarted(event InputSpeechStartedEvent) {
	e.inputStarted.Emit(event)
}
func (e *RealtimeSessionEvents) EmitInputSpeechStopped(event InputSpeechStoppedEvent) {
	e.inputStopped.Emit(event)
}
func (e *RealtimeSessionEvents) EmitInputTranscriptionCompleted(event InputTranscriptionCompletedEvent) {
	e.transcription.Emit(event)
}
func (e *RealtimeSessionEvents) EmitGenerationCreated(event GenerationCreatedEvent) {
	e.generation.Emit(event)
}
func (e *RealtimeSessionEvents) EmitMetrics(metric metrics.Realtime) { e.metrics.Emit(metric) }
func (e *RealtimeSessionEvents) EmitError(event RealtimeModelError) {
	if event.Type == "" {
		event.Type = "realtime_model_error"
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	e.errors.Emit(event)
}
func (e *RealtimeSessionEvents) EmitReconnected(event RealtimeSessionReconnectedEvent) {
	e.reconnected.Emit(event)
}

type RealtimeSessionUpdate struct {
	Instructions *string
	ChatContext  *ChatContext
	Tools        *Context
}

// UpdateRealtimeSessionBestEffort preserves the JS compatibility behavior:
// each update is attempted in order, failures are logged, and later updates
// still run. Providers should use the individual methods when errors matter.
func UpdateRealtimeSessionBestEffort(ctx context.Context, session RealtimeSession, update RealtimeSessionUpdate) {
	if session == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if update.Instructions != nil {
		if err := session.UpdateInstructions(ctx, *update.Instructions); err != nil {
			slog.Error("failed to update realtime instructions", "error", err)
		}
	}
	if update.ChatContext != nil {
		if err := session.UpdateChatContext(ctx, update.ChatContext); err != nil {
			slog.Error("failed to update realtime chat context", "error", err)
		}
	}
	if update.Tools != nil {
		if err := session.UpdateTools(ctx, update.Tools); err != nil {
			slog.Error("failed to update realtime tools", "error", err)
		}
	}
}

// RealtimeAudioInput owns the single replaceable input-audio pump used by a
// realtime session. It creates no goroutine until a stream is attached, never
// polls, and waits for the previous pump to stop before replacement so frames
// from two sources cannot be reordered.
type RealtimeAudioInput struct {
	operationMu sync.Mutex
	mu          sync.Mutex
	ctx         context.Context
	cancelBase  context.CancelCauseFunc
	push        func(context.Context, agents.AudioFrame) error
	onError     func(error)
	cancelInput context.CancelCauseFunc
	done        chan struct{}
	closed      bool
}

func NewRealtimeAudioInput(parent context.Context, push func(context.Context, agents.AudioFrame) error, onError func(error)) (*RealtimeAudioInput, error) {
	if push == nil {
		return nil, errors.New("realtime audio push callback is required")
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	close(done)
	return &RealtimeAudioInput{ctx: ctx, cancelBase: cancel, push: push, onError: onError, done: done}, nil
}

// Replace attaches source, or detaches the current source when source is nil.
func (i *RealtimeAudioInput) Replace(ctx context.Context, source stream.Reader[agents.AudioFrame]) error {
	if i == nil {
		return stream.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	i.operationMu.Lock()
	defer i.operationMu.Unlock()

	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return stream.ErrClosed
	}
	oldCancel, oldDone := i.cancelInput, i.done
	i.cancelInput = nil
	i.mu.Unlock()
	if oldCancel != nil {
		oldCancel(stream.ErrClosed)
	}
	select {
	case <-oldDone:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	if source == nil || isNilInterface(source) {
		return nil
	}

	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return stream.ErrClosed
	}
	runContext, cancel := context.WithCancelCause(i.ctx)
	done := make(chan struct{})
	i.cancelInput, i.done = cancel, done
	i.mu.Unlock()
	go i.pump(runContext, source, done)
	return nil
}

func (i *RealtimeAudioInput) pump(ctx context.Context, source stream.Reader[agents.AudioFrame], done chan struct{}) {
	defer close(done)
	for {
		frame, err := source.Recv(ctx)
		if err != nil {
			if context.Cause(ctx) == nil && !errors.Is(err, io.EOF) && i.onError != nil {
				i.onError(err)
			}
			return
		}
		if err := i.push(ctx, frame); err != nil {
			if context.Cause(ctx) == nil && !errors.Is(err, stream.ErrClosed) && i.onError != nil {
				i.onError(err)
			}
			return
		}
	}
}

func (i *RealtimeAudioInput) Close(ctx context.Context) error {
	if i == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	i.operationMu.Lock()
	defer i.operationMu.Unlock()
	i.mu.Lock()
	if !i.closed {
		i.closed = true
		i.cancelBase(stream.ErrClosed)
		if i.cancelInput != nil {
			i.cancelInput(stream.ErrClosed)
		}
	}
	done := i.done
	i.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func CloneModalities(values []Modality) []Modality { return slices.Clone(values) }
