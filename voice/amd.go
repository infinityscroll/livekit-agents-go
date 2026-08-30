// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/inference"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// AMDCategory is the pinned agents-js 1.7.1 answering-machine verdict.
type AMDCategory string

const (
	AMDCategoryHuman              AMDCategory = "human"
	AMDCategoryMachineIVR         AMDCategory = "machine-ivr"
	AMDCategoryMachineVM          AMDCategory = "machine-vm"
	AMDCategoryMachineUnavailable AMDCategory = "machine-unavailable"
	AMDCategoryUncertain          AMDCategory = "uncertain"
)

const (
	DefaultAMDHumanSpeechThreshold    = 2500 * time.Millisecond
	DefaultAMDHumanSilenceThreshold   = 500 * time.Millisecond
	DefaultAMDMachineSilenceThreshold = 1500 * time.Millisecond
	DefaultAMDNoSpeechTimeout         = 10 * time.Second
	DefaultAMDDetectionTimeout        = 20 * time.Second
	DefaultAMDMaxEndpointingDelay     = 3 * time.Second
	DefaultAMDTrackPublicationTimeout = 5 * time.Second
	DefaultAMDInterruptTimeout        = 5 * time.Second
	DefaultAMDCloseTimeout            = 5 * time.Second
	DefaultAMDMaxExtensions           = 3
	DefaultAMDMaxExtension            = 10 * time.Second
	DefaultAMDLLMModel                = "google/gemini-3.1-flash-lite"
	DefaultAMDSTTModel                = "cartesia/ink-whisper"
)

// AMDPrompt is the benchmarked classifier prompt from agents-js 1.7.1 and the
// Python AMD classifier. Its examples intentionally remain verbatim.
const AMDPrompt = `Task:
Classify the call greeting transcript into exactly one of these categories:

human: A person answered (e.g., "Hello?", "This is John.").
machine-ivr: A prompt to press a key (e.g., "Press 1 to continue").
machine-vm: A voicemail greeting where leaving a message IS possible.
machine-unavailable: Any greeting indicating it's NOT possible to leave message, eg because mailbox is full, not setup, etc.
uncertain: For partial transcripts that are ambiguous.

Examples:
Input: "The person you called has a voice mailbox that hasn't been set up yet. Goodbye."
Output: machine-unavailable

Input: "Thank you for calling Truly Pizza in Dana Pointe. Our hours of operation are 11AM to 8PM, Sunday through Thursday, 11AM to 9PM, Friday and Saturday, and we're closed on Tuesdays."
Output: uncertain

Input: "You for calling Truly Pizza in Dana Pointe. Our hours of operation are 11AM to 8PM, Sunday through Thursday, 11AM to 9PM, Friday and Saturday, and we're closed on Tuesdays. If you'd like to place an order, please press 1 or head to our website to order online for pickup and local delivery."
Output: machine-ivr

Input: "Please state your name and why you're calling, and I will check if the person is available"
Output: machine-ivr
Note: this should apply for any call screening prompts.

Input: "I'm away from my desk. If you leave a message, I will get back to you."
Output: machine-vm

Input: "Hello, this is Lisa."
Output: human`

const EventAMDPrediction EventType = "amd_prediction"

const (
	amdSpanName               = "answering_machine_detection"
	amdAttrCategory           = "lk.amd.category"
	amdAttrReason             = "lk.amd.reason"
	amdAttrIsMachine          = "lk.amd.is_machine"
	amdAttrInterruptOnMachine = "lk.amd.interrupt_on_machine"
	amdAttrSpeechDuration     = "lk.amd.speech_duration"
	amdAttrDelay              = "lk.amd.delay"
	amdAttrTranscript         = "lk.pii.amd.transcript"
	amdAttrOperationName      = "gen_ai.operation.name"
)

var (
	ErrAMDClosed         = errors.New("voice AMD is closed")
	ErrAMDAlreadyRunning = errors.New("voice AMD execute is already running")
	ErrAMDNoLLM          = errors.New("voice AMD has no LLM available")
	ErrAMDInvalidOptions = errors.New("voice AMD options are invalid")
	ErrAMDNoAudioSource  = errors.New("voice AMD dedicated STT has no audio source")
	errAMDRunComplete    = errors.New("voice AMD run complete")
)

// AMDOptionError identifies a rejected option while remaining compatible with
// errors.Is(err, ErrAMDInvalidOptions).
type AMDOptionError struct {
	Field string
	Err   error
}

func (e *AMDOptionError) Error() string {
	if e == nil {
		return ErrAMDInvalidOptions.Error()
	}
	return fmt.Sprintf("voice AMD option %s: %v", e.Field, e.Err)
}
func (e *AMDOptionError) Unwrap() []error { return []error{ErrAMDInvalidOptions, e.Err} }

// AMDPanicError converts extension callback/provider panics into ordinary,
// inspectable errors rather than allowing a media or lifecycle goroutine to die.
type AMDPanicError struct {
	Operation string
	Value     any
	Stack     []byte
}

func (e *AMDPanicError) Error() string {
	return fmt.Sprintf("voice AMD %s panicked: %v", e.Operation, e.Value)
}

// AMDPredictionEvent is emitted once for every successful Execute run.
// Durations retain the exact millisecond wire names used by agents-js.
type AMDPredictionEvent struct {
	EventBase
	Category         AMDCategory `json:"category"`
	Transcript       string      `json:"transcript"`
	Reason           string      `json:"reason"`
	RawResponse      string      `json:"rawResponse"`
	IsMachine        bool        `json:"isMachine"`
	SpeechDurationMS int64       `json:"speechDurationMs"`
	DelayMS          int64       `json:"delayMs"`
}

func (e AMDPredictionEvent) SpeechDuration() time.Duration {
	return time.Duration(e.SpeechDurationMS) * time.Millisecond
}
func (e AMDPredictionEvent) Delay() time.Duration {
	return time.Duration(e.DelayMS) * time.Millisecond
}

// AMDMetrics is the allocation-light lifecycle metric emitted with a verdict.
// Model-specific token/audio metrics remain available via OnLLMMetrics and
// OnSTTMetrics.
type AMDMetrics struct {
	Timestamp      time.Time
	Duration       time.Duration
	Category       AMDCategory
	Reason         string
	IsMachine      bool
	SpeechDuration time.Duration
	Delay          time.Duration
	Transcript     string
}

type AMDErrorEvent struct {
	Timestamp time.Time
	Operation string
	Err       error
}

// AMDTranscriptSource prevents the session STT and a dedicated AMD STT from
// double-feeding the classifier.
type AMDTranscriptSource string

const (
	AMDTranscriptSourceSessionSTT   AMDTranscriptSource = "stt"
	AMDTranscriptSourceDedicatedSTT AMDTranscriptSource = "amd_stt"
)

// AMDListeningGate is the RTC/RoomIO seam. Implementations wait until the
// selected participant has subscribed audio and, for SIP, callStatus=active.
// Returning an error settles the run as uncertain/participant_missing.
type AMDListeningGate interface {
	WaitForAudio(context.Context, string) error
}

type AMDListeningGateFunc func(context.Context, string) error

func (f AMDListeningGateFunc) WaitForAudio(ctx context.Context, identity string) error {
	if f == nil {
		return errors.New("nil AMD listening gate")
	}
	return f(ctx, identity)
}

// AMDAudioSubscription is an independently closable branch of participant
// audio. RoomIO's primary AudioInput is single-consumer, so adapters must tee
// frames rather than return that primary reader directly.
type AMDAudioSubscription interface {
	Recv(context.Context) (agents.AudioFrame, error)
	Close() error
}

type AMDAudioSource interface {
	SubscribeAMD(context.Context) (AMDAudioSubscription, error)
}

type AMDAudioSourceFunc func(context.Context) (AMDAudioSubscription, error)

func (f AMDAudioSourceFunc) SubscribeAMD(ctx context.Context) (AMDAudioSubscription, error) {
	if f == nil {
		return nil, ErrAMDNoAudioSource
	}
	return f(ctx)
}

// AMDSessionAdapter is the narrow test/remote-session seam. Subscribe may be
// nil when the application drives the recognition hooks manually. CurrentLLM
// is consulted only when neither an explicit LLM nor Cloud auto-selection is
// available.
type AMDSessionAdapter struct {
	PauseReplyAuthorization  func() error
	ResumeReplyAuthorization func() error
	Subscribe                func(EventSubscriptionOptions) (*EventSubscription, error)
	CurrentLLM               func() llm.LLM
	MaxEndpointingDelay      func() time.Duration
	Interrupt                func(context.Context, bool) error
	PublishPrediction        func(context.Context, AMDPredictionEvent) error
	Bind                     func(*AMD)
}

// AMDOptions preserves the agents-js option semantics. Pointer scalar fields
// distinguish omission from an intentional zero/false value.
type AMDOptions struct {
	LLM      llm.LLM
	LLMModel string
	STT      stt.STT
	STTModel string

	InterruptOnMachine           *bool
	NoSpeechTimeout              *time.Duration
	DetectionTimeout             *time.Duration
	HumanSpeechThreshold         *time.Duration
	HumanSilenceThreshold        *time.Duration
	MachineSilenceThreshold      *time.Duration
	WaitUntilFinished            *bool
	MaxEndpointingDelay          *time.Duration
	Prompt                       *string
	ParticipantIdentity          string
	SuppressCompatibilityWarning bool

	ListeningGate           AMDListeningGate
	TrackPublicationTimeout *time.Duration
	AudioSource             AMDAudioSource
	Interrupt               func(context.Context, AMDPredictionEvent) error
	InterruptTimeout        time.Duration
	CloseTimeout            time.Duration
	EventCapacity           int
	OnError                 func(AMDErrorEvent)
}

type resolvedAMDOptions struct {
	interruptOnMachine      bool
	noSpeechTimeout         time.Duration
	detectionTimeout        time.Duration
	humanSpeechThreshold    time.Duration
	humanSilenceThreshold   time.Duration
	machineSilenceThreshold time.Duration
	waitUntilFinished       bool
	maxEndpointingDelay     time.Duration
	prompt                  string
	participantIdentity     string
	listeningGate           AMDListeningGate
	trackTimeout            time.Duration
	audioSource             AMDAudioSource
	interrupt               func(context.Context, AMDPredictionEvent) error
	interruptTimeout        time.Duration
	closeTimeout            time.Duration
	eventCapacity           int
}

// AMDExecution is a one-shot result handle analogous to the Promise returned
// by agents-js execute(). Wait does not cancel the run; the context passed to
// Start owns execution, while Wait's context only bounds that wait.
type AMDExecution struct {
	done   chan struct{}
	once   sync.Once
	mu     sync.RWMutex
	event  AMDPredictionEvent
	err    error
	cancel context.CancelCauseFunc
}

func (e *AMDExecution) Wait(ctx context.Context) (AMDPredictionEvent, error) {
	if e == nil {
		return AMDPredictionEvent{}, ErrAMDClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-e.done:
		e.mu.RLock()
		event, err := e.event, e.err
		e.mu.RUnlock()
		return event, err
	case <-ctx.Done():
		return AMDPredictionEvent{}, context.Cause(ctx)
	}
}

func (e *AMDExecution) Done() <-chan struct{} {
	if e == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return e.done
}

// Cancel stops this execution without closing the reusable detector.
func (e *AMDExecution) Cancel(cause error) {
	if e == nil || e.cancel == nil {
		return
	}
	if cause == nil {
		cause = context.Canceled
	}
	e.cancel(cause)
}

func (e *AMDExecution) complete(event AMDPredictionEvent, err error) {
	e.once.Do(func() {
		e.mu.Lock()
		e.event, e.err = event, err
		e.mu.Unlock()
		close(e.done)
	})
}

// AMD is a reusable detector. Runs are sequential; Close is permanent and
// closes only inference models constructed from model strings or auto-selection.
type AMD struct {
	session  AMDSessionAdapter
	opts     resolvedAMDOptions
	llm      llm.LLM
	stt      stt.STT
	llmOwned bool
	sttOwned bool

	operationMu sync.Mutex
	mu          sync.RWMutex
	active      *amdRun
	last        *AMDPredictionEvent
	closed      bool

	predictions agents.EventEmitter[AMDPredictionEvent]
	amdMetrics  agents.EventEmitter[AMDMetrics]
	errors      agents.EventEmitter[AMDErrorEvent]

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// NewAMD binds AMD to a concrete AgentSession without requiring session or
// activity source changes.
func NewAMD[UserData any](session *AgentSession[UserData], options AMDOptions) (*AMD, error) {
	if session == nil {
		return nil, &AMDOptionError{Field: "session", Err: errors.New("session is required")}
	}
	adapter := AdaptAMDSession(session)
	amd, err := NewAMDWithSession(adapter, options)
	if err != nil {
		return nil, err
	}
	return amd, nil
}

// MustAMD is the configuration-time convenience counterpart of NewAMD.
func MustAMD[UserData any](session *AgentSession[UserData], options AMDOptions) *AMD {
	amd, err := NewAMD(session, options)
	if err != nil {
		panic(err)
	}
	return amd
}

func NewAMDWithSession(session AMDSessionAdapter, options AMDOptions) (*AMD, error) {
	resolved, err := resolveAMDOptions(session, options)
	if err != nil {
		return nil, err
	}
	model, speech, llmOwned, sttOwned, err := resolveAMDModels(session, options)
	if err != nil {
		return nil, err
	}
	amd := &AMD{
		session: session, opts: resolved, llm: model, stt: speech,
		llmOwned: llmOwned, sttOwned: sttOwned, closeDone: make(chan struct{}),
	}
	if options.OnError != nil {
		amd.errors.Subscribe(options.OnError)
	}
	if !options.SuppressCompatibilityWarning {
		modelName, modelErr := guardAMDValue("read LLM model name", func() (string, error) {
			return model.Model(), nil
		})
		if modelErr != nil {
			closeResolvedAMDModels(model, speech, llmOwned, sttOwned, resolved.closeTimeout)
			return nil, modelErr
		}
		warnAMDModel(modelName, evaluatedAMDLLMModels[:], "llm")
		if speech != nil {
			speechName, speechErr := guardAMDValue("read STT model name", func() (string, error) {
				return speech.Model(), nil
			})
			if speechErr != nil {
				closeResolvedAMDModels(model, speech, llmOwned, sttOwned, resolved.closeTimeout)
				return nil, speechErr
			}
			warnAMDModel(speechName, evaluatedAMDSTTModels[:], "stt")
		}
	}
	if session.Bind != nil {
		if bindErr := guardAMD("session bind", func() error { session.Bind(amd); return nil }); bindErr != nil {
			closeResolvedAMDModels(model, speech, llmOwned, sttOwned, resolved.closeTimeout)
			return nil, bindErr
		}
	}
	return amd, nil
}

// MustAMDWithSession panics only for invalid static configuration.
func MustAMDWithSession(session AMDSessionAdapter, options AMDOptions) *AMD {
	amd, err := NewAMDWithSession(session, options)
	if err != nil {
		panic(err)
	}
	return amd
}

func (a *AMD) OnPrediction(fn func(AMDPredictionEvent)) func() {
	return a.predictions.Subscribe(fn)
}
func (a *AMD) OnMetrics(fn func(AMDMetrics)) func()     { return a.amdMetrics.Subscribe(fn) }
func (a *AMD) OnError(fn func(AMDErrorEvent)) func()    { return a.errors.Subscribe(fn) }
func (a *AMD) OnLLMMetrics(fn func(metrics.LLM)) func() { return a.llm.OnMetrics(fn) }
func (a *AMD) OnSTTMetrics(fn func(metrics.STT)) func() {
	if a.stt == nil {
		return func() {}
	}
	return a.stt.OnMetrics(fn)
}

func (a *AMD) Active() bool {
	a.mu.RLock()
	active := a.active != nil
	a.mu.RUnlock()
	return active
}
func (a *AMD) Listening() bool {
	a.mu.RLock()
	run := a.active
	a.mu.RUnlock()
	return run != nil && run.listening.Load()
}
func (a *AMD) Closed() bool {
	a.mu.RLock()
	closed := a.closed
	a.mu.RUnlock()
	return closed
}
func (a *AMD) LastPrediction() (AMDPredictionEvent, bool) {
	a.mu.RLock()
	if a.last == nil {
		a.mu.RUnlock()
		return AMDPredictionEvent{}, false
	}
	result := *a.last
	a.mu.RUnlock()
	return result, true
}

// Start begins detection and returns immediately, matching the Promise-style
// concurrency used to start AMD before dialing a SIP participant.
func (a *AMD) Start(parent context.Context) (*AMDExecution, error) {
	if parent == nil {
		parent = context.Background()
	}
	if err := context.Cause(parent); err != nil {
		return nil, err
	}
	a.operationMu.Lock()
	defer a.operationMu.Unlock()
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, ErrAMDClosed
	}
	if a.active != nil {
		a.mu.Unlock()
		return nil, ErrAMDAlreadyRunning
	}
	a.mu.Unlock()

	if a.session.PauseReplyAuthorization != nil {
		if err := guardAMD("pause reply authorization", a.session.PauseReplyAuthorization); err != nil {
			return nil, err
		}
	}

	spanCtx, span := otel.Tracer("github.com/infinityscroll/livekit-agents-go/voice").Start(parent, amdSpanName,
		trace.WithAttributes(
			attribute.Bool(amdAttrInterruptOnMachine, a.opts.interruptOnMachine),
			attribute.String(amdAttrOperationName, "classification"),
		))
	run, err := newAMDRun(spanCtx, a, span)
	if err != nil {
		span.End()
		a.resumeReplies("run setup rollback")
		return nil, err
	}
	a.mu.Lock()
	if a.closed || a.active != nil {
		a.mu.Unlock()
		run.cancel(ErrAMDClosed)
		run.closeResources()
		span.End()
		a.resumeReplies("concurrent start rollback")
		if a.closed {
			return nil, ErrAMDClosed
		}
		return nil, ErrAMDAlreadyRunning
	}
	a.active = run
	a.mu.Unlock()
	run.start()
	go a.completeRun(run)
	return run.execution, nil
}

// Execute starts one run and waits for its one-shot prediction.
func (a *AMD) Execute(ctx context.Context) (AMDPredictionEvent, error) {
	execution, err := a.Start(ctx)
	if err != nil {
		return AMDPredictionEvent{}, err
	}
	return execution.Wait(ctx)
}

func (a *AMD) completeRun(run *amdRun) {
	outcome := <-run.outcome
	if outcome.err == nil && outcome.event.IsMachine && a.opts.interruptOnMachine {
		a.interrupt(outcome.event)
	}
	a.resumeReplies("run completion")

	a.mu.Lock()
	if a.active == run {
		a.active = nil
	}
	if outcome.err == nil {
		value := outcome.event
		a.last = &value
	}
	a.mu.Unlock()
	run.execution.complete(outcome.event, outcome.err)

	if outcome.err != nil {
		run.span.RecordError(outcome.err, trace.WithStackTrace(true))
		run.span.SetStatus(codes.Error, outcome.err.Error())
		run.span.End()
		return
	}
	a.setSpanResult(run.span, outcome.event)
	run.span.End()

	// Match agents-js ordering: forward to the owning session, then notify
	// detector-local listeners. Both are panic/error isolated.
	if a.session.PublishPrediction != nil {
		publishCtx, cancel := context.WithTimeout(context.Background(), a.opts.interruptTimeout)
		err := guardAMD("publish AMD prediction", func() error {
			return a.session.PublishPrediction(publishCtx, outcome.event)
		})
		cancel()
		if err != nil {
			a.report("publish prediction", err)
		}
	}
	a.predictions.Emit(outcome.event)
	a.amdMetrics.Emit(AMDMetrics{
		Timestamp: time.Now(), Duration: time.Since(run.started),
		Category: outcome.event.Category, Reason: outcome.event.Reason,
		IsMachine: outcome.event.IsMachine, SpeechDuration: outcome.event.SpeechDuration(),
		Delay: outcome.event.Delay(), Transcript: outcome.event.Transcript,
	})
}

func (a *AMD) interrupt(event AMDPredictionEvent) {
	callback := a.opts.interrupt
	if callback == nil && a.session.Interrupt != nil {
		callback = func(ctx context.Context, _ AMDPredictionEvent) error {
			return a.session.Interrupt(ctx, true)
		}
	}
	if callback == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.opts.interruptTimeout)
	err := guardAMD("interrupt on machine", func() error { return callback(ctx, event) })
	cancel()
	if err != nil && !errors.Is(err, ErrNoSpeech) {
		a.report("interrupt", err)
	}
}

func (a *AMD) resumeReplies(operation string) {
	if a.session.ResumeReplyAuthorization == nil {
		return
	}
	if err := guardAMD("resume reply authorization", a.session.ResumeReplyAuthorization); err != nil {
		a.report(operation, err)
	}
}

func (a *AMD) setSpanResult(span trace.Span, result AMDPredictionEvent) {
	if span == nil {
		return
	}
	span.SetAttributes(
		attribute.String(amdAttrCategory, string(result.Category)),
		attribute.String(amdAttrReason, result.Reason),
		attribute.Bool(amdAttrIsMachine, result.IsMachine),
		attribute.Int64(amdAttrSpeechDuration, result.SpeechDurationMS),
		attribute.Int64(amdAttrDelay, result.DelayMS),
		attribute.String(amdAttrTranscript, result.Transcript),
	)
}

func (a *AMD) report(operation string, err error) {
	if err == nil {
		return
	}
	event := AMDErrorEvent{Timestamp: time.Now(), Operation: operation, Err: err}
	a.errors.Emit(event)
	slog.Warn("AMD operation failed", "operation", operation, "error", err)
}

// Close permanently cancels an active run and releases AMD-owned models. It
// is idempotent; caller-supplied LLM/STT instances remain caller-owned.
func (a *AMD) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.closeOnce.Do(func() { go a.closeWorker() })
	select {
	case <-a.closeDone:
		return a.closeErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// AClose is the migration alias for agents-js aclose().
func (a *AMD) AClose(ctx context.Context) error { return a.Close(ctx) }

func (a *AMD) closeWorker() {
	defer close(a.closeDone)
	a.operationMu.Lock()
	a.mu.Lock()
	a.closed = true
	run := a.active
	a.mu.Unlock()
	if a.session.Bind != nil {
		if err := guardAMD("session unbind", func() error { a.session.Bind(nil); return nil }); err != nil {
			a.closeErr = errors.Join(a.closeErr, err)
		}
	}
	if run != nil {
		run.cancel(ErrAMDClosed)
	}
	a.operationMu.Unlock()

	closeCtx, cancel := context.WithTimeout(context.Background(), a.opts.closeTimeout)
	defer cancel()
	if run != nil {
		select {
		case <-run.execution.Done():
		case <-closeCtx.Done():
			a.closeErr = errors.Join(a.closeErr, context.Cause(closeCtx))
		}
	}
	if a.sttOwned && a.stt != nil {
		if err := guardAMD("close owned STT", func() error { return a.stt.Close(closeCtx) }); err != nil {
			a.closeErr = errors.Join(a.closeErr, fmt.Errorf("close AMD STT: %w", err))
		}
	}
	if a.llmOwned && a.llm != nil {
		if err := guardAMD("close owned LLM", func() error { return a.llm.Close(closeCtx) }); err != nil {
			a.closeErr = errors.Join(a.closeErr, fmt.Errorf("close AMD LLM: %w", err))
		}
	}
}

func resolveAMDOptions(session AMDSessionAdapter, options AMDOptions) (resolvedAMDOptions, error) {
	boolValue := func(value *bool, fallback bool) bool {
		if value == nil {
			return fallback
		}
		return *value
	}
	durationValue := func(value *time.Duration, fallback time.Duration) time.Duration {
		if value == nil {
			return fallback
		}
		return *value
	}
	maxEndpointing := DefaultAMDMaxEndpointingDelay
	if options.MaxEndpointingDelay != nil {
		maxEndpointing = *options.MaxEndpointingDelay
	} else if session.MaxEndpointingDelay != nil {
		value, err := guardAMDValue("read session max endpointing delay", func() (time.Duration, error) {
			return session.MaxEndpointingDelay(), nil
		})
		if err != nil {
			return resolvedAMDOptions{}, err
		}
		if value >= 0 {
			maxEndpointing = value
		}
	}
	prompt := AMDPrompt
	if options.Prompt != nil {
		prompt = *options.Prompt
	}
	resolved := resolvedAMDOptions{
		interruptOnMachine:      boolValue(options.InterruptOnMachine, true),
		noSpeechTimeout:         durationValue(options.NoSpeechTimeout, DefaultAMDNoSpeechTimeout),
		detectionTimeout:        durationValue(options.DetectionTimeout, DefaultAMDDetectionTimeout),
		humanSpeechThreshold:    durationValue(options.HumanSpeechThreshold, DefaultAMDHumanSpeechThreshold),
		humanSilenceThreshold:   durationValue(options.HumanSilenceThreshold, DefaultAMDHumanSilenceThreshold),
		machineSilenceThreshold: durationValue(options.MachineSilenceThreshold, DefaultAMDMachineSilenceThreshold),
		waitUntilFinished:       boolValue(options.WaitUntilFinished, true),
		maxEndpointingDelay:     maxEndpointing, prompt: prompt,
		participantIdentity: strings.TrimSpace(options.ParticipantIdentity),
		listeningGate:       options.ListeningGate,
		trackTimeout:        durationValue(options.TrackPublicationTimeout, DefaultAMDTrackPublicationTimeout),
		audioSource:         options.AudioSource, interrupt: options.Interrupt,
		interruptTimeout: options.InterruptTimeout, closeTimeout: options.CloseTimeout,
		eventCapacity: options.EventCapacity,
	}
	if resolved.interruptTimeout == 0 {
		resolved.interruptTimeout = DefaultAMDInterruptTimeout
	}
	if resolved.closeTimeout == 0 {
		resolved.closeTimeout = DefaultAMDCloseTimeout
	}
	if resolved.eventCapacity == 0 {
		resolved.eventCapacity = DefaultEventSubscriberCapacity
	}
	for field, value := range map[string]time.Duration{
		"NoSpeechTimeout":         resolved.noSpeechTimeout,
		"DetectionTimeout":        resolved.detectionTimeout,
		"HumanSpeechThreshold":    resolved.humanSpeechThreshold,
		"HumanSilenceThreshold":   resolved.humanSilenceThreshold,
		"MachineSilenceThreshold": resolved.machineSilenceThreshold,
		"MaxEndpointingDelay":     resolved.maxEndpointingDelay,
		"TrackPublicationTimeout": resolved.trackTimeout,
		"InterruptTimeout":        resolved.interruptTimeout,
		"CloseTimeout":            resolved.closeTimeout,
	} {
		if value < 0 {
			return resolvedAMDOptions{}, &AMDOptionError{Field: field, Err: errors.New("must not be negative")}
		}
	}
	if resolved.eventCapacity < 1 {
		return resolvedAMDOptions{}, &AMDOptionError{Field: "EventCapacity", Err: errors.New("must be positive")}
	}
	if options.LLM != nil && options.LLMModel != "" {
		return resolvedAMDOptions{}, &AMDOptionError{Field: "LLM", Err: errors.New("set either LLM or LLMModel, not both")}
	}
	if options.STT != nil && options.STTModel != "" {
		return resolvedAMDOptions{}, &AMDOptionError{Field: "STT", Err: errors.New("set either STT or STTModel, not both")}
	}
	return resolved, nil
}

func resolveAMDModels(session AMDSessionAdapter, options AMDOptions) (llm.LLM, stt.STT, bool, bool, error) {
	model, speech := options.LLM, options.STT
	llmModel, sttModel := strings.TrimSpace(options.LLMModel), strings.TrimSpace(options.STTModel)
	if (model == nil && llmModel == "") || (speech == nil && sttModel == "") {
		if amdCloudAutoSelectionAvailable() {
			if model == nil && llmModel == "" {
				llmModel = DefaultAMDLLMModel
			}
			if speech == nil && sttModel == "" {
				sttModel = DefaultAMDSTTModel
			}
		}
	}
	var llmOwned, sttOwned bool
	var err error
	if model == nil && llmModel != "" {
		model, err = inference.LLMFromModelString(llmModel)
		if err != nil {
			return nil, nil, false, false, fmt.Errorf("resolve AMD LLM model %q: %w", llmModel, err)
		}
		llmOwned = true
	}
	if model == nil && session.CurrentLLM != nil {
		model, err = guardAMDValue("read session LLM", func() (llm.LLM, error) {
			return session.CurrentLLM(), nil
		})
		if err != nil {
			return nil, nil, false, false, err
		}
	}
	if model == nil {
		return nil, nil, false, false, fmt.Errorf("%w: set Cloud credentials or pass AMDOptions.LLM", ErrAMDNoLLM)
	}
	if speech == nil && sttModel != "" {
		speech, err = inference.STTFromModelString(sttModel)
		if err != nil {
			if llmOwned {
				closeResolvedAMDModels(model, nil, true, false, DefaultAMDCloseTimeout)
			}
			return nil, nil, false, false, fmt.Errorf("resolve AMD STT model %q: %w", sttModel, err)
		}
		sttOwned = true
	}
	if speech != nil {
		capabilities, capabilitiesErr := guardAMDValue("read STT capabilities", func() (stt.Capabilities, error) {
			return speech.Capabilities(), nil
		})
		if capabilitiesErr != nil {
			if llmOwned || sttOwned {
				closeResolvedAMDModels(model, speech, llmOwned, sttOwned, DefaultAMDCloseTimeout)
			}
			return nil, nil, false, false, capabilitiesErr
		}
		if !capabilities.Streaming {
			if llmOwned || sttOwned {
				closeResolvedAMDModels(model, speech, llmOwned, sttOwned, DefaultAMDCloseTimeout)
			}
			return nil, nil, false, false, &AMDOptionError{Field: "STT", Err: errors.New("dedicated AMD STT must support streaming")}
		}
	}
	return model, speech, llmOwned, sttOwned, nil
}

func closeResolvedAMDModels(model llm.LLM, speech stt.STT, closeLLM, closeSTT bool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if closeSTT && speech != nil {
		_ = guardAMD("close resolved AMD STT", func() error { return speech.Close(ctx) })
	}
	if closeLLM && model != nil {
		_ = guardAMD("close resolved AMD LLM", func() error { return model.Close(ctx) })
	}
}

func amdCloudAutoSelectionAvailable() bool {
	parsed, err := url.Parse(strings.TrimSpace(os.Getenv("LIVEKIT_URL")))
	if err != nil || parsed.Hostname() == "" {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	cloud := strings.HasSuffix(host, ".livekit.cloud") || strings.HasSuffix(host, ".livekit.run")
	if !cloud {
		return false
	}
	key := firstAMDEnvironment("LIVEKIT_INFERENCE_API_KEY", "LIVEKIT_API_KEY")
	secret := firstAMDEnvironment("LIVEKIT_INFERENCE_API_SECRET", "LIVEKIT_API_SECRET")
	return key != "" && secret != ""
}

func firstAMDEnvironment(names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

var evaluatedAMDLLMModels = [...]string{
	"google/gemini-3.1-flash-lite", "google/gemini-3-flash-preview",
	"openai/gpt-4.1", "openai/gpt-5.2", "openai/gpt-5.4",
	"openai/gpt-5.1", "openai/gpt-4o", "openai/gpt-5.1-chat-latest",
	"openai/gpt-4.1-mini", "openai/gpt-4.1-nano",
	"openai/gpt-5.2-chat-latest", "google/gemini-2.5-flash-lite",
}
var evaluatedAMDSTTModels = [...]string{
	"deepgram/nova-3", "assemblyai/universal-streaming-multilingual",
	"cartesia/ink-whisper",
}

func warnAMDModel(model string, evaluated []string, kind string) {
	if model == "" || model == "unknown" {
		return
	}
	lower := strings.ToLower(model)
	for _, candidate := range evaluated {
		candidate = strings.ToLower(candidate)
		if lower == candidate || strings.Contains(candidate, lower) || strings.Contains(lower, candidate) {
			return
		}
	}
	slog.Warn(kind+" model hasn't been evaluated with the AMD benchmark; it might not be compatible",
		"model", model, "suppressOption", "SuppressCompatibilityWarning")
}

func guardAMD(operation string, callback func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &AMDPanicError{Operation: operation, Value: recovered, Stack: debug.Stack()}
		}
	}()
	return callback()
}

func isAMDCategory(value AMDCategory) bool {
	switch value {
	case AMDCategoryHuman, AMDCategoryMachineIVR, AMDCategoryMachineVM,
		AMDCategoryMachineUnavailable, AMDCategoryUncertain:
		return true
	default:
		return false
	}
}

func isAMDMachine(value AMDCategory) bool {
	return value == AMDCategoryMachineIVR || value == AMDCategoryMachineVM || value == AMDCategoryMachineUnavailable
}
