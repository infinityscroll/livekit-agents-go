// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/inference"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/stt"
	"github.com/livekit/agents-go/tts"
	"github.com/livekit/agents-go/vad"
)

const (
	DefaultMaxToolSteps              = 3
	DefaultUserAwayTimeout           = 15 * time.Second
	DefaultTTSReadIdleTimeout        = 10 * time.Second
	DefaultForwardAudioIdleTimeout   = 10 * time.Second
	DefaultSessionShutdownTimeout    = 5 * time.Second
	DefaultSpeechQueueCapacity       = 64
	DefaultGenerationQueueCapacity   = 16
	DefaultGenerationConcurrency     = 2
	DefaultHandoffQueueCapacity      = 4
	DefaultRecognitionQueueCapacity  = 32
	DefaultSessionEventQueueCapacity = DefaultEventQueueCapacity
)

var (
	ErrSessionNotStarted     = errors.New("voice agent session is not started")
	ErrSessionClosing        = errors.New("voice agent session is closing")
	ErrSessionClosed         = errors.New("voice agent session is closed")
	ErrSessionAlreadyStarted = errors.New("voice agent session is already started")
	ErrNoSpeech              = errors.New("voice agent session has no speech to interrupt")
)

// AgentSessionOptions configures one reusable model set and its bounded voice
// pipeline. Model implementations are caller-owned and are never closed by the
// session; activity streams created from them are always closed.
type AgentSessionOptions[UserData any] struct {
	STT stt.STT
	VAD vad.VAD
	// VADSelection is the explicit inherit/use/disable form of VAD. Its zero
	// value auto-provisions a lazy inference.VAD when VAD is nil. Use
	// agents.Disable[vad.VAD]() to opt out. VAD and a non-inherited selection
	// are mutually exclusive.
	VADSelection agents.Override[vad.VAD]
	LLM          llm.LLM
	// Realtime is mutually exclusive with LLM. Agent-level tri-state
	// overrides may select a different Chat or realtime model per handoff.
	Realtime llm.RealtimeModel
	TTS      tts.TTS
	// Model-string alternatives mirror the TypeScript/Python inference
	// shorthand (for example "openai/gpt-4.1-mini"). A concrete model and its
	// model string are mutually exclusive. Models resolved here are session-owned.
	STTModel string
	LLMModel string
	TTSModel string

	UserData UserData
	Tools    *llm.Context

	ConnectOptions agents.SessionConnectOptions
	TurnHandling   *TurnHandlingOptions
	Keyterms       KeytermsOptions
	ToolHandling   ToolHandlingOptions
	Expressive     agents.Override[ExpressiveOptions]

	MaxToolSteps            int
	UserAwayTimeout         *time.Duration
	DisableUserAwayTimeout  bool
	TranscriptionTimeout    *time.Duration
	TTSReadIdleTimeout      time.Duration
	ForwardAudioIdleTimeout time.Duration
	ShutdownTimeout         time.Duration
	UseTTSAlignedTranscript *bool

	SpeechQueueCapacity      int
	GenerationQueueCapacity  int
	GenerationConcurrency    int
	RecognitionQueueCapacity int
	EventQueueCapacity       int

	// ParentContext owns the session lifetime. A nil context uses Background;
	// Start's context only bounds start-up and never accidentally owns a running
	// session after Start returns.
	ParentContext context.Context
}

type CloseOptions struct {
	Reason CloseReason
	Err    error
	// Drain lets already accepted speech finish. The default interrupts all
	// speech before teardown.
	Drain bool
}

// WaitForIdleOptions controls which side of the conversation must be idle.
// Nil fields default to true, matching the Python/TypeScript SDKs.
type WaitForIdleOptions struct {
	WaitForAgent *bool
	WaitForUser  *bool
}

type resolvedSessionOptions struct {
	connect                     agents.SessionConnectOptions
	turn                        TurnHandlingOptions
	toolHandling                ToolHandlingOptions
	expressive                  agents.Override[ExpressiveOptions]
	maxToolSteps                int
	userAwayTimeout             time.Duration
	userAwayEnabled             bool
	transcriptionTimeout        time.Duration
	transcriptionTimeoutEnabled bool
	ttsReadIdleTimeout          time.Duration
	forwardAudioIdleTimeout     time.Duration
	shutdownTimeout             time.Duration
	useTTSAlignedTranscript     bool
	speechQueueCapacity         int
	generationQueueCapacity     int
	generationConcurrency       int
	recognitionQueueCapacity    int
}

// AgentSession is the context-first Go equivalent of the Python/TypeScript
// AgentSession. It owns activities, streams, queues and events, but not model
// instances supplied in AgentSessionOptions.
type AgentSession[UserData any] struct {
	ctx         context.Context
	lifetimeCtx context.Context
	cancel      context.CancelCauseFunc
	opts        resolvedSessionOptions

	models      activityModels
	ownedModels []sessionOwnedModel
	tools       *llm.Context
	data        UserData

	input                     *AgentInput
	output                    *AgentOutput
	events                    *EventBus
	usage                     *metrics.ModelUsageCollector
	keyterms                  *KeytermDetector
	keytermMetricsUnsubscribe func()
	toolRegistry              *toolTaskRegistry
	sessionToolExecutors      map[*llm.AsyncToolset]*ToolExecutor

	transition chan struct{}
	handoffs   *stream.Channel[*Agent[UserData]]

	mu               sync.RWMutex
	started          bool
	closing          bool
	closed           bool
	agent            *Agent[UserData]
	activity         *agentActivity[UserData]
	chat             *llm.ChatContext
	userState        UserState
	agentState       AgentState
	llmUnrecoverable int
	ttsUnrecoverable int
	userAwayTimer    *time.Timer
	replyAuthPaused  int
	idleChanged      chan struct{}
	idleVersion      uint64
	idleHolds        int
	activityChanging bool

	runMu sync.Mutex
	run   activeSessionRun

	background sync.WaitGroup

	realtimeMu      sync.Mutex
	realtimeModel   llm.RealtimeModel
	realtimeSession llm.RealtimeSession
	realtimeCancel  context.CancelCauseFunc
	// Instructions are not exposed by RealtimeSession, so retain the last value
	// applied by the voice runtime for exact handoff-reuse eligibility checks.
	realtimeInstructions string
	closeOnce            sync.Once
	closeDone            chan struct{}
	closeErr             error
}

type sessionOwnedModel struct {
	label string
	close func(context.Context) error
}

func NewAgentSession[UserData any](options AgentSessionOptions[UserData]) (*AgentSession[UserData], error) {
	for _, model := range []any{options.STT, options.VAD, options.LLM, options.Realtime, options.TTS} {
		if isTypedNil(model) {
			return nil, errors.New("voice session model must not contain a typed nil")
		}
	}
	if selected, ok := options.VADSelection.Value(); ok && isTypedNil(selected) {
		return nil, errors.New("voice session VAD selection must not contain a typed nil; use agents.Disable instead")
	}
	turnOwned, err := provisionDefaultTurnDetector(&options)
	if err != nil {
		return nil, err
	}
	models, owned, err := resolveSessionModels(&options)
	if err != nil {
		closeOwnedModelsBestEffort(turnOwned)
		return nil, err
	}
	owned = append(owned, turnOwned...)
	resolved, err := resolveSessionOptions(options)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), DefaultSessionShutdownTimeout)
		_ = closeOwnedModels(cleanupCtx, owned)
		cancel()
		return nil, err
	}
	keyterms, err := NewKeytermDetector(options.Keyterms)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), DefaultSessionShutdownTimeout)
		_ = closeOwnedModels(cleanupCtx, owned)
		cancel()
		return nil, fmt.Errorf("voice session keyterms: %w", err)
	}
	parent := options.ParentContext
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	tools := options.Tools
	if tools == nil {
		tools = llm.EmptyToolContext()
	} else {
		tools = tools.Copy()
	}
	s := &AgentSession[UserData]{
		ctx: ctx, lifetimeCtx: ctx, cancel: cancel, opts: resolved,
		models: models, ownedModels: owned,
		tools: tools, data: options.UserData, chat: llm.EmptyChatContext(),
		transition: make(chan struct{}, 1), handoffs: stream.NewChannel[*Agent[UserData]](DefaultHandoffQueueCapacity),
		usage: metrics.NewModelUsageCollector(), keyterms: keyterms, closeDone: make(chan struct{}),
		toolRegistry: newToolTaskRegistry(), sessionToolExecutors: make(map[*llm.AsyncToolset]*ToolExecutor),
		idleChanged: make(chan struct{}),
		userState:   UserStateListening, agentState: AgentStateInitializing,
	}
	s.transition <- struct{}{}
	// The event dispatcher outlives session-context cancellation long enough to
	// deliver the terminal Close event. It is explicitly stopped by Close after
	// the PublishAndWait barrier, so this does not add an independent lifetime.
	s.events = NewEventBus(context.Background(), EventBusOptions{QueueCapacity: options.EventQueueCapacity})
	s.keytermMetricsUnsubscribe = keyterms.OnMetrics(func(value metrics.LLM) { s.collectMetric(value) })
	s.input = NewAgentInput(s.audioInputChanged, s.audioInputEnabledChanged)
	s.output = NewAgentOutput(s.audioOutputChanged, s.textOutputChanged)
	if options.LLM != nil {
		if err := guardedActivityCall("LLM prewarm", func() error { options.LLM.Prewarm(ctx); return nil }); err != nil {
			s.cancel(err)
			_ = s.events.Close()
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), resolved.shutdownTimeout)
			_ = closeOwnedModels(cleanupCtx, owned)
			cleanupCancel()
			return nil, fmt.Errorf("prewarm session LLM: %w", err)
		}
	}
	go func() {
		<-ctx.Done()
		_ = s.Close(context.Background(), CloseOptions{Reason: CloseReasonError, Err: context.Cause(ctx)})
	}()
	return s, nil
}

func resolveSessionModels[UserData any](options *AgentSessionOptions[UserData]) (activityModels, []sessionOwnedModel, error) {
	if options.LLM != nil && options.Realtime != nil {
		return activityModels{}, nil, errors.New("voice session accepts one LLM selection: Chat LLM or RealtimeModel")
	}
	if options.STT != nil && options.STTModel != "" || options.LLM != nil && options.LLMModel != "" || options.TTS != nil && options.TTSModel != "" {
		return activityModels{}, nil, errors.New("voice session accepts either a concrete model or its model string, not both")
	}
	if options.VAD != nil && !options.VADSelection.IsInherited() {
		return activityModels{}, nil, errors.New("voice session accepts either VAD or VADSelection, not both")
	}
	owned := make([]sessionOwnedModel, 0, 4)
	vadDefault := false
	switch {
	case options.VADSelection.IsDisabled():
		options.VAD = nil
	case !options.VADSelection.IsInherited():
		options.VAD, _ = options.VADSelection.Value()
	case options.VAD == nil:
		model, err := inference.NewVAD(inference.VADOptions{})
		if err != nil {
			return activityModels{}, nil, fmt.Errorf("create default inference VAD: %w", err)
		}
		options.VAD, vadDefault = model, true
		owned = append(owned, sessionOwnedModel{label: "default VAD", close: model.Close})
	}
	if options.STTModel != "" {
		model, err := inference.STTFromModelString(options.STTModel)
		if err != nil {
			return activityModels{}, nil, fmt.Errorf("resolve STT model %q: %w", options.STTModel, err)
		}
		options.STT = model
		owned = append(owned, sessionOwnedModel{label: "STT", close: model.Close})
	}
	if options.LLMModel != "" {
		model, err := inference.LLMFromModelString(options.LLMModel)
		if err != nil {
			closeOwnedModelsBestEffort(owned)
			return activityModels{}, nil, fmt.Errorf("resolve LLM model %q: %w", options.LLMModel, err)
		}
		options.LLM = model
		owned = append(owned, sessionOwnedModel{label: "LLM", close: model.Close})
	}
	if options.TTSModel != "" {
		model, err := inference.TTSFromModelString(options.TTSModel)
		if err != nil {
			closeOwnedModelsBestEffort(owned)
			return activityModels{}, nil, fmt.Errorf("resolve TTS model %q: %w", options.TTSModel, err)
		}
		options.TTS = model
		owned = append(owned, sessionOwnedModel{label: "TTS", close: model.Close})
	}
	return activityModels{stt: options.STT, vad: options.VAD, vadDefault: vadDefault, llm: options.LLM, realtime: options.Realtime, tts: options.TTS}, owned, nil
}

func provisionDefaultTurnDetector[UserData any](options *AgentSessionOptions[UserData]) ([]sessionOwnedModel, error) {
	turn := options.TurnHandling
	if turn == nil {
		value := DefaultTurnHandlingOptions(true)
		turn = &value
	} else {
		turn = cloneTurnHandling(turn)
	}
	if !turn.TurnDetection.IsInherited() {
		options.TurnHandling = turn
		return nil, nil
	}
	detectorOptions := inference.TurnDetectorOptions{ConnectOptions: options.ConnectOptions.STT}
	if executor, ok := agents.InferenceExecutorFromContext(options.ParentContext); ok {
		detectorOptions.Executor = executor
	}
	detector, err := inference.NewTurnDetector(detectorOptions)
	if err != nil {
		return nil, fmt.Errorf("create default inference turn detector: %w", err)
	}
	turn.TurnDetection = UseTurnDetector(detector)
	// A streaming detector uses the streaming endpointing defaults unless the
	// caller supplied a non-zero endpointing configuration.
	if turn.Endpointing == (EndpointingOptions{}) {
		turn.Endpointing = StreamingEndpointingOptions
	}
	options.TurnHandling = turn
	return []sessionOwnedModel{{label: "default turn detector", close: detector.Close}}, nil
}

func closeOwnedModelsBestEffort(models []sessionOwnedModel) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultSessionShutdownTimeout)
	_ = closeOwnedModels(ctx, models)
	cancel()
}

func closeOwnedModels(ctx context.Context, models []sessionOwnedModel) []error {
	var errs []error
	for _, model := range models {
		if err := guardedActivityCall("session-owned model close", func() error { return model.close(ctx) }); err != nil {
			errs = append(errs, fmt.Errorf("close session-owned %s model: %w", model.label, err))
		}
	}
	return errs
}

func (s *AgentSession[UserData]) activateRealtime(
	ctx context.Context,
	model llm.RealtimeModel,
	instructions string,
	chat *llm.ChatContext,
	tools *llm.Context,
	reuse bool,
) (llm.RealtimeSession, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.realtimeMu.Lock()
	defer s.realtimeMu.Unlock()

	current, currentModel, currentCancel := s.realtimeSession, s.realtimeModel, s.realtimeCancel
	if model == nil {
		if currentCancel != nil {
			currentCancel(ErrSessionClosed)
		}
		if current != nil {
			if err := current.Close(ctx); err != nil {
				return nil, fmt.Errorf("close replaced realtime session: %w", err)
			}
		}
		s.realtimeSession, s.realtimeModel, s.realtimeCancel, s.realtimeInstructions = nil, nil, nil, ""
		return nil, nil
	}

	canReuse := current != nil && sameInterface(currentModel, model) && reuse
	instructionsChanged, chatChanged, toolsChanged := true, true, true
	if canReuse {
		caps := model.Capabilities()
		instructionsChanged = s.realtimeInstructions != instructions
		currentChat := current.ChatContext()
		chatChanged = !realtimeChatEquivalent(currentChat, chat)
		currentTools := current.Tools()
		toolsChanged = !toolContextsEquivalent(currentTools, tools)
		canReuse = (!instructionsChanged || caps.MidSessionInstructionsUpdate) &&
			(!chatChanged || caps.MidSessionChatContextUpdate) &&
			(!toolsChanged || caps.MidSessionToolsUpdate)
	}
	if canReuse {
		if err := current.Interrupt(ctx); err != nil {
			return nil, fmt.Errorf("interrupt reused realtime session: %w", err)
		}
		if err := current.ClearAudio(ctx); err != nil {
			return nil, fmt.Errorf("clear reused realtime input: %w", err)
		}
		if err := updateRealtimeConfigurationFields(ctx, current, instructions, chat, tools, instructionsChanged, chatChanged, toolsChanged); err != nil {
			return nil, err
		}
		s.realtimeInstructions = instructions
		return current, nil
	}

	// Realtime providers commonly derive all transport goroutines from the
	// Session context. The caller's Start/UpdateAgent context only bounds the
	// transition and is frequently cancelled immediately after it returns; using
	// it here would tear down an otherwise successful long-lived connection.
	created, createdCancel, err := s.openRealtimeSession(ctx, model)
	if err != nil {
		return nil, fmt.Errorf("open realtime session: %w", err)
	}
	if created == nil {
		createdCancel(errors.New("realtime model returned a nil session"))
		return nil, errors.New("realtime model returned a nil session")
	}
	if err := updateRealtimeConfiguration(ctx, created, instructions, chat, tools); err != nil {
		createdCancel(err)
		_ = created.Close(ctx)
		return nil, err
	}
	// Open and configure the replacement before releasing the current session.
	// A failed provider connection therefore leaves the running activity usable
	// and makes UpdateAgent/UpdateOptions transactional from the caller's view.
	if current != nil {
		if currentCancel != nil {
			currentCancel(ErrSessionClosed)
		}
		if err := current.Close(ctx); err != nil {
			createdCancel(err)
			_ = created.Close(ctx)
			return nil, fmt.Errorf("close replaced realtime session: %w", err)
		}
	}
	s.realtimeSession, s.realtimeModel, s.realtimeCancel, s.realtimeInstructions = created, model, createdCancel, instructions
	return created, nil
}

type realtimeOpenResult struct {
	session llm.RealtimeSession
	err     error
}

// openRealtimeSession separates the provider transport lifetime from the
// caller's transition deadline. Successful transports remain children of the
// AgentSession context; a cancelled Start/UpdateAgent still returns promptly
// and tells the opening goroutine to close any late provider result.
func (s *AgentSession[UserData]) openRealtimeSession(ctx context.Context, model llm.RealtimeModel) (llm.RealtimeSession, context.CancelCauseFunc, error) {
	lifetimeCtx, lifetimeCancel := context.WithCancelCause(s.ctx)
	result := make(chan realtimeOpenResult, 1)
	accept := make(chan bool, 1)
	go func() {
		created, err := model.Session(lifetimeCtx)
		result <- realtimeOpenResult{session: created, err: err}
		if <-accept || created == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), s.opts.shutdownTimeout)
		_ = created.Close(cleanupCtx)
		cancel()
	}()
	select {
	case opened := <-result:
		accept <- opened.err == nil
		if opened.err != nil {
			lifetimeCancel(opened.err)
			return nil, nil, opened.err
		}
		return opened.session, lifetimeCancel, nil
	case <-ctx.Done():
		cause := context.Cause(ctx)
		lifetimeCancel(cause)
		accept <- false
		return nil, nil, cause
	case <-s.ctx.Done():
		cause := context.Cause(s.ctx)
		lifetimeCancel(cause)
		accept <- false
		return nil, nil, cause
	}
}

func updateRealtimeConfiguration(ctx context.Context, session llm.RealtimeSession, instructions string, chat *llm.ChatContext, tools *llm.Context) error {
	return updateRealtimeConfigurationFields(ctx, session, instructions, chat, tools, true, true, true)
}

func updateRealtimeConfigurationFields(
	ctx context.Context,
	session llm.RealtimeSession,
	instructions string,
	chat *llm.ChatContext,
	tools *llm.Context,
	updateInstructions bool,
	updateChat bool,
	updateTools bool,
) error {
	var errs []error
	if updateInstructions {
		if err := session.UpdateInstructions(ctx, instructions); err != nil {
			errs = append(errs, fmt.Errorf("update realtime instructions: %w", err))
		}
	}
	if updateChat {
		if err := session.UpdateChatContext(ctx, chat); err != nil {
			errs = append(errs, fmt.Errorf("update realtime chat context: %w", err))
		}
	}
	if updateTools {
		if err := session.UpdateTools(ctx, tools); err != nil {
			errs = append(errs, fmt.Errorf("update realtime tools: %w", err))
		}
	}
	return errors.Join(errs...)
}

func realtimeChatEquivalent(left, right *llm.ChatContext) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	options := llm.CopyOptions{ExcludeInstructions: true, ExcludeHandoff: true, ExcludeConfigUpdate: true}
	return left.Copy(options).IsEquivalent(right.Copy(options))
}

func toolContextsEquivalent(left, right *llm.Context) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftTools, rightTools := left.Flatten(), right.Flatten()
	if len(leftTools) != len(rightTools) {
		return false
	}
	rightByID := make(map[string]llm.Tool, len(rightTools))
	for _, tool := range rightTools {
		rightByID[tool.ID()] = tool
	}
	for _, tool := range leftTools {
		other, ok := rightByID[tool.ID()]
		if !ok || !sameInterface(tool, other) {
			return false
		}
	}
	leftSets, rightSets := left.Toolsets(), right.Toolsets()
	if len(leftSets) != len(rightSets) {
		return false
	}
	rightSet := make(map[*llm.Toolset]struct{}, len(rightSets))
	for _, toolset := range rightSets {
		rightSet[toolset] = struct{}{}
	}
	for _, toolset := range leftSets {
		if _, ok := rightSet[toolset]; !ok {
			return false
		}
	}
	return true
}

func (s *AgentSession[UserData]) noteRealtimeInstructions(session llm.RealtimeSession, instructions string) {
	s.realtimeMu.Lock()
	if sameInterface(s.realtimeSession, session) {
		s.realtimeInstructions = instructions
	}
	s.realtimeMu.Unlock()
}

func (s *AgentSession[UserData]) closeRealtime(ctx context.Context) error {
	s.realtimeMu.Lock()
	defer s.realtimeMu.Unlock()
	if s.realtimeSession == nil {
		return nil
	}
	if s.realtimeCancel != nil {
		s.realtimeCancel(ErrSessionClosed)
	}
	err := s.realtimeSession.Close(ctx)
	s.realtimeSession, s.realtimeModel, s.realtimeCancel, s.realtimeInstructions = nil, nil, nil, ""
	if err != nil {
		return fmt.Errorf("close realtime session: %w", err)
	}
	return nil
}

func isTypedNil(value any) bool {
	if value == nil {
		return false
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return ref.IsNil()
	default:
		return false
	}
}

func resolveSessionOptions[UserData any](options AgentSessionOptions[UserData]) (resolvedSessionOptions, error) {
	result := resolvedSessionOptions{
		connect: options.ConnectOptions.Resolve(), maxToolSteps: options.MaxToolSteps,
		userAwayTimeout:    optionsValue(options.UserAwayTimeout, DefaultUserAwayTimeout),
		userAwayEnabled:    !options.DisableUserAwayTimeout,
		ttsReadIdleTimeout: options.TTSReadIdleTimeout, forwardAudioIdleTimeout: options.ForwardAudioIdleTimeout,
		shutdownTimeout: options.ShutdownTimeout, useTTSAlignedTranscript: true,
		speechQueueCapacity: options.SpeechQueueCapacity, generationQueueCapacity: options.GenerationQueueCapacity,
		generationConcurrency: options.GenerationConcurrency, recognitionQueueCapacity: options.RecognitionQueueCapacity,
		toolHandling: cloneToolHandling(options.ToolHandling), expressive: cloneExpressiveOverride(options.Expressive),
	}
	if result.maxToolSteps == 0 {
		result.maxToolSteps = DefaultMaxToolSteps
	}
	if result.ttsReadIdleTimeout == 0 {
		result.ttsReadIdleTimeout = DefaultTTSReadIdleTimeout
	}
	if result.forwardAudioIdleTimeout == 0 {
		result.forwardAudioIdleTimeout = DefaultForwardAudioIdleTimeout
	}
	if result.shutdownTimeout == 0 {
		result.shutdownTimeout = DefaultSessionShutdownTimeout
	}
	if result.speechQueueCapacity == 0 {
		result.speechQueueCapacity = DefaultSpeechQueueCapacity
	}
	if result.generationQueueCapacity == 0 {
		result.generationQueueCapacity = DefaultGenerationQueueCapacity
	}
	if result.generationConcurrency == 0 {
		result.generationConcurrency = DefaultGenerationConcurrency
	}
	if result.recognitionQueueCapacity == 0 {
		result.recognitionQueueCapacity = DefaultRecognitionQueueCapacity
	}
	if options.UseTTSAlignedTranscript != nil {
		result.useTTSAlignedTranscript = *options.UseTTSAlignedTranscript
	}
	if options.TranscriptionTimeout != nil {
		result.transcriptionTimeout = *options.TranscriptionTimeout
		result.transcriptionTimeoutEnabled = true
	}
	if options.TurnHandling == nil {
		result.turn = DefaultTurnHandlingOptions(false)
	} else {
		result.turn = *cloneTurnHandling(options.TurnHandling)
	}
	if result.maxToolSteps < 1 || result.speechQueueCapacity < 1 || result.generationQueueCapacity < 1 || result.generationConcurrency < 1 || result.recognitionQueueCapacity < 1 {
		return resolvedSessionOptions{}, errors.New("voice session queue, concurrency, and tool-step limits must be positive")
	}
	if result.userAwayTimeout < 0 || result.transcriptionTimeout < 0 || result.ttsReadIdleTimeout < 0 || result.forwardAudioIdleTimeout < 0 || result.shutdownTimeout < 0 {
		return resolvedSessionOptions{}, errors.New("voice session timeouts must not be negative")
	}
	if err := result.turn.Validate(); err != nil {
		return resolvedSessionOptions{}, fmt.Errorf("voice session turn handling: %w", err)
	}
	return result, nil
}

func optionsValue(value *time.Duration, fallback time.Duration) time.Duration {
	if value == nil {
		return fallback
	}
	return *value
}

func (s *AgentSession[UserData]) UserData() *UserData  { return &s.data }
func (s *AgentSession[UserData]) Input() *AgentInput   { return s.input }
func (s *AgentSession[UserData]) Output() *AgentOutput { return s.output }

func (s *AgentSession[UserData]) ChatContext() *llm.ChatContext {
	s.mu.RLock()
	chat := s.chat.Copy(llm.CopyOptions{})
	s.mu.RUnlock()
	return chat
}

func (s *AgentSession[UserData]) Usage() AgentSessionUsage {
	return AgentSessionUsage{ModelUsage: s.usage.Snapshot()}
}

func (s *AgentSession[UserData]) ToolHandling() ToolHandlingOptions {
	return cloneToolHandling(s.opts.toolHandling)
}

// KeytermDetector returns the session-scoped detector. Its confirmed state is
// retained across agent handoffs; callers may update static terms at runtime.
func (s *AgentSession[UserData]) KeytermDetector() *KeytermDetector { return s.keyterms }

func (s *AgentSession[UserData]) Agent() *Agent[UserData] {
	s.mu.RLock()
	agent := s.agent
	s.mu.RUnlock()
	return agent
}

func (s *AgentSession[UserData]) Started() bool {
	s.mu.RLock()
	started := s.started
	s.mu.RUnlock()
	return started
}

func (s *AgentSession[UserData]) Closing() bool {
	s.mu.RLock()
	closing := s.closing
	s.mu.RUnlock()
	return closing
}

func (s *AgentSession[UserData]) UserState() UserState {
	s.mu.RLock()
	state := s.userState
	s.mu.RUnlock()
	return state
}

func (s *AgentSession[UserData]) AgentState() AgentState {
	s.mu.RLock()
	state := s.agentState
	s.mu.RUnlock()
	return state
}

type idleHoldContextKey struct{}

func boolOption(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func (s *AgentSession[UserData]) ownsIdleHold(ctx context.Context) bool {
	return ctx != nil && ctx.Value(idleHoldContextKey{}) == any(s)
}

func (s *AgentSession[UserData]) notifyIdleChangeLocked() {
	s.idleVersion++
	close(s.idleChanged)
	s.idleChanged = make(chan struct{})
}

func (s *AgentSession[UserData]) notifyIdleChange() {
	s.mu.Lock()
	s.notifyIdleChangeLocked()
	s.mu.Unlock()
}

// WaitForIdle waits without polling until the selected sides of the active
// conversation are inactive. Activity handoffs are retried transparently.
func (s *AgentSession[UserData]) WaitForIdle(ctx context.Context, options ...WaitForIdleOptions) (*Agent[UserData], error) {
	if len(options) > 1 {
		return nil, errors.New("WaitForIdle accepts at most one options value")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var option WaitForIdleOptions
	if len(options) == 1 {
		option = options[0]
	}
	waitAgent := boolOption(option.WaitForAgent, true)
	waitUser := boolOption(option.WaitForUser, true)
	for {
		s.mu.RLock()
		if s.closing || s.closed {
			s.mu.RUnlock()
			return nil, ErrSessionClosing
		}
		activity, agent, started := s.activity, s.agent, s.started
		version, changed := s.idleVersion, s.idleChanged
		userIdle := !waitUser || s.userState == UserStateListening
		holdBlocked := s.activityChanging || s.idleHolds > 0 && !s.ownsIdleHold(ctx)
		s.mu.RUnlock()
		if !started || activity == nil || agent == nil {
			return nil, ErrSessionNotStarted
		}
		activityIdle := !waitAgent || activity.idle()
		if userIdle && activityIdle && !holdBlocked {
			s.mu.RLock()
			stable := s.activity == activity && s.idleVersion == version && !s.activityChanging && !s.closing && !s.closed &&
				(!waitUser || s.userState == UserStateListening) && (s.idleHolds == 0 || s.ownsIdleHold(ctx))
			s.mu.RUnlock()
			if stable && (!waitAgent || activity.idle()) {
				return agent, nil
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-s.ctx.Done():
			return nil, context.Cause(s.ctx)
		case <-changed:
		}
	}
}

// WaitForIdleAndHold runs fn while other idle waiters are held behind an
// event-driven barrier. The derived callback context makes nested holds on the
// same session reentrant.
func (s *AgentSession[UserData]) WaitForIdleAndHold(ctx context.Context, fn func(context.Context, *Agent[UserData]) error) error {
	_, err := WaitForIdleAndHoldValue(ctx, s, func(holdCtx context.Context, agent *Agent[UserData]) (struct{}, error) {
		if fn == nil {
			return struct{}{}, errors.New("WaitForIdleAndHold callback is required")
		}
		return struct{}{}, fn(holdCtx, agent)
	})
	return err
}

// WaitForIdleAndHoldValue is the generic result-bearing form. Go methods cannot
// introduce their own type parameters, so this helper preserves the generic DX
// without reflection or any-typed return values.
func WaitForIdleAndHoldValue[UserData, Value any](ctx context.Context, session *AgentSession[UserData], fn func(context.Context, *Agent[UserData]) (Value, error)) (result Value, err error) {
	if session == nil {
		return result, errors.New("WaitForIdleAndHold session is required")
	}
	if fn == nil {
		return result, errors.New("WaitForIdleAndHold callback is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reentrant := session.ownsIdleHold(ctx)
	var agent *Agent[UserData]
	if reentrant {
		agent = session.Agent()
		if agent == nil {
			return result, ErrSessionNotStarted
		}
	} else {
		agent, err = session.WaitForIdle(ctx)
		if err != nil {
			return result, err
		}
	}
	session.mu.Lock()
	if session.closing || session.closed {
		session.mu.Unlock()
		return result, ErrSessionClosing
	}
	session.idleHolds++
	session.notifyIdleChangeLocked()
	session.mu.Unlock()
	defer func() {
		session.mu.Lock()
		if session.idleHolds > 0 {
			session.idleHolds--
		}
		session.notifyIdleChangeLocked()
		session.mu.Unlock()
	}()
	holdCtx := context.WithValue(ctx, idleHoldContextKey{}, any(session))
	return fn(holdCtx, agent)
}

func (s *AgentSession[UserData]) Subscribe(options EventSubscriptionOptions) (*EventSubscription, error) {
	return s.events.Subscribe(options)
}

func (s *AgentSession[UserData]) OnEvent(fn func(Event), options EventSubscriptionOptions) (func(), error) {
	return s.events.OnEvent(fn, options)
}

func (s *AgentSession[UserData]) Start(ctx context.Context, agent *Agent[UserData]) (resultErr error) {
	if agent == nil {
		return errors.New("voice session start requires an agent")
	}
	if err := s.lockTransition(ctx); err != nil {
		return err
	}
	defer s.unlockTransition()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	if s.started {
		s.mu.Unlock()
		return nil
	}
	if s.closing {
		s.mu.Unlock()
		return ErrSessionClosing
	}
	// Job entrypoints commonly pass their worker-scoped inference executor to
	// Start rather than Session construction. Preserve the established session
	// cancellation tree while making that executor discoverable by lazily opened
	// default VAD/EOT streams. Start's cancellation still only bounds startup.
	if executor, ok := agents.InferenceExecutorFromContext(ctx); ok {
		s.ctx = agents.WithInferenceExecutor(s.ctx, executor)
	}
	s.mu.Unlock()
	if err := s.ensureSessionToolExecutors(); err != nil {
		return err
	}
	rollbackConsole, err := acquireDefaultConsoleIO(ctx, s)
	if err != nil {
		return err
	}
	defer func() {
		if resultErr != nil && rollbackConsole != nil {
			resultErr = errors.Join(resultErr, rollbackConsole())
		}
	}()

	for _, toolset := range s.tools.Toolsets() {
		if err := guardedActivityCall("session toolset setup", func() error { return toolset.Setup(ctx, s.tools) }); err != nil {
			return fmt.Errorf("setup session toolset %q: %w", toolset.ID(), err)
		}
	}
	activity, err := newAgentActivity(s, agent)
	if err != nil {
		return err
	}
	if err := activity.start(ctx, true); err != nil {
		_ = s.closeActivityBestEffort(activity)
		return err
	}

	s.mu.Lock()
	s.agent, s.activity, s.started = agent, activity, true
	s.chat = s.chat.Merge(agent.ChatContext(), llm.CopyOptions{})
	s.notifyIdleChangeLocked()
	s.mu.Unlock()
	s.commitHandoffItem(nil, agent)
	s.setAgentState(AgentStateListening)
	s.startBackground()
	activity.enter()
	return nil
}

func (s *AgentSession[UserData]) startBackground() {
	s.background.Add(1)
	go func() {
		defer s.background.Done()
		for {
			agent, err := s.handoffs.Recv(s.ctx)
			if err != nil {
				return
			}
			transitionCtx, cancel := context.WithTimeout(s.ctx, s.opts.shutdownTimeout)
			err = s.UpdateAgent(transitionCtx, agent)
			cancel()
			if err != nil && context.Cause(s.ctx) == nil {
				s.emitError(err, agent)
			}
		}
	}()
}

func (s *AgentSession[UserData]) UpdateAgent(ctx context.Context, agent *Agent[UserData]) error {
	if agent == nil {
		return errors.New("voice session update requires an agent")
	}
	s.mu.RLock()
	old := s.activity
	started, closing := s.started, s.closing
	s.mu.RUnlock()
	if !started {
		return ErrSessionNotStarted
	}
	if closing {
		return ErrSessionClosing
	}
	if old != nil {
		old.blockNewTurns()
	}
	if err := s.lockTransition(ctx); err != nil {
		if old != nil {
			old.allowNewTurns()
		}
		return err
	}
	defer s.unlockTransition()

	s.mu.Lock()
	old = s.activity
	if s.closing {
		s.mu.Unlock()
		return ErrSessionClosing
	}
	if s.agent == agent {
		s.mu.Unlock()
		if old != nil {
			old.allowNewTurns()
		}
		return nil
	}
	s.activityChanging = true
	s.notifyIdleChangeLocked()
	s.mu.Unlock()
	transitionCommitted := false
	defer func() {
		if transitionCommitted {
			return
		}
		s.mu.Lock()
		s.activityChanging = false
		s.notifyIdleChangeLocked()
		s.mu.Unlock()
	}()

	next, err := newAgentActivity(s, agent)
	if err != nil {
		if old != nil {
			old.allowNewTurns()
		}
		return err
	}
	if err := next.start(ctx, false); err != nil {
		_ = s.closeActivityBestEffort(next)
		if old != nil {
			old.allowNewTurns()
		}
		return err
	}

	if old != nil {
		if err := old.drain(ctx); err != nil {
			_ = s.closeActivityBestEffort(next)
			old.allowNewTurns()
			return fmt.Errorf("drain previous agent activity: %w", err)
		}
	}
	if err := next.activateRealtime(ctx, true); err != nil {
		_ = s.closeActivityBestEffort(next)
		if old != nil {
			old.allowNewTurns()
		}
		return err
	}
	if old != nil {
		if err := old.close(ctx); err != nil {
			// Resource teardown is complete before OnExit/toolset errors are returned.
			// The realtime replacement has already committed, so finish the handoff
			// and surface cleanup failure without leaving a closed old activity active.
			s.emitError(fmt.Errorf("close previous agent activity: %w", err), old.agent)
		}
	}
	if err := next.activateKeyterms(); err != nil {
		// Keyterm extraction is auxiliary to the media transition. Surface a
		// failed binding without rolling back a realtime/model handoff whose old
		// activity has already completed.
		s.emitError(fmt.Errorf("activate handoff keyterms: %w", err), agent)
	}

	s.mu.Lock()
	oldAgent := s.agent
	s.agent, s.activity = agent, next
	s.chat = s.chat.Merge(agent.ChatContext(), llm.CopyOptions{})
	s.activityChanging = false
	s.notifyIdleChangeLocked()
	transitionCommitted = true
	s.mu.Unlock()
	s.commitHandoffItem(oldAgent, agent)
	s.setAgentState(AgentStateListening)
	if s.input.AudioEnabled() {
		next.attachAudioInput(s.input.Audio())
	}
	next.enter()
	return nil
}

func (s *AgentSession[UserData]) closeActivityBestEffort(activity *agentActivity[UserData]) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), s.opts.shutdownTimeout)
	defer cancel()
	return activity.close(cleanupCtx)
}

func (s *AgentSession[UserData]) scheduleHandoff(ctx context.Context, agent *Agent[UserData]) error {
	if agent == nil {
		return errors.New("tool handoff returned a nil agent")
	}
	return s.handoffs.Send(ctx, agent)
}

func (s *AgentSession[UserData]) lockTransition(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.lifetimeCtx.Done():
		return context.Cause(s.lifetimeCtx)
	case <-s.transition:
		return nil
	}
}

func (s *AgentSession[UserData]) unlockTransition() { s.transition <- struct{}{} }

func (s *AgentSession[UserData]) Say(ctx context.Context, text string, options SayOptions) (*SpeechHandle, error) {
	activity, err := s.currentActivity()
	if err != nil {
		return nil, err
	}
	handle, err := activity.say(ctx, stream.FromSlice([]string{text}), options)
	if err == nil {
		s.watchActiveRun(handle)
	}
	return handle, err
}

func (s *AgentSession[UserData]) SayStream(ctx context.Context, text stream.Reader[string], options SayOptions) (*SpeechHandle, error) {
	if text == nil {
		return nil, errors.New("voice say stream must not be nil")
	}
	activity, err := s.currentActivity()
	if err != nil {
		return nil, err
	}
	handle, err := activity.say(ctx, text, options)
	if err == nil {
		s.watchActiveRun(handle)
	}
	return handle, err
}

func (s *AgentSession[UserData]) GenerateReply(ctx context.Context, options GenerateReplyOptions) (*SpeechHandle, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	activity, err := s.currentActivity()
	if err != nil {
		return nil, err
	}
	handle, err := activity.generateReply(ctx, options, true, true, false)
	if err == nil {
		s.watchActiveRun(handle)
	}
	return handle, err
}

func (s *AgentSession[UserData]) Interrupt(ctx context.Context, force bool) error {
	activity, err := s.currentActivity()
	if err != nil {
		return err
	}
	return activity.interrupt(ctx, force)
}

func (s *AgentSession[UserData]) CommitUserTurn(ctx context.Context) error {
	activity, err := s.currentActivity()
	if err != nil {
		return err
	}
	activity.mu.RLock()
	recognition := activity.recognition
	activity.mu.RUnlock()
	if recognition != nil {
		return recognition.Commit(ctx)
	}
	if realtime := activity.realtimeSnapshot(); realtime != nil {
		if err := realtime.CommitAudio(ctx); err != nil {
			return err
		}
		_, err := activity.generateReply(ctx, GenerateReplyOptions{InputModality: InputModalityAudio}, true, true, false)
		return err
	}
	return errors.New("voice session has no active audio recognition")
}

func (s *AgentSession[UserData]) ClearUserTurn(ctx context.Context) error {
	activity, err := s.currentActivity()
	if err != nil {
		return err
	}
	activity.cancelPreemptiveReply()
	activity.mu.RLock()
	recognition := activity.recognition
	activity.mu.RUnlock()
	var errs []error
	if recognition != nil {
		errs = append(errs, recognition.Clear(ctx))
	}
	if realtime := activity.realtimeSnapshot(); realtime != nil {
		errs = append(errs, realtime.ClearAudio(ctx))
	}
	if recognition == nil && activity.realtimeSnapshot() == nil {
		return errors.New("voice session has no active audio recognition")
	}
	return errors.Join(errs...)
}

func (s *AgentSession[UserData]) PauseReplyAuthorization() error {
	activity, err := s.currentActivity()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.replyAuthPaused++
	s.mu.Unlock()
	activity.pauseReplyAuthorization()
	return nil
}

func (s *AgentSession[UserData]) ResumeReplyAuthorization() error {
	activity, err := s.currentActivity()
	if err != nil {
		return err
	}
	s.mu.Lock()
	resumed := false
	if s.replyAuthPaused > 0 {
		s.replyAuthPaused--
		resumed = true
	}
	s.mu.Unlock()
	if resumed {
		activity.resumeReplyAuthorization()
	}
	return nil
}

func (s *AgentSession[UserData]) currentActivity() (*agentActivity[UserData], error) {
	s.mu.RLock()
	activity, started, closing := s.activity, s.started, s.closing
	s.mu.RUnlock()
	if closing {
		return nil, ErrSessionClosing
	}
	if !started || activity == nil {
		return nil, ErrSessionNotStarted
	}
	return activity, nil
}

// Drain stops accepting new work and waits for all already accepted speech.
// It is idempotent; call Close afterwards to release streams and subscriptions.
func (s *AgentSession[UserData]) Drain(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	s.closing = true
	s.notifyIdleChangeLocked()
	activity := s.activity
	s.stopUserAwayTimerLocked()
	s.mu.Unlock()
	if activity == nil {
		return nil
	}
	return activity.drain(ctx)
}

func (s *AgentSession[UserData]) Close(ctx context.Context, options ...CloseOptions) error {
	var option CloseOptions
	if len(options) > 1 {
		return errors.New("voice session close accepts at most one CloseOptions value")
	}
	if len(options) == 1 {
		option = options[0]
	}
	if option.Reason == "" {
		option.Reason = CloseReasonUserInitiated
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		s.notifyIdleChangeLocked()
		s.stopUserAwayTimerLocked()
		s.mu.Unlock()
		go s.closeInBackground(option)
	})
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.closeDone:
		s.mu.RLock()
		err := s.closeErr
		s.mu.RUnlock()
		return err
	}
}

func (s *AgentSession[UserData]) closeInBackground(options CloseOptions) {
	defer close(s.closeDone)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), s.opts.shutdownTimeout)
	defer shutdownCancel()

	s.mu.RLock()
	activity := s.activity
	s.mu.RUnlock()
	var errs []error
	if activity != nil {
		if !options.Drain {
			if err := activity.interrupt(shutdownCtx, true); err != nil && !errors.Is(err, ErrNoSpeech) {
				errs = append(errs, err)
			}
		}
		if err := activity.drain(shutdownCtx); err != nil {
			errs = append(errs, err)
		}
		if err := activity.close(shutdownCtx); err != nil {
			errs = append(errs, err)
		}
	}
	s.mu.RLock()
	sessionExecutors := make([]*ToolExecutor, 0, len(s.sessionToolExecutors))
	for _, executor := range s.sessionToolExecutors {
		sessionExecutors = append(sessionExecutors, executor)
	}
	s.mu.RUnlock()
	for _, executor := range sessionExecutors {
		if err := executor.Close(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("close session async tool executor: %w", err))
		}
	}

	for _, toolset := range s.tools.Toolsets() {
		if err := guardedActivityCall("session toolset close", func() error { return toolset.Close(shutdownCtx) }); err != nil {
			errs = append(errs, fmt.Errorf("close session toolset %q: %w", toolset.ID(), err))
		}
	}
	if err := s.closeRealtime(shutdownCtx); err != nil {
		errs = append(errs, err)
	}
	if s.keytermMetricsUnsubscribe != nil {
		s.keytermMetricsUnsubscribe()
		s.keytermMetricsUnsubscribe = nil
	}
	if s.keyterms != nil {
		if err := s.keyterms.Close(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("close keyterm detector: %w", err))
		}
	}
	errs = append(errs, closeOwnedModels(shutdownCtx, s.ownedModels)...)
	s.input.SetAudio(nil)
	s.output.SetAudio(nil)
	s.output.SetTranscription(nil)

	if options.Err != nil {
		errs = append(errs, options.Err)
	}
	closeErr := errors.Join(errs...)
	if err := s.events.PublishAndWait(shutdownCtx, NewCloseEvent(options.Reason, closeErr, time.Now())); err != nil {
		errs = append(errs, fmt.Errorf("dispatch close event: %w", err))
	}
	s.cancel(ErrSessionClosed)
	_ = s.handoffs.Close()
	done := make(chan struct{})
	go func() { s.background.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		errs = append(errs, context.Cause(shutdownCtx))
	}
	if err := s.events.Close(); err != nil {
		errs = append(errs, err)
	}
	s.mu.Lock()
	s.started, s.closing, s.closed = false, false, true
	s.activity, s.agent = nil, nil
	s.userState, s.agentState = UserStateListening, AgentStateInitializing
	s.closeErr = errors.Join(errs...)
	s.notifyIdleChangeLocked()
	s.mu.Unlock()
}

func (s *AgentSession[UserData]) commitHandoffItem(oldAgent, newAgent *Agent[UserData]) {
	oldID := ""
	if oldAgent != nil {
		oldID = oldAgent.ID()
	}
	item := &llm.AgentHandoffItem{ID: agents.ShortUUID("item_"), OldAgentID: oldID, NewAgentID: newAgent.ID(), CreatedAt: time.Now()}
	s.commitItems(nil, item)
}

func (s *AgentSession[UserData]) commitItems(handle *SpeechHandle, items ...llm.ChatItem) {
	filtered := make([]llm.ChatItem, 0, len(items))
	for _, item := range items {
		if item != nil {
			filtered = append(filtered, item)
		}
	}
	if len(filtered) == 0 {
		return
	}
	for _, item := range filtered {
		s.mu.Lock()
		chat := s.chat
		if err := chat.Insert(item); err != nil {
			s.mu.Unlock()
			if _, exists := chat.GetByID(item.ItemID()); !exists {
				s.emitError(err, item)
			}
			continue
		}
		s.mu.Unlock()
		if handle != nil {
			handle.AddChatItems(item)
		}
		_ = s.events.Publish(s.ctx, ConversationItemAddedEvent{EventBase: newEventBase(EventConversationItemAdded, time.Now()), Item: item})
	}
}

func (s *AgentSession[UserData]) replaceChat(chat *llm.ChatContext) error {
	if chat == nil {
		return errors.New("voice session chat context must not be nil")
	}
	s.mu.Lock()
	if s.closing || s.closed {
		s.mu.Unlock()
		return ErrSessionClosing
	}
	s.chat = chat.Copy(llm.CopyOptions{})
	s.mu.Unlock()
	return nil
}

func (s *AgentSession[UserData]) collectMetric(metric metrics.Metric) {
	if metric == nil {
		return
	}
	s.usage.Collect(metric)
	s.events.TryPublish(MetricsCollectedEvent{EventBase: newEventBase(EventMetricsCollected, metric.MetricTime()), Metrics: metric})
	s.events.TryPublish(SessionUsageUpdatedEvent{EventBase: newEventBase(EventSessionUsageUpdated, time.Now()), Usage: s.Usage()})
}

func (s *AgentSession[UserData]) emitError(err error, source any) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, stream.ErrClosed) || errors.Is(err, ErrSessionClosed) {
		return
	}
	_ = s.events.Publish(s.ctx, NewErrorEvent(err, source, time.Now()))
}

func (s *AgentSession[UserData]) setUserState(state UserState) {
	s.mu.Lock()
	old := s.userState
	if old == state || s.closed {
		s.mu.Unlock()
		return
	}
	s.userState = state
	s.notifyIdleChangeLocked()
	if state == UserStateListening {
		s.resetUserAwayTimerLocked()
	} else {
		s.stopUserAwayTimerLocked()
	}
	s.mu.Unlock()
	_ = s.events.Publish(s.ctx, NewUserStateChangedEvent(old, state, time.Now()))
}

func (s *AgentSession[UserData]) setAgentState(state AgentState) {
	at := time.Now()
	s.mu.Lock()
	old := s.agentState
	if old == state || s.closed {
		s.mu.Unlock()
		return
	}
	s.agentState = state
	s.notifyIdleChangeLocked()
	if state == AgentStateListening || state == AgentStateIdle {
		s.resetUserAwayTimerLocked()
	} else {
		s.stopUserAwayTimerLocked()
	}
	activity := s.activity
	s.mu.Unlock()
	if activity != nil {
		activity.agentSpeechStateChanged(old, state, at)
	}
	_ = s.events.Publish(s.ctx, NewAgentStateChangedEvent(old, state, at))
}

func (s *AgentSession[UserData]) resetUserAwayTimerLocked() {
	if !s.opts.userAwayEnabled || s.opts.userAwayTimeout <= 0 || !s.started || s.userState != UserStateListening || (s.agentState != AgentStateListening && s.agentState != AgentStateIdle) {
		return
	}
	if s.userAwayTimer == nil {
		s.userAwayTimer = time.AfterFunc(s.opts.userAwayTimeout, func() { s.setUserState(UserStateAway) })
	} else {
		s.userAwayTimer.Reset(s.opts.userAwayTimeout)
	}
}

func (s *AgentSession[UserData]) stopUserAwayTimerLocked() {
	if s.userAwayTimer != nil {
		s.userAwayTimer.Stop()
		s.userAwayTimer = nil
	}
}

func (s *AgentSession[UserData]) audioInputChanged() {
	s.mu.RLock()
	activity := s.activity
	s.mu.RUnlock()
	if activity != nil {
		activity.attachAudioInput(s.input.Audio())
	}
}

func (s *AgentSession[UserData]) audioInputEnabledChanged(enabled bool) {
	s.mu.RLock()
	activity := s.activity
	s.mu.RUnlock()
	if activity == nil {
		return
	}
	if enabled {
		activity.attachAudioInput(s.input.Audio())
	} else {
		activity.attachAudioInput(nil)
	}
}

func (s *AgentSession[UserData]) audioOutputChanged() {}
func (s *AgentSession[UserData]) textOutputChanged()  {}
