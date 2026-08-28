// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
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

type activityModels struct {
	stt        stt.STT
	vad        vad.VAD
	vadDefault bool
	llm        llm.LLM
	realtime   llm.RealtimeModel
	tts        tts.TTS
}

type ActivityPanicError struct {
	Operation string
	Value     any
	Stack     []byte
}

func (e *ActivityPanicError) Error() string {
	return fmt.Sprintf("voice activity %s panicked: %v", e.Operation, e.Value)
}

func guardedActivityCall(operation string, fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &ActivityPanicError{Operation: operation, Value: recovered, Stack: debug.Stack()}
		}
	}()
	return fn()
}

type speechJobKind uint8

const (
	speechJobSay speechJobKind = iota
	speechJobReply
)

type speechJob struct {
	kind   speechJobKind
	source SpeechSource
	handle *SpeechHandle

	text          stream.Reader[string]
	audio         stream.Reader[agents.AudioFrame]
	say           SayOptions
	reply         GenerateReplyOptions
	realtimeEvent *llm.GenerationCreatedEvent
	segments      *stream.Channel[*generationSegment]
	preemptive    bool
	userCommitted bool

	ready  chan struct{}
	result generationResult
	prepMu sync.Mutex

	finishOnce sync.Once
}

func (j *speechJob) setResult(result generationResult) {
	j.prepMu.Lock()
	j.result = result
	j.prepMu.Unlock()
	close(j.ready)
}

func (j *speechJob) getResult() generationResult {
	j.prepMu.Lock()
	result := j.result
	j.prepMu.Unlock()
	return result
}

type agentActivity[UserData any] struct {
	session *AgentSession[UserData]
	agent   *Agent[UserData]
	ctx     context.Context
	cancel  context.CancelCauseFunc

	mu                sync.RWMutex
	models            activityModels
	tools             *llm.Context
	instructions      llm.Instructions
	turn              TurnHandlingOptions
	endpointing       Endpointing
	useAligned        bool
	toolHandling      ToolHandlingOptions
	paused            bool
	closed            bool
	authPaused        int
	current           *SpeechHandle
	jobs              map[string]*speechJob
	recognition       *AudioRecognition
	realtime          llm.RealtimeSession
	realtimeSubs      []func()
	realtimeAudio     bool
	realtimeSeen      map[string]struct{}
	preemptive        *speechJob
	preemptText       string
	preemptTries      int
	adaptiveDetector  *inference.AdaptiveInterruptionDetector
	adaptiveOwned     bool
	adaptiveAttempted bool

	speechQueue    *SpeechQueue
	generation     *stream.Channel[*speechJob]
	toolExecutor   *ToolExecutor
	asyncExecutors map[*llm.AsyncToolset]*ToolExecutor
	executorByTool map[string]*ToolExecutor
	jobsWG         sync.WaitGroup
	workersWG      sync.WaitGroup
	hooksWG        sync.WaitGroup
	attachWG       sync.WaitGroup
	updatesWG      sync.WaitGroup
	closeOnce      sync.Once
	shutdownErr    error
	unsubscribers  []func()
	subscriptionMu sync.Mutex
	recognitionMu  sync.Mutex
	updateMu       sync.Mutex
	ownedToolsets  []*llm.Toolset
	keytermsActive bool
}

func newAgentActivity[UserData any](session *AgentSession[UserData], agent *Agent[UserData]) (*agentActivity[UserData], error) {
	models := resolveActivityModels(session.models, agent)
	turn := session.opts.turn
	if override := agent.TurnHandling(); override != nil {
		turn = *override
		if turn.TurnDetection.IsInherited() {
			turn.TurnDetection = session.opts.turn.TurnDetection
		}
	}
	if err := turn.Validate(); err != nil {
		return nil, fmt.Errorf("agent %q turn handling: %w", agent.ID(), err)
	}
	endpointing, err := NewEndpointing(turn.Endpointing)
	if err != nil {
		return nil, err
	}
	agentTools := agent.ToolContext()
	tools, err := mergeToolContexts(session.tools, agentTools)
	if err != nil {
		return nil, fmt.Errorf("agent %q tools: %w", agent.ID(), err)
	}
	tools, err = augmentTaskManagementTools(tools)
	if err != nil {
		return nil, fmt.Errorf("agent %q task-management tools: %w", agent.ID(), err)
	}
	toolHandling := session.opts.toolHandling
	agent.mu.RLock()
	if agent.toolHandling.Async != nil {
		toolHandling = cloneToolHandling(agent.toolHandling)
	}
	agent.mu.RUnlock()
	ctx, cancel := context.WithCancelCause(session.ctx)
	executor, err := session.newToolExecutor(ctx, toolHandling.Async)
	if err != nil {
		cancel(err)
		return nil, err
	}
	asyncExecutors, created, err := createAgentAsyncExecutors(session, ctx, agentTools, nil, toolHandling.Async)
	if err != nil {
		_ = executor.Close(context.Background())
		cancel(err)
		return nil, err
	}
	executorByTool, err := buildToolExecutorMap(session, agentTools, asyncExecutors)
	if err != nil {
		for _, createdExecutor := range created {
			_ = createdExecutor.Close(context.Background())
		}
		_ = executor.Close(context.Background())
		cancel(err)
		return nil, err
	}
	useAligned := session.opts.useTTSAlignedTranscript
	if override := agent.UseTTSAlignedTranscript(); override != nil {
		useAligned = *override
	}
	session.mu.RLock()
	authPaused := session.replyAuthPaused
	session.mu.RUnlock()
	return &agentActivity[UserData]{
		session: session, agent: agent, ctx: ctx, cancel: cancel,
		models: models, tools: tools, instructions: agent.Instructions(), turn: turn, endpointing: endpointing,
		useAligned: useAligned, toolHandling: toolHandling, authPaused: authPaused, speechQueue: NewSpeechQueue(session.opts.speechQueueCapacity),
		generation: stream.NewChannel[*speechJob](session.opts.generationQueueCapacity), toolExecutor: executor,
		asyncExecutors: asyncExecutors, executorByTool: executorByTool,
		jobs: make(map[string]*speechJob), realtimeSeen: make(map[string]struct{}), ownedToolsets: agentTools.Toolsets(),
	}, nil
}

func resolveActivityModels[UserData any](base activityModels, agent *Agent[UserData]) activityModels {
	result := base
	result.stt, _ = agent.STTOverride().Resolve(base.stt, base.stt != nil)
	vadOverride := agent.VADOverride()
	result.vad, _ = vadOverride.Resolve(base.vad, base.vad != nil)
	if !vadOverride.IsInherited() {
		result.vadDefault = false
	}
	llmOverride, realtimeOverride := agent.LLMOverride(), agent.RealtimeOverride()
	switch {
	case !realtimeOverride.IsInherited():
		result.realtime, _ = realtimeOverride.Resolve(base.realtime, base.realtime != nil)
		result.llm = nil
	case !llmOverride.IsInherited():
		result.llm, _ = llmOverride.Resolve(base.llm, base.llm != nil)
		result.realtime = nil
	}
	result.tts, _ = agent.TTSOverride().Resolve(base.tts, base.tts != nil)
	return result
}

func mergeToolContexts(contexts ...*llm.Context) (*llm.Context, error) {
	entries := make([]any, 0)
	for _, tools := range contexts {
		if tools == nil {
			continue
		}
		for _, tool := range tools.Flatten() {
			entries = append(entries, tool)
		}
		for _, toolset := range tools.Toolsets() {
			entries = append(entries, toolset)
		}
	}
	return llm.NewToolContext(entries...)
}

func createAgentAsyncExecutors[UserData any](
	session *AgentSession[UserData],
	parent context.Context,
	tools *llm.Context,
	existing map[*llm.AsyncToolset]*ToolExecutor,
	fallback *AsyncToolOptions,
) (map[*llm.AsyncToolset]*ToolExecutor, []*ToolExecutor, error) {
	result := make(map[*llm.AsyncToolset]*ToolExecutor)
	created := make([]*ToolExecutor, 0)
	if tools == nil {
		return result, created, nil
	}
	for _, toolset := range tools.Toolsets() {
		async, ok := toolset.Async()
		if !ok {
			continue
		}
		if executor := existing[async]; executor != nil {
			result[async] = executor
			continue
		}
		options := async.ToolHandling()
		if options == nil && fallback != nil {
			copy := *fallback
			options = &copy
		}
		executor, err := session.newToolExecutor(parent, options)
		if err != nil {
			for _, prior := range created {
				_ = prior.Close(context.Background())
			}
			return nil, nil, fmt.Errorf("create agent async toolset %q executor: %w", toolset.ID(), err)
		}
		result[async] = executor
		created = append(created, executor)
	}
	return result, created, nil
}

func buildToolExecutorMap[UserData any](
	session *AgentSession[UserData],
	agentTools *llm.Context,
	agentExecutors map[*llm.AsyncToolset]*ToolExecutor,
) (map[string]*ToolExecutor, error) {
	result := make(map[string]*ToolExecutor)
	session.mu.RLock()
	for _, toolset := range session.tools.Toolsets() {
		async, ok := toolset.Async()
		if !ok {
			continue
		}
		executor := session.sessionToolExecutors[async]
		if executor == nil {
			session.mu.RUnlock()
			return nil, fmt.Errorf("session async toolset %q has no executor", toolset.ID())
		}
		for _, tool := range toolset.Tools() {
			result[tool.ID()] = executor
		}
	}
	session.mu.RUnlock()
	if agentTools == nil {
		return result, nil
	}
	for _, toolset := range agentTools.Toolsets() {
		async, ok := toolset.Async()
		if !ok {
			continue
		}
		executor := agentExecutors[async]
		if executor == nil {
			return nil, fmt.Errorf("agent async toolset %q has no executor", toolset.ID())
		}
		for _, tool := range toolset.Tools() {
			result[tool.ID()] = executor
		}
	}
	return result, nil
}

func (a *agentActivity[UserData]) start(ctx context.Context, attachAudio bool) error {
	if err := a.agent.bindRuntime(a); err != nil {
		return err
	}
	for _, toolset := range a.ownedToolsets {
		if err := guardedActivityCall("toolset setup", func() error { return toolset.Setup(ctx, a.tools) }); err != nil {
			a.agent.unbindRuntime(a)
			return fmt.Errorf("setup agent toolset %q: %w", toolset.ID(), err)
		}
	}
	if model := a.modelsSnapshot().llm; model != nil {
		if err := guardedActivityCall("activity LLM prewarm", func() error { model.Prewarm(a.ctx); return nil }); err != nil {
			a.agent.unbindRuntime(a)
			return fmt.Errorf("prewarm agent %q LLM: %w", a.agent.ID(), err)
		}
	}
	if attachAudio {
		if err := a.activateRealtime(ctx, false); err != nil {
			a.agent.unbindRuntime(a)
			return err
		}
	}
	a.configureAdaptiveInterruption(ctx)
	a.subscribeModels()
	if attachAudio {
		if err := a.activateKeyterms(); err != nil {
			a.agent.unbindRuntime(a)
			return fmt.Errorf("activate agent keyterms: %w", err)
		}
	}
	for range a.session.opts.generationConcurrency {
		a.workersWG.Add(1)
		go a.generationWorker()
	}
	a.workersWG.Add(1)
	go a.playoutWorker()
	if attachAudio && a.session.input.AudioEnabled() {
		a.attachAudioInput(a.session.input.Audio())
	}
	return nil
}

func (a *agentActivity[UserData]) enter() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.hooksWG.Add(1)
	a.mu.Unlock()
	go func() {
		defer a.hooksWG.Done()
		if err := guardedActivityCall("onEnter", func() error { return a.agent.runOnEnter(a.ctx) }); err != nil && !IsStopResponse(err) && context.Cause(a.ctx) == nil {
			a.session.emitError(fmt.Errorf("agent %q onEnter: %w", a.agent.ID(), err), a.agent)
		}
	}()
}

func (a *agentActivity[UserData]) Session() AgentSessionAccess[UserData] { return a.session }

func (a *agentActivity[UserData]) UpdateChatContext(ctx context.Context, chat *llm.ChatContext) error {
	if chat == nil {
		return errors.New("voice session chat context must not be nil")
	}
	if realtime := a.realtimeSnapshot(); realtime != nil {
		if !a.modelsSnapshot().realtime.Capabilities().MidSessionChatContextUpdate {
			return errors.New("active realtime model does not support mid-session chat-context updates")
		}
		if err := realtime.UpdateChatContext(ctx, chat.Copy(llm.CopyOptions{})); err != nil {
			return fmt.Errorf("update realtime chat context: %w", err)
		}
	}
	return a.session.replaceChat(chat)
}

func (a *agentActivity[UserData]) UpdateInstructions(ctx context.Context, instructions llm.Instructions) error {
	if realtime := a.realtimeSnapshot(); realtime != nil {
		if !a.modelsSnapshot().realtime.Capabilities().MidSessionInstructionsUpdate {
			return errors.New("active realtime model does not support mid-session instruction updates")
		}
		if err := realtime.UpdateInstructions(ctx, instructions.Value()); err != nil {
			return fmt.Errorf("update realtime instructions: %w", err)
		}
		a.session.noteRealtimeInstructions(realtime, instructions.Value())
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrSessionClosing
	}
	a.instructions = instructions
	a.mu.Unlock()
	return nil
}

func (a *agentActivity[UserData]) UpdateModels(ctx context.Context, update AgentUpdateOptions) error {
	if !a.beginUpdate() {
		return ErrSessionClosing
	}
	defer a.updatesWG.Done()
	a.updateMu.Lock()
	defer a.updateMu.Unlock()
	a.mu.RLock()
	models := a.models
	a.mu.RUnlock()
	if update.STT != nil {
		models.stt, _ = update.STT.Resolve(a.session.models.stt, a.session.models.stt != nil)
	}
	if update.VAD != nil {
		models.vad, _ = update.VAD.Resolve(a.session.models.vad, a.session.models.vad != nil)
		models.vadDefault = update.VAD.IsInherited() && a.session.models.vadDefault
	}
	if update.LLM != nil || update.Realtime != nil {
		llmOverride, realtimeOverride := a.agent.LLMOverride(), a.agent.RealtimeOverride()
		if update.LLM != nil {
			llmOverride = *update.LLM
		}
		if update.Realtime != nil {
			realtimeOverride = *update.Realtime
		}
		switch {
		case !realtimeOverride.IsInherited():
			models.realtime, _ = realtimeOverride.Resolve(a.session.models.realtime, a.session.models.realtime != nil)
			models.llm = nil
		case !llmOverride.IsInherited():
			models.llm, _ = llmOverride.Resolve(a.session.models.llm, a.session.models.llm != nil)
			models.realtime = nil
		default:
			models.llm, models.realtime = a.session.models.llm, a.session.models.realtime
		}
	}
	if update.TTS != nil {
		models.tts, _ = update.TTS.Resolve(a.session.models.tts, a.session.models.tts != nil)
	}
	if (update.LLM != nil || update.Realtime != nil) && models.llm != nil {
		if err := guardedActivityCall("updated LLM prewarm", func() error { models.llm.Prewarm(a.ctx); return nil }); err != nil {
			return fmt.Errorf("prewarm updated agent %q LLM: %w", a.agent.ID(), err)
		}
	}
	a.mu.RLock()
	if a.closed {
		a.mu.RUnlock()
		return ErrSessionClosing
	}
	sttChanged := !sameInterface(models.stt, a.models.stt)
	recognitionChanged := sttChanged || !sameInterface(models.vad, a.models.vad) || !sameInterface(models.realtime, a.models.realtime)
	a.mu.RUnlock()
	// Opening and configuring a realtime replacement can fail. Do that before
	// publishing the candidate model snapshot so generation, recognition, and a
	// subsequent Agent.UpdateOptions continue to observe the old configuration
	// when the provider transition is rejected.
	realtimeActivated := update.LLM != nil || update.Realtime != nil
	if realtimeActivated {
		if err := a.activateRealtimeModels(ctx, models, true); err != nil {
			return err
		}
	}
	if !realtimeActivated {
		a.mu.Lock()
		if a.closed {
			a.mu.Unlock()
			return ErrSessionClosing
		}
		a.models = models
		a.mu.Unlock()
	}
	a.configureAdaptiveInterruption(ctx)
	a.subscribeModels()
	if sttChanged {
		if err := a.restartKeyterms(ctx); err != nil {
			a.session.emitError(fmt.Errorf("update keyterm STT binding: %w", err), models.stt)
		}
	}
	if recognitionChanged {
		a.attachAudioInput(a.session.input.Audio())
	}
	return nil
}

func (a *agentActivity[UserData]) UpdateTools(ctx context.Context, tools *llm.Context) error {
	if !a.beginUpdate() {
		return ErrSessionClosing
	}
	defer a.updatesWG.Done()
	a.updateMu.Lock()
	defer a.updateMu.Unlock()
	merged, err := mergeToolContexts(a.session.tools, tools)
	if err != nil {
		return err
	}
	merged, err = augmentTaskManagementTools(merged)
	if err != nil {
		return err
	}
	owned := tools.Toolsets()
	a.mu.RLock()
	previous := append([]*llm.Toolset(nil), a.ownedToolsets...)
	previousExecutors := make(map[*llm.AsyncToolset]*ToolExecutor, len(a.asyncExecutors))
	for async, executor := range a.asyncExecutors {
		previousExecutors[async] = executor
	}
	fallbackOptions := cloneToolHandling(a.toolHandling).Async
	a.mu.RUnlock()
	previousByID := make(map[string]*llm.Toolset, len(previous))
	for _, toolset := range previous {
		previousByID[toolset.ID()] = toolset
	}
	setUp := make([]*llm.Toolset, 0, len(owned))
	for _, toolset := range owned {
		if prior := previousByID[toolset.ID()]; prior == toolset {
			continue
		}
		if err := guardedActivityCall("updated toolset setup", func() error { return toolset.Setup(ctx, merged) }); err != nil {
			for index := len(setUp) - 1; index >= 0; index-- {
				_ = guardedActivityCall("rollback updated toolset", func() error { return setUp[index].Close(ctx) })
			}
			return fmt.Errorf("setup updated agent toolset %q: %w", toolset.ID(), err)
		}
		setUp = append(setUp, toolset)
	}
	nextExecutors, createdExecutors, err := createAgentAsyncExecutors(a.session, a.ctx, tools, previousExecutors, fallbackOptions)
	if err != nil {
		for index := len(setUp) - 1; index >= 0; index-- {
			_ = guardedActivityCall("rollback updated toolset", func() error { return setUp[index].Close(ctx) })
		}
		return err
	}
	closeCreatedExecutors := func() {
		for _, executor := range createdExecutors {
			if closeErr := executor.Close(ctx); closeErr != nil {
				a.session.emitError(fmt.Errorf("rollback async tool executor: %w", closeErr), executor)
			}
		}
	}
	nextRoutes, err := buildToolExecutorMap(a.session, tools, nextExecutors)
	if err != nil {
		closeCreatedExecutors()
		for index := len(setUp) - 1; index >= 0; index-- {
			_ = guardedActivityCall("rollback updated toolset", func() error { return setUp[index].Close(ctx) })
		}
		return err
	}
	if realtime := a.realtimeSnapshot(); realtime != nil {
		if !a.modelsSnapshot().realtime.Capabilities().MidSessionToolsUpdate {
			closeCreatedExecutors()
			for index := len(setUp) - 1; index >= 0; index-- {
				_ = guardedActivityCall("rollback unsupported realtime toolset", func() error { return setUp[index].Close(ctx) })
			}
			return errors.New("active realtime model does not support mid-session tool updates")
		}
		if err := realtime.UpdateTools(ctx, merged.Copy()); err != nil {
			closeCreatedExecutors()
			for index := len(setUp) - 1; index >= 0; index-- {
				_ = guardedActivityCall("rollback failed realtime toolset", func() error { return setUp[index].Close(ctx) })
			}
			return fmt.Errorf("update realtime tools: %w", err)
		}
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		closeCreatedExecutors()
		for _, toolset := range setUp {
			_ = guardedActivityCall("discarded updated toolset close", func() error { return toolset.Close(ctx) })
		}
		return ErrSessionClosing
	}
	a.tools, a.ownedToolsets = merged, owned
	a.asyncExecutors, a.executorByTool = nextExecutors, nextRoutes
	a.mu.Unlock()
	for async, executor := range previousExecutors {
		if nextExecutors[async] == executor {
			continue
		}
		if err := executor.Close(ctx); err != nil {
			a.session.emitError(fmt.Errorf("close replaced async tool executor: %w", err), executor)
		}
	}
	ownedByID := make(map[string]*llm.Toolset, len(owned))
	for _, toolset := range owned {
		ownedByID[toolset.ID()] = toolset
	}
	for _, toolset := range previous {
		if current := ownedByID[toolset.ID()]; current == toolset {
			continue
		}
		if err := guardedActivityCall("replaced toolset close", func() error { return toolset.Close(ctx) }); err != nil {
			a.session.emitError(err, toolset)
		}
	}
	return nil
}

func (a *agentActivity[UserData]) beginUpdate() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return false
	}
	a.updatesWG.Add(1)
	return true
}

func (a *agentActivity[UserData]) activateKeyterms() error {
	a.mu.RLock()
	closed, speech := a.closed, a.models.stt
	a.mu.RUnlock()
	if closed {
		return ErrSessionClosing
	}
	if speech == nil {
		if err := a.session.keyterms.SwapSTT(nil); err != nil {
			return err
		}
	} else if err := a.session.keyterms.Start(a.ctx, a.session, speech); err != nil {
		return err
	}
	a.mu.Lock()
	if !a.closed {
		a.keytermsActive = true
	}
	a.mu.Unlock()
	return nil
}

func (a *agentActivity[UserData]) restartKeyterms(ctx context.Context) error {
	a.mu.Lock()
	active := a.keytermsActive
	a.keytermsActive = false
	a.mu.Unlock()
	if active {
		if err := a.session.keyterms.Pause(ctx); err != nil {
			return err
		}
	}
	return a.activateKeyterms()
}

func (a *agentActivity[UserData]) resolvedTurnDetection(models activityModels) (TurnDetectionMode, *inference.TurnDetector, error) {
	selected, set := a.turn.TurnDetection.Value()
	if a.turn.TurnDetection.IsDisabled() {
		return TurnDetectionManual, nil, nil
	}
	mode := TurnDetectionMode("")
	var detector *inference.TurnDetector
	var err error
	if set {
		mode, detector, err = turnDetectionSelection(selected)
		if err != nil {
			return "", nil, err
		}
	}
	if !set || mode == "" {
		switch {
		case models.realtime != nil && models.realtime.Capabilities().TurnDetection:
			mode = TurnDetectionRealtimeLLM
		case models.vad != nil:
			mode = TurnDetectionVAD
		case models.stt != nil:
			mode = TurnDetectionSTT
		default:
			mode = TurnDetectionManual
		}
	}
	if detector != nil && models.vad == nil {
		return "", nil, errors.New("TurnDetector requires a VAD model; provide VAD or explicitly disable turn detection")
	}
	if models.realtime != nil {
		caps := models.realtime.Capabilities()
		if caps.TurnDetection {
			return TurnDetectionRealtimeLLM, nil, nil
		}
		if mode == TurnDetectionRealtimeLLM || mode == TurnDetectionSTT {
			if models.vad != nil {
				mode = TurnDetectionVAD
			} else {
				mode = TurnDetectionManual
			}
		}
	}
	return mode, detector, nil
}

func adaptiveCompatible(models activityModels, mode TurnDetectionMode) bool {
	if models.vad == nil || mode == TurnDetectionManual || mode == TurnDetectionRealtimeLLM {
		return false
	}
	if models.realtime != nil {
		return !models.realtime.Capabilities().TurnDetection
	}
	if models.stt == nil {
		return false
	}
	caps := models.stt.Capabilities()
	return caps.Streaming && caps.AlignedTranscript != stt.AlignedTranscriptNone
}

func (a *agentActivity[UserData]) configureAdaptiveInterruption(ctx context.Context) {
	models := a.modelsSnapshot()
	mode, _, err := a.resolvedTurnDetection(models)
	if err != nil {
		a.session.emitError(err, a.agent)
		return
	}
	requested := a.turn.Interruption.Enabled && a.turn.Interruption.Mode != InterruptionVAD
	compatible := requested && adaptiveCompatible(models, mode)

	a.mu.Lock()
	old, oldOwned := a.adaptiveDetector, a.adaptiveOwned
	if !compatible {
		a.adaptiveDetector, a.adaptiveOwned = nil, false
		a.adaptiveAttempted = false
		a.mu.Unlock()
		if oldOwned && old != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), a.session.opts.shutdownTimeout)
			if closeErr := old.Close(closeCtx); closeErr != nil {
				a.session.emitError(fmt.Errorf("close adaptive interruption detector: %w", closeErr), old)
			}
			cancel()
		}
		if requested && a.turn.Interruption.Mode == InterruptionAdaptive {
			a.session.emitError(errors.New("adaptive interruption is incompatible with the active VAD/STT/realtime configuration; using VAD interruption"), a.agent)
		}
		return
	}
	if old != nil || a.adaptiveAttempted {
		a.mu.Unlock()
		return
	}
	a.adaptiveAttempted = true
	provided := a.turn.Interruption.Detector
	a.mu.Unlock()

	detector := provided
	owned := false
	if detector == nil {
		detector, err = inference.NewAdaptiveInterruptionDetector(inference.AdaptiveInterruptionDetectorOptions{
			ConnectOptions: a.session.opts.connect.STT,
		})
		owned = err == nil
	}
	if err != nil {
		a.session.emitError(fmt.Errorf("adaptive interruption unavailable; using VAD interruption: %w", err), a.agent)
		return
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		if owned {
			_ = detector.Close(ctx)
		}
		return
	}
	a.adaptiveDetector, a.adaptiveOwned = detector, owned
	a.mu.Unlock()
}

func (a *agentActivity[UserData]) activateRealtime(ctx context.Context, reuse bool) error {
	return a.activateRealtimeModels(ctx, a.modelsSnapshot(), reuse)
}

func (a *agentActivity[UserData]) activateRealtimeModels(ctx context.Context, models activityModels, reuse bool) error {
	chat := a.session.ChatContext().Merge(a.agent.ChatContext(), llm.CopyOptions{})
	session, err := a.session.activateRealtime(ctx, models.realtime, a.instructionsSnapshot().Value(), chat, a.toolsSnapshot(), reuse)
	if err != nil {
		return fmt.Errorf("activate realtime model for agent %q: %w", a.agent.ID(), err)
	}
	a.subscriptionMu.Lock()
	defer a.subscriptionMu.Unlock()
	for _, unsubscribe := range a.realtimeSubs {
		unsubscribe()
	}
	a.realtimeSubs = nil
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrSessionClosing
	}
	a.models = models
	a.realtime = session
	a.realtimeSeen = make(map[string]struct{})
	a.mu.Unlock()
	if session != nil {
		a.realtimeSubs = a.subscribeRealtime(session)
	}
	return nil
}

func (a *agentActivity[UserData]) realtimeSnapshot() llm.RealtimeSession {
	a.mu.RLock()
	session := a.realtime
	a.mu.RUnlock()
	return session
}

func (a *agentActivity[UserData]) say(ctx context.Context, text stream.Reader[string], options SayOptions) (*SpeechHandle, error) {
	allow := true
	if options.AllowInterruptions != nil {
		allow = *options.AllowInterruptions
	}
	handle := NewSpeechHandle(SpeechHandleOptions{AllowInterruptions: allow, AllowInterruptionsSet: true, InputDetails: InputDetails{Modality: InputModalityText}})
	job := &speechJob{kind: speechJobSay, source: SpeechSourceSay, handle: handle, text: text, audio: options.Audio, say: options, ready: make(chan struct{})}
	job.setResult(generationResult{})
	if err := a.acceptJob(ctx, job, SpeechPriorityNormal, false); err != nil {
		return nil, err
	}
	return handle, nil
}

func (a *agentActivity[UserData]) generateReply(ctx context.Context, options GenerateReplyOptions, userInitiated, authorize, preemptive bool) (*SpeechHandle, error) {
	input := options.InputModality
	if input == "" {
		input = InputModalityText
	}
	allow := true
	if options.AllowInterruptions != nil {
		allow = *options.AllowInterruptions
	}
	handle := NewSpeechHandle(SpeechHandleOptions{AllowInterruptions: allow, AllowInterruptionsSet: true, InputDetails: InputDetails{Modality: input}})
	job := &speechJob{
		kind: speechJobReply, source: SpeechSourceGenerateReply, handle: handle, reply: options,
		ready: make(chan struct{}), segments: stream.NewChannel[*generationSegment](2), preemptive: preemptive,
	}
	a.mu.RLock()
	paused := a.authPaused != 0
	a.mu.RUnlock()
	if authorize && !paused {
		handle.AuthorizeGeneration()
	}
	if err := a.acceptJob(ctx, job, SpeechPriorityLow, userInitiated); err != nil {
		return nil, err
	}
	if err := a.generation.Send(ctx, job); err != nil {
		job.setResult(generationResult{err: err})
		a.finishJob(job, err)
		return nil, err
	}
	return handle, nil
}

func (a *agentActivity[UserData]) acceptJob(ctx context.Context, job *speechJob, priority int, userInitiated bool) error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrSessionClosed
	}
	if a.paused {
		a.mu.Unlock()
		return ErrSessionClosing
	}
	a.jobs[job.handle.ID()] = job
	a.jobsWG.Add(1)
	a.mu.Unlock()
	a.session.notifyIdleChange()
	job.handle.MarkScheduled()
	if err := a.speechQueue.Push(ctx, job.handle, priority); err != nil {
		a.finishJob(job, err)
		return err
	}
	_ = a.session.events.Publish(a.ctx, NewSpeechCreatedEvent(job.handle, job.source, userInitiated, time.Now()))
	return nil
}

func (a *agentActivity[UserData]) finishJob(job *speechJob, err error) {
	job.finishOnce.Do(func() {
		if errors.Is(err, context.Canceled) || errors.Is(err, stream.ErrClosed) || errors.Is(err, ErrSessionClosed) {
			err = nil
		}
		job.handle.MarkDone(err)
		a.mu.Lock()
		delete(a.jobs, job.handle.ID())
		if a.current == job.handle {
			a.current = nil
		}
		if a.preemptive == job {
			a.preemptive, a.preemptText, a.preemptTries = nil, "", 0
		}
		a.mu.Unlock()
		a.session.notifyIdleChange()
		a.jobsWG.Done()
	})
}

func (a *agentActivity[UserData]) startPreemptiveReply(ctx context.Context, transcript string, speechDuration time.Duration) {
	options := a.turn.PreemptiveGeneration
	if !options.Enabled || strings.TrimSpace(transcript) == "" || options.MaxSpeechDuration > 0 && speechDuration > options.MaxSpeechDuration {
		return
	}
	a.mu.Lock()
	if a.closed || a.paused || a.preemptText == transcript {
		a.mu.Unlock()
		return
	}
	previous := a.preemptive
	if previous != nil {
		if a.preemptTries >= options.MaxRetries {
			a.mu.Unlock()
			return
		}
		a.preemptTries++
	} else {
		a.preemptTries = 0
	}
	a.mu.Unlock()
	if previous != nil {
		_ = previous.handle.Interrupt(true)
	}
	handle, err := a.generateReply(ctx, GenerateReplyOptions{UserInput: transcript, InputModality: InputModalityAudio}, true, false, true)
	if err != nil {
		a.session.emitError(fmt.Errorf("start preemptive generation: %w", err), a.agent)
		return
	}
	a.mu.Lock()
	job := a.jobs[handle.ID()]
	if job != nil {
		a.preemptive, a.preemptText = job, transcript
	}
	a.mu.Unlock()
}

func (a *agentActivity[UserData]) authorizePreemptiveReply(transcript string) (*SpeechHandle, bool) {
	a.mu.Lock()
	job := a.preemptive
	if job == nil || a.preemptText != transcript || job.handle.Done() || job.handle.Interrupted() {
		a.mu.Unlock()
		return nil, false
	}
	a.preemptive, a.preemptText, a.preemptTries = nil, "", 0
	paused := a.authPaused != 0
	a.mu.Unlock()
	_ = a.commitReplyUser(job)
	if !paused {
		job.handle.AuthorizeGeneration()
	}
	return job.handle, true
}

func (a *agentActivity[UserData]) cancelPreemptiveReply() {
	a.mu.Lock()
	job := a.preemptive
	a.preemptive, a.preemptText, a.preemptTries = nil, "", 0
	a.mu.Unlock()
	if job != nil {
		_ = job.handle.Interrupt(true)
	}
}

func (a *agentActivity[UserData]) blockNewTurns() {
	a.mu.Lock()
	a.paused = true
	a.mu.Unlock()
}

func (a *agentActivity[UserData]) allowNewTurns() {
	a.mu.Lock()
	if !a.closed {
		a.paused = false
	}
	a.mu.Unlock()
}

func (a *agentActivity[UserData]) pauseReplyAuthorization() {
	a.mu.Lock()
	a.authPaused++
	for _, job := range a.jobs {
		if job.kind == speechJobReply && !job.handle.Done() {
			job.handle.ClearAuthorization()
		}
	}
	a.mu.Unlock()
}

func (a *agentActivity[UserData]) resumeReplyAuthorization() {
	a.mu.Lock()
	if a.authPaused > 0 {
		a.authPaused--
	}
	if a.authPaused == 0 {
		for _, job := range a.jobs {
			if job.kind == speechJobReply && !job.handle.Done() {
				job.handle.AuthorizeGeneration()
			}
		}
	}
	a.mu.Unlock()
}

func (a *agentActivity[UserData]) interrupt(ctx context.Context, force bool) error {
	realtime := a.realtimeSnapshot()
	a.mu.RLock()
	current := a.current
	// A playout worker removes a handle from SpeechQueue immediately before it
	// publishes it as current. Snapshot accepted jobs as well so Interrupt cannot
	// miss speech in that hand-off window.
	accepted := make([]*SpeechHandle, 0, len(a.jobs))
	for _, job := range a.jobs {
		accepted = append(accepted, job.handle)
	}
	a.mu.RUnlock()
	handles := make([]*SpeechHandle, 0, 1+a.speechQueue.Len()+len(accepted))
	seen := make(map[string]struct{}, cap(handles))
	appendHandle := func(handle *SpeechHandle) {
		if handle == nil {
			return
		}
		if _, exists := seen[handle.ID()]; exists {
			return
		}
		seen[handle.ID()] = struct{}{}
		handles = append(handles, handle)
	}
	if current != nil {
		appendHandle(current)
	}
	for _, handle := range a.speechQueue.Ordered() {
		appendHandle(handle)
	}
	for _, handle := range accepted {
		appendHandle(handle)
	}
	if len(handles) == 0 {
		if realtime != nil {
			if err := realtime.Interrupt(ctx); err != nil && context.Cause(a.ctx) == nil {
				a.session.emitError(fmt.Errorf("interrupt realtime generation: %w", err), realtime)
			}
			return nil
		}
		return ErrNoSpeech
	}
	// Never cancel the provider response while the corresponding local speech is
	// explicitly uninterruptible. Doing so would let local playout continue even
	// though the provider has discarded its generation and conversation state.
	if !force && !handles[0].AllowInterruptions() {
		return ErrInterruptionsDisabled
	}
	if realtime != nil {
		if err := realtime.Interrupt(ctx); err != nil && context.Cause(a.ctx) == nil {
			a.session.emitError(fmt.Errorf("interrupt realtime generation: %w", err), realtime)
		}
	}
	interrupted := false
	for _, handle := range handles {
		if !force && !handle.AllowInterruptions() {
			if !interrupted {
				return ErrInterruptionsDisabled
			}
			break
		}
		if err := handle.Interrupt(force); err != nil {
			if errors.Is(err, ErrInterruptionsDisabled) {
				break
			}
			return err
		}
		interrupted = true
	}
	if current != nil && current.Interrupted() {
		if output := a.session.output.Audio(); output != nil && a.session.output.AudioEnabled() {
			if err := output.ClearBuffer(ctx); err != nil {
				a.session.emitError(err, output)
			}
		}
	}
	return nil
}

func (a *agentActivity[UserData]) drain(ctx context.Context) error {
	a.blockNewTurns()
	a.cancelPreemptiveReply()
	a.resumeAllAuthorization()
	a.mu.RLock()
	executors := make([]*ToolExecutor, 0, 1+len(a.asyncExecutors))
	if a.toolExecutor != nil {
		executors = append(executors, a.toolExecutor)
	}
	for _, executor := range a.asyncExecutors {
		executors = append(executors, executor)
	}
	a.mu.RUnlock()
	for _, executor := range executors {
		executor.beginDrain()
	}
	jobsDone := make(chan struct{})
	go func() { a.jobsWG.Wait(); close(jobsDone) }()
	executorsDone := make(chan error, 1)
	go func() {
		var errs []error
		for _, executor := range executors {
			if err := executor.Drain(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		executorsDone <- errors.Join(errs...)
	}()
	var jobsComplete, executorsComplete bool
	var executorErr error
	for !jobsComplete || !executorsComplete {
		select {
		case <-ctx.Done():
			return errors.Join(context.Cause(ctx), executorErr)
		case <-jobsDone:
			jobsComplete = true
			jobsDone = nil
		case executorErr = <-executorsDone:
			executorsComplete = true
			executorsDone = nil
		}
	}
	return executorErr
}

func (a *agentActivity[UserData]) resumeAllAuthorization() {
	a.mu.Lock()
	a.authPaused = 0
	for _, job := range a.jobs {
		if job.kind == speechJobReply && !job.handle.Done() {
			job.handle.AuthorizeGeneration()
		}
	}
	a.mu.Unlock()
}

func (a *agentActivity[UserData]) close(ctx context.Context) error {
	a.closeOnce.Do(func() {
		var errs []error
		a.mu.Lock()
		a.closed, a.paused = true, true
		recognition := a.recognition
		a.recognition = nil
		realtime, realtimeAudio := a.realtime, a.realtimeAudio
		a.realtime = nil
		a.realtimeAudio = false
		jobs := make([]*speechJob, 0, len(a.jobs))
		for _, job := range a.jobs {
			jobs = append(jobs, job)
		}
		keytermsActive := a.keytermsActive
		a.keytermsActive = false
		adaptive, adaptiveOwned := a.adaptiveDetector, a.adaptiveOwned
		a.adaptiveDetector, a.adaptiveOwned = nil, false
		asyncExecutors := make([]*ToolExecutor, 0, len(a.asyncExecutors))
		for _, executor := range a.asyncExecutors {
			asyncExecutors = append(asyncExecutors, executor)
		}
		a.mu.Unlock()

		if keytermsActive {
			if err := a.session.keyterms.Pause(ctx); err != nil {
				errs = append(errs, fmt.Errorf("pause keyterm detection: %w", err))
			}
		}
		a.cancel(ErrSessionClosed)
		_ = a.generation.Close()
		a.speechQueue.Close()
		if recognition != nil {
			if err := recognition.Close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if adaptiveOwned && adaptive != nil {
			if err := adaptive.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("close adaptive interruption detector: %w", err))
			}
		}
		if realtime != nil && realtimeAudio {
			if err := realtime.SetInputAudioStream(ctx, nil); err != nil {
				errs = append(errs, fmt.Errorf("detach realtime audio input: %w", err))
			}
		}
		for _, job := range jobs {
			_ = job.handle.Interrupt(true)
		}
		a.subscriptionMu.Lock()
		for _, unsubscribe := range a.unsubscribers {
			unsubscribe()
		}
		for _, unsubscribe := range a.realtimeSubs {
			unsubscribe()
		}
		a.unsubscribers = nil
		a.realtimeSubs = nil
		a.subscriptionMu.Unlock()

		if err := a.toolExecutor.Close(ctx); err != nil {
			errs = append(errs, err)
		}
		for _, executor := range asyncExecutors {
			if err := executor.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("close agent async tool executor: %w", err))
			}
		}
		if err := waitGroups(ctx, &a.workersWG, &a.hooksWG, &a.attachWG, &a.updatesWG); err != nil {
			errs = append(errs, err)
		}
		a.mu.RLock()
		toolsets := append([]*llm.Toolset(nil), a.ownedToolsets...)
		a.mu.RUnlock()
		for _, toolset := range toolsets {
			if err := guardedActivityCall("toolset close", func() error { return toolset.Close(ctx) }); err != nil {
				errs = append(errs, fmt.Errorf("close agent toolset %q: %w", toolset.ID(), err))
			}
		}
		if err := guardedActivityCall("onExit", func() error { return a.agent.runOnExit(ctx) }); err != nil && !IsStopResponse(err) {
			errs = append(errs, fmt.Errorf("agent %q onExit: %w", a.agent.ID(), err))
		}
		a.agent.unbindRuntime(a)
		a.mu.Lock()
		a.shutdownErr = errors.Join(errs...)
		a.mu.Unlock()
	})

	a.mu.RLock()
	err := a.shutdownErr
	a.mu.RUnlock()
	return err
}

func waitGroups(ctx context.Context, groups ...*sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		for _, group := range groups {
			group.Wait()
		}
		close(done)
	}()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
		return nil
	}
}

func (a *agentActivity[UserData]) subscribeModels() {
	a.subscriptionMu.Lock()
	defer a.subscriptionMu.Unlock()
	for _, unsubscribe := range a.unsubscribers {
		unsubscribe()
	}
	a.unsubscribers = nil
	a.mu.RLock()
	closed, models := a.closed, a.models
	a.mu.RUnlock()
	if closed {
		return
	}
	if models.stt != nil {
		a.unsubscribers = append(a.unsubscribers,
			models.stt.OnMetrics(func(value metrics.STT) { a.session.collectMetric(value) }),
			models.stt.OnError(func(event stt.ErrorEvent) { a.modelError(event.Err, event.Recoverable, models.stt) }),
		)
	}
	if models.vad != nil {
		a.unsubscribers = append(a.unsubscribers, models.vad.OnMetrics(func(value metrics.VAD) { a.session.collectMetric(value) }))
	}
	if _, detector, err := a.resolvedTurnDetection(models); err == nil && detector != nil {
		a.unsubscribers = append(a.unsubscribers, detector.OnMetrics(func(value metrics.EOTInference) { a.session.collectMetric(value) }))
	}
	a.mu.RLock()
	adaptive := a.adaptiveDetector
	a.mu.RUnlock()
	if adaptive != nil {
		a.unsubscribers = append(a.unsubscribers,
			adaptive.OnMetrics(func(value metrics.Interruption) { a.session.collectMetric(value) }),
			adaptive.OnError(func(event inference.InterruptionDetectionError) {
				a.session.emitError(event, adaptive)
				if !event.Recoverable {
					go a.fallbackAdaptiveInterruption(adaptive, event)
				}
			}),
		)
	}
	if models.llm != nil {
		a.unsubscribers = append(a.unsubscribers,
			models.llm.OnMetrics(func(value metrics.LLM) { a.session.collectMetric(value) }),
			models.llm.OnError(func(event llm.ErrorEvent) { a.modelError(event.Err, event.Recoverable, models.llm) }),
		)
	}
	if models.tts != nil {
		a.unsubscribers = append(a.unsubscribers,
			models.tts.OnMetrics(func(value metrics.TTS) { a.session.collectMetric(value) }),
			models.tts.OnError(func(event tts.ErrorEvent) { a.modelError(event.Err, event.Recoverable, models.tts) }),
		)
	}
}

func (a *agentActivity[UserData]) fallbackAdaptiveInterruption(detector *inference.AdaptiveInterruptionDetector, cause error) {
	a.mu.Lock()
	if a.adaptiveDetector != detector {
		a.mu.Unlock()
		return
	}
	owned := a.adaptiveOwned
	a.adaptiveDetector, a.adaptiveOwned = nil, false
	recognition := a.recognition
	a.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), a.session.opts.shutdownTimeout)
	if recognition != nil {
		if err := recognition.DisableAdaptiveInterruption(ctx); err != nil && !errors.Is(err, ErrRecognitionClosed) {
			a.session.emitError(fmt.Errorf("disable failed adaptive interruption: %w", err), detector)
		}
	}
	if owned {
		if err := detector.Close(ctx); err != nil {
			a.session.emitError(fmt.Errorf("close failed adaptive interruption: %w", err), detector)
		}
	}
	cancel()
	a.session.emitError(fmt.Errorf("adaptive interruption disabled after unrecoverable error; using VAD interruption: %w", cause), detector)
}

func (a *agentActivity[UserData]) subscribeRealtime(session llm.RealtimeSession) []func() {
	return []func(){
		session.OnInputSpeechStarted(func(llm.InputSpeechStartedEvent) {
			a.session.setUserState(UserStateSpeaking)
			if err := a.interrupt(a.ctx, false); err != nil && !errors.Is(err, ErrNoSpeech) && !errors.Is(err, ErrInterruptionsDisabled) {
				a.session.emitError(err, session)
			}
		}),
		session.OnInputSpeechStopped(func(event llm.InputSpeechStoppedEvent) {
			a.session.setUserState(UserStateListening)
			if event.UserTranscriptionEnabled {
				_ = a.session.events.Publish(a.ctx, UserInputTranscribedEvent{
					EventBase: newEventBase(EventUserInputTranscribed, time.Now()), Final: false,
				})
			}
		}),
		session.OnInputTranscriptionCompleted(a.onRealtimeTranscription),
		session.OnGenerationCreated(func(event llm.GenerationCreatedEvent) {
			if err := a.enqueueRealtimeGeneration(event); err != nil && context.Cause(a.ctx) == nil {
				a.session.emitError(fmt.Errorf("enqueue realtime generation: %w", err), session)
			}
		}),
		session.OnMetrics(func(value metrics.Realtime) { a.session.collectMetric(value) }),
		session.OnError(func(event llm.RealtimeModelError) {
			a.session.emitError(event, session)
			if !event.Recoverable {
				a.session.cancel(fmt.Errorf("unrecoverable realtime model error: %w", event))
			}
		}),
		session.OnReconnected(func(llm.RealtimeSessionReconnectedEvent) {
			if err := updateRealtimeConfiguration(a.ctx, session, a.instructionsSnapshot().Value(), a.session.ChatContext(), a.toolsSnapshot()); err != nil && context.Cause(a.ctx) == nil {
				a.session.emitError(fmt.Errorf("resync reconnected realtime session: %w", err), session)
			}
		}),
	}
}

func (a *agentActivity[UserData]) onRealtimeTranscription(event llm.InputTranscriptionCompletedEvent) {
	itemID := event.ItemID
	var itemIDPointer *string
	if itemID != "" {
		itemIDPointer = &itemID
	}
	_ = a.session.events.Publish(a.ctx, UserInputTranscribedEvent{
		EventBase: newEventBase(EventUserInputTranscribed, time.Now()), Transcript: event.Transcript,
		Final: event.IsFinal, ItemID: itemIDPointer,
	})
	if !event.IsFinal || strings.TrimSpace(event.Transcript) == "" {
		return
	}
	key := itemID
	if key == "" {
		key = event.Transcript
	}
	a.mu.Lock()
	if _, exists := a.realtimeSeen[key]; exists {
		a.mu.Unlock()
		return
	}
	a.realtimeSeen[key] = struct{}{}
	a.mu.Unlock()
	message := llm.NewChatMessage(llm.RoleUser, event.Transcript)
	if itemID != "" {
		message.ID = itemID
	}
	if event.TurnStartedAt != nil {
		message.CreatedAt = *event.TurnStartedAt
		message.Metrics.StartedSpeakingAt = *event.TurnStartedAt
	}
	a.session.commitItems(nil, message)
}

func (a *agentActivity[UserData]) modelError(err error, recoverable bool, source any) {
	a.session.emitError(err, source)
	if recoverable {
		return
	}
	a.session.mu.Lock()
	tooMany := true
	switch source.(type) {
	case llm.LLM:
		a.session.llmUnrecoverable++
		tooMany = a.session.llmUnrecoverable > a.session.opts.connect.MaxUnrecoverableErrors
	case tts.TTS:
		a.session.ttsUnrecoverable++
		tooMany = a.session.ttsUnrecoverable > a.session.opts.connect.MaxUnrecoverableErrors
	}
	a.session.mu.Unlock()
	if tooMany {
		a.session.cancel(fmt.Errorf("too many unrecoverable model errors: %w", err))
	}
}

func (a *agentActivity[UserData]) modelsSnapshot() activityModels {
	a.mu.RLock()
	models := a.models
	a.mu.RUnlock()
	return models
}

func (a *agentActivity[UserData]) agentSpeechStateChanged(old, state AgentState, at time.Time) {
	a.mu.RLock()
	recognition := a.recognition
	a.mu.RUnlock()
	if recognition == nil {
		return
	}
	var err error
	switch {
	case old != AgentStateSpeaking && state == AgentStateSpeaking:
		err = recognition.AgentSpeechStarted(a.ctx, at)
	case old == AgentStateSpeaking && state != AgentStateSpeaking:
		err = recognition.AgentSpeechEnded(a.ctx, at)
	}
	if err != nil && context.Cause(a.ctx) == nil && !errors.Is(err, ErrRecognitionClosed) {
		a.session.emitError(fmt.Errorf("update adaptive agent speech lifecycle: %w", err), recognition)
	}
}

func (a *agentActivity[UserData]) toolsSnapshot() *llm.Context {
	a.mu.RLock()
	tools := a.tools.Copy()
	a.mu.RUnlock()
	return tools
}

func (a *agentActivity[UserData]) executeFunctionTools(ctx context.Context, handle *SpeechHandle, calls []*llm.FunctionCall, tools *llm.Context) ([]ToolExecutionResult, error) {
	if len(calls) > DefaultToolBatchLimit {
		return nil, fmt.Errorf("%w: got %d, limit %d", ErrToolBatchTooLarge, len(calls), DefaultToolBatchLimit)
	}
	a.mu.RLock()
	defaultExecutor := a.toolExecutor
	routes := make(map[string]*ToolExecutor, len(a.executorByTool))
	for name, executor := range a.executorByTool {
		routes[name] = executor
	}
	a.mu.RUnlock()
	if defaultExecutor == nil {
		return nil, ErrToolExecutorClosed
	}
	type executorGroup struct {
		executor *ToolExecutor
		calls    []*llm.FunctionCall
		indices  []int
	}
	groupsByExecutor := make(map[*ToolExecutor]*executorGroup)
	groups := make([]*executorGroup, 0)
	for index, call := range calls {
		executor := defaultExecutor
		if call != nil && routes[call.Name] != nil {
			executor = routes[call.Name]
		}
		group := groupsByExecutor[executor]
		if group == nil {
			group = &executorGroup{executor: executor}
			groupsByExecutor[executor] = group
			groups = append(groups, group)
		}
		group.calls = append(group.calls, call)
		group.indices = append(group.indices, index)
	}
	results := make([]ToolExecutionResult, len(calls))
	if len(groups) == 0 {
		return results, nil
	}
	type groupResult struct {
		group   *executorGroup
		results []ToolExecutionResult
		err     error
	}
	completed := make(chan groupResult, len(groups))
	for _, group := range groups {
		go func(group *executorGroup) {
			values, err := group.executor.Execute(ctx, group.calls, tools, ToolExecutionOptions{SpeechHandle: handle})
			completed <- groupResult{group: group, results: values, err: err}
		}(group)
	}
	var errs []error
	for range groups {
		result := <-completed
		if result.err != nil {
			errs = append(errs, result.err)
			continue
		}
		if len(result.results) != len(result.group.indices) {
			errs = append(errs, errors.New("tool executor returned an invalid result count"))
			continue
		}
		for index, target := range result.group.indices {
			results[target] = result.results[index]
		}
	}
	return results, errors.Join(errs...)
}

func (a *agentActivity[UserData]) instructionsSnapshot() llm.Instructions {
	a.mu.RLock()
	instructions := a.instructions
	a.mu.RUnlock()
	return instructions
}

func (a *agentActivity[UserData]) playoutWorker() {
	defer a.workersWG.Done()
	for {
		handle, err := a.speechQueue.Pop(a.ctx)
		if err != nil {
			return
		}
		a.mu.Lock()
		job := a.jobs[handle.ID()]
		if job != nil {
			a.current = handle
		}
		a.mu.Unlock()
		if job == nil {
			continue
		}
		if handle.Interrupted() {
			a.finishJob(job, nil)
			continue
		}
		jobCtx, release := speechContext(a.ctx, handle)
		err = guardedActivityCall("speech playout", func() error { return a.playJob(jobCtx, job) })
		release()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrSessionClosed) && !errors.Is(err, stream.ErrClosed) && !errors.Is(err, io.EOF) {
			a.session.emitError(err, a.agent)
		}
		a.finishJob(job, err)
		if a.pendingJobs() == 0 {
			a.session.setAgentState(AgentStateListening)
		}
	}
}

func (a *agentActivity[UserData]) pendingJobs() int {
	a.mu.RLock()
	count := len(a.jobs)
	a.mu.RUnlock()
	return count
}

func (a *agentActivity[UserData]) idle() bool {
	a.mu.RLock()
	idle := !a.closed && len(a.jobs) == 0 && a.current == nil
	recognition := a.recognition
	a.mu.RUnlock()
	return idle && (recognition == nil || !recognition.Busy())
}

func speechContext(parent context.Context, handle *SpeechHandle) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	go func() {
		select {
		case <-handle.InterruptSignal():
			cancel(context.Canceled)
		case <-ctx.Done():
		case <-done:
		}
	}()
	owned, unregister := handle.OwnContext(ctx)
	var once sync.Once
	return owned, func() {
		once.Do(func() {
			close(done)
			unregister()
			cancel(nil)
		})
	}
}
