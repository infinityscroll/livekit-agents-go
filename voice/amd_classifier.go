// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/stt"
	"go.opentelemetry.io/otel/trace"
)

type amdCommandKind uint8

const (
	amdCommandStartListening amdCommandKind = iota
	amdCommandParticipantMissing
	amdCommandSpeechStarted
	amdCommandSpeechEnded
	amdCommandTranscript
	amdCommandEndOfTurn
	amdCommandSessionClosed
	amdCommandClassification
)

type amdCommand struct {
	kind       amdCommandKind
	at         time.Time
	duration   time.Duration
	text       string
	source     AMDTranscriptSource
	generation uint64
	detection  amdDetection
	err        error
	response   chan amdCommandResponse
}

type amdCommandResponse struct {
	skip bool
	err  error
}

type amdOutcome struct {
	event AMDPredictionEvent
	err   error
}

type amdDetection struct {
	category   AMDCategory
	reason     string
	raw        string
	transcript string
	saved      bool
	toolCalls  bool
	extensions []time.Duration
	speechMS   int64
	delayMS    int64
}

type amdSilenceTrigger uint8

const (
	amdSilenceNone amdSilenceTrigger = iota
	amdSilenceShortSpeech
	amdSilenceLongSpeech
	amdSilenceExtension
)

type amdRun struct {
	owner     *AMD
	ctx       context.Context
	cancel    context.CancelCauseFunc
	execution *AMDExecution
	commands  chan amdCommand
	outcome   chan amdOutcome
	actorDone chan struct{}
	started   time.Time
	span      trace.Span
	listening atomic.Bool

	subscription *EventSubscription
	workers      sync.WaitGroup

	resourceMu sync.Mutex
	stopped    bool
	llmStream  llm.LLMStream
	sttStream  stt.SpeechStream
	audio      AMDAudioSubscription
}

type amdActorState struct {
	run *amdRun

	listening      bool
	settled        bool
	transcript     []string
	verdict        *AMDPredictionEvent
	silenceReached bool
	eotReached     bool
	speechStarted  time.Time
	speechEnded    time.Time
	speechActive   bool

	detectGeneration uint64
	extensionCount   int
	classifying      bool
	classifyingGen   uint64
	classifyCancel   context.CancelCauseFunc
	pendingClassify  bool

	noSpeechTimer  *time.Timer
	detectionTimer *time.Timer
	silenceTimer   *time.Timer
	eotTimer       *time.Timer
	silenceTrigger amdSilenceTrigger
}

func newAMDRun(parent context.Context, owner *AMD, span trace.Span) (*amdRun, error) {
	ctx, cancel := context.WithCancelCause(parent)
	execution := &AMDExecution{done: make(chan struct{}), cancel: cancel}
	run := &amdRun{
		owner: owner, ctx: ctx, cancel: cancel, execution: execution,
		commands: make(chan amdCommand), outcome: make(chan amdOutcome, 1),
		actorDone: make(chan struct{}), started: time.Now(), span: span,
	}
	if owner.session.Subscribe != nil {
		var subscription *EventSubscription
		err := guardAMD("subscribe session events", func() (err error) {
			subscription, err = owner.session.Subscribe(EventSubscriptionOptions{Capacity: owner.opts.eventCapacity})
			return err
		})
		if err != nil {
			cancel(err)
			return nil, fmt.Errorf("subscribe AMD session events: %w", err)
		}
		run.subscription = subscription
	}
	return run, nil
}

func (r *amdRun) start() {
	subscription := r.subscription
	initialWorkers := 0
	if subscription != nil {
		initialWorkers++
	}
	if r.owner.stt != nil {
		initialWorkers++
	}
	if r.owner.opts.listeningGate != nil {
		initialWorkers++
	}
	if initialWorkers != 0 {
		r.workers.Add(initialWorkers)
	}
	go r.runActor()
	if subscription != nil {
		go r.runSessionEvents(subscription)
	}
	if r.owner.stt != nil {
		go r.runDedicatedSTT()
	}
	if r.owner.opts.listeningGate != nil {
		go r.runListeningGate()
		return
	}
	_ = r.send(r.ctx, amdCommand{kind: amdCommandStartListening})
}

func (r *amdRun) send(ctx context.Context, command amdCommand) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case r.commands <- command:
		return nil
	case <-r.actorDone:
		return errAMDRunComplete
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.ctx.Done():
		return context.Cause(r.ctx)
	}
}

func (r *amdRun) runActor() {
	// Listening is observable state, so clear it on every terminal path,
	// including cancellation and setup/classifier failures.
	defer r.listening.Store(false)
	state := amdActorState{run: r}
	outcome := state.loop()
	r.cancel(errAMDRunComplete)
	r.closeResources()
	r.workers.Wait()
	state.stopAllTimers()
	close(r.actorDone)
	r.outcome <- outcome
}

func (s *amdActorState) loop() amdOutcome {
	for {
		select {
		case <-s.run.ctx.Done():
			return amdOutcome{err: context.Cause(s.run.ctx)}
		case command := <-s.run.commands:
			if outcome, done := s.handleCommand(command); done {
				return outcome
			}
		case <-timerChannel(s.noSpeechTimer):
			s.noSpeechTimer = nil
			s.settle(AMDCategoryUncertain, "no_speech_timeout", nil)
			if outcome, done := s.tryFinish(); done {
				return outcome
			}
		case <-timerChannel(s.detectionTimer):
			s.detectionTimer = nil
			s.settle(AMDCategoryUncertain, "detection_timeout", nil)
			if outcome, done := s.tryFinish(); done {
				return outcome
			}
		case <-timerChannel(s.silenceTimer):
			trigger := s.silenceTrigger
			s.silenceTimer = nil
			s.silenceTrigger = amdSilenceNone
			s.silenceReached = true
			if trigger == amdSilenceShortSpeech {
				duration := s.computeSpeechDuration()
				s.settle(AMDCategoryHuman, "short_greeting", &duration)
			} else if trigger == amdSilenceExtension {
				s.scheduleClassification()
			}
			if outcome, done := s.tryFinish(); done {
				return outcome
			}
		case <-timerChannel(s.eotTimer):
			s.eotTimer = nil
			s.eotReached = true
			if outcome, done := s.tryFinish(); done {
				return outcome
			}
		}
	}
}

func (s *amdActorState) handleCommand(command amdCommand) (amdOutcome, bool) {
	switch command.kind {
	case amdCommandStartListening:
		s.startListening()
	case amdCommandParticipantMissing:
		s.settle(AMDCategoryUncertain, "participant_missing", nil)
	case amdCommandSpeechStarted:
		s.onSpeechStarted(command.at)
	case amdCommandSpeechEnded:
		s.onSpeechEnded(command.at, command.duration)
	case amdCommandTranscript:
		s.onTranscript(command.text, command.source)
	case amdCommandEndOfTurn:
		s.onEndOfTurn()
		outcome, done := s.tryFinish()
		skip := s.run.owner.opts.interruptOnMachine && s.settled && s.verdict != nil && s.verdict.IsMachine
		if command.response != nil {
			command.response <- amdCommandResponse{skip: skip}
		}
		return outcome, done
	case amdCommandSessionClosed:
		s.eotReached = true
		s.settle(AMDCategoryUncertain, "session_closed", nil)
	case amdCommandClassification:
		if outcome, done := s.onClassification(command); done {
			return outcome, true
		}
	}
	return s.tryFinish()
}

func (s *amdActorState) startListening() {
	if s.settled || s.listening {
		return
	}
	s.listening = true
	s.run.listening.Store(true)
	s.resetTimer(&s.detectionTimer, s.run.owner.opts.detectionTimeout)
	s.resetTimer(&s.noSpeechTimer, s.run.owner.opts.noSpeechTimeout)
}

func (s *amdActorState) onSpeechStarted(at time.Time) {
	if s.settled || !s.listening {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	s.stopTimer(&s.silenceTimer)
	s.silenceTrigger = amdSilenceNone
	s.stopTimer(&s.noSpeechTimer)
	s.stopTimer(&s.eotTimer)
	if s.speechStarted.IsZero() {
		s.speechStarted = at
	}
	s.speechActive = true
	s.silenceReached = false
	s.eotReached = false
}

func (s *amdActorState) onSpeechEnded(at time.Time, silenceDuration time.Duration) {
	if s.settled || !s.listening || s.speechStarted.IsZero() {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	if silenceDuration < 0 {
		silenceDuration = 0
	}
	s.speechEnded = at.Add(-silenceDuration)
	s.speechActive = false
	s.stopTimer(&s.silenceTimer)
	s.silenceTrigger = amdSilenceNone
	s.armEOT(max(0, s.run.owner.opts.maxEndpointingDelay-silenceDuration))
	speechDuration := s.computeSpeechDuration()
	if speechDuration <= s.run.owner.opts.humanSpeechThreshold && len(s.transcript) == 0 {
		s.resetTimer(&s.silenceTimer, max(0, s.run.owner.opts.humanSilenceThreshold-silenceDuration))
		s.silenceTrigger = amdSilenceShortSpeech
		return
	}
	s.resetTimer(&s.silenceTimer, max(0, s.run.owner.opts.machineSilenceThreshold-silenceDuration))
	s.silenceTrigger = amdSilenceLongSpeech
}

func (s *amdActorState) onTranscript(text string, source AMDTranscriptSource) {
	if s.settled || !s.listening || source != s.run.owner.transcriptSource() {
		return
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	s.armEOT(s.run.owner.opts.maxEndpointingDelay)
	if s.silenceTimer != nil && s.silenceTrigger == amdSilenceShortSpeech {
		s.stopTimer(&s.silenceTimer)
		s.silenceTrigger = amdSilenceNone
		if !s.speechEnded.IsZero() {
			remaining := max(time.Duration(0), time.Until(s.speechEnded.Add(s.run.owner.opts.machineSilenceThreshold)))
			s.resetTimer(&s.silenceTimer, remaining)
			s.silenceTrigger = amdSilenceLongSpeech
		}
	}
	s.stopTimer(&s.noSpeechTimer)
	s.transcript = append(s.transcript, text)
	s.scheduleClassification()
}

func (s *amdActorState) onEndOfTurn() {
	if s.settled {
		return
	}
	s.stopTimer(&s.eotTimer)
	s.eotReached = true
}

func (s *amdActorState) settle(category AMDCategory, reason string, speechDuration *time.Duration) {
	if s.settled {
		return
	}
	s.stopTimer(&s.silenceTimer)
	s.silenceTrigger = amdSilenceNone
	s.silenceReached = true
	hasSpeech := !s.speechStarted.IsZero() || len(s.transcript) > 0
	if !(s.run.owner.opts.waitUntilFinished && hasSpeech) {
		s.eotReached = true
	}
	if s.verdict == nil {
		duration := s.computeSpeechDuration()
		if speechDuration != nil {
			duration = *speechDuration
		}
		event := s.newEvent(category, reason, "", s.joinTranscript(), duration, s.computeDelay())
		s.verdict = &event
	}
}

func (s *amdActorState) tryFinish() (amdOutcome, bool) {
	if s.settled || s.verdict == nil || !s.silenceReached {
		return amdOutcome{}, false
	}
	if s.verdict.Category != AMDCategoryHuman && !s.eotReached {
		return amdOutcome{}, false
	}
	s.settled = true
	s.stopTimer(&s.detectionTimer)
	s.stopTimer(&s.noSpeechTimer)
	s.stopTimer(&s.silenceTimer)
	s.stopTimer(&s.eotTimer)
	if s.classifyCancel != nil {
		s.classifyCancel(errAMDRunComplete)
		s.classifyCancel = nil
	}
	s.run.closeLLMStream()
	s.run.listening.Store(false)
	return amdOutcome{event: *s.verdict}, true
}

func (s *amdActorState) scheduleClassification() {
	if len(s.transcript) == 0 || s.settled {
		return
	}
	s.detectGeneration++
	if s.classifying {
		s.pendingClassify = true
		if s.classifyCancel != nil {
			s.classifyCancel(context.Canceled)
		}
		s.run.closeLLMStream()
		return
	}
	s.startClassification()
}

func (s *amdActorState) startClassification() {
	if s.classifying || s.settled || len(s.transcript) == 0 {
		return
	}
	generation := s.detectGeneration
	transcript := s.joinTranscript()
	ctx, cancel := context.WithCancelCause(s.run.ctx)
	s.classifying = true
	s.classifyingGen = generation
	s.classifyCancel = cancel
	s.pendingClassify = false
	extensions := s.extensionCount
	speechDuration, delay := s.computeSpeechDuration(), s.computeDelay()
	s.run.workers.Add(1)
	go func() {
		defer s.run.workers.Done()
		detection, err := s.run.owner.detect(ctx, s.run, transcript, generation, extensions, speechDuration, delay)
		_ = s.run.send(s.run.ctx, amdCommand{
			kind: amdCommandClassification, generation: generation,
			detection: detection, err: err,
		})
	}()
}

func (s *amdActorState) onClassification(command amdCommand) (amdOutcome, bool) {
	if !s.classifying || command.generation != s.classifyingGen {
		return amdOutcome{}, false
	}
	s.classifying = false
	s.classifyCancel = nil
	stale := command.generation != s.detectGeneration || s.settled
	if !stale && command.err != nil {
		return amdOutcome{err: fmt.Errorf("classify AMD transcript: %w", command.err)}, true
	}
	if !stale {
		for _, extension := range command.detection.extensions {
			s.extensionCount++
			s.stopTimer(&s.silenceTimer)
			s.silenceTrigger = amdSilenceNone
			s.resetTimer(&s.silenceTimer, extension)
			s.silenceTrigger = amdSilenceExtension
		}
		if command.detection.saved || !command.detection.toolCalls && command.detection.category != AMDCategoryUncertain {
			event := s.newEvent(
				command.detection.category, command.detection.reason,
				command.detection.raw, command.detection.transcript,
				time.Duration(command.detection.speechMS)*time.Millisecond,
				time.Duration(command.detection.delayMS)*time.Millisecond,
			)
			s.verdict = &event
		}
	}
	if s.pendingClassify || stale && command.generation != s.detectGeneration {
		s.startClassification()
	}
	return s.tryFinish()
}

func (s *amdActorState) armEOT(delay time.Duration) {
	if s.settled || s.speechActive {
		return
	}
	s.stopTimer(&s.eotTimer)
	s.eotReached = false
	s.resetTimer(&s.eotTimer, max(time.Duration(0), delay))
}

func (s *amdActorState) resetTimer(target **time.Timer, duration time.Duration) {
	s.stopTimer(target)
	*target = time.NewTimer(max(time.Duration(0), duration))
}

func (s *amdActorState) stopTimer(target **time.Timer) {
	if *target == nil {
		return
	}
	if !(*target).Stop() {
		select {
		case <-(*target).C:
		default:
		}
	}
	*target = nil
}

func (s *amdActorState) stopAllTimers() {
	s.stopTimer(&s.noSpeechTimer)
	s.stopTimer(&s.detectionTimer)
	s.stopTimer(&s.silenceTimer)
	s.stopTimer(&s.eotTimer)
}

func (s *amdActorState) joinTranscript() string { return strings.Join(s.transcript, " ") }

func (s *amdActorState) computeSpeechDuration() time.Duration {
	if s.speechStarted.IsZero() {
		return 0
	}
	end := s.speechEnded
	if end.IsZero() {
		end = time.Now()
	}
	return max(time.Duration(0), end.Sub(s.speechStarted))
}

func (s *amdActorState) computeDelay() time.Duration {
	if s.speechEnded.IsZero() {
		return 0
	}
	return max(time.Duration(0), time.Since(s.speechEnded))
}

func (s *amdActorState) newEvent(category AMDCategory, reason, raw, transcript string, speechDuration, delay time.Duration) AMDPredictionEvent {
	return AMDPredictionEvent{
		EventBase: newEventBase(EventAMDPrediction, time.Now()),
		Category:  category, Transcript: transcript, Reason: reason, RawResponse: raw,
		IsMachine: isAMDMachine(category), SpeechDurationMS: ceilMilliseconds(speechDuration),
		DelayMS: ceilMilliseconds(delay),
	}
}

func timerChannel(timer *time.Timer) <-chan time.Time {
	if timer == nil {
		return nil
	}
	return timer.C
}

func ceilMilliseconds(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return int64((duration + time.Millisecond - 1) / time.Millisecond)
}

func (a *AMD) transcriptSource() AMDTranscriptSource {
	if a.stt != nil {
		return AMDTranscriptSourceDedicatedSTT
	}
	return AMDTranscriptSourceSessionSTT
}

type amdSavePredictionInput struct {
	Label AMDCategory `json:"label"`
}
type amdPostponeInput struct {
	Seconds float64 `json:"seconds"`
}

var amdSavePredictionSchema = json.RawMessage(`{"type":"object","properties":{"label":{"type":"string","enum":["human","machine-ivr","machine-vm","machine-unavailable","uncertain"]}},"required":["label"],"additionalProperties":false}`)
var amdPostponeSchema = json.RawMessage(`{"type":"object","properties":{"seconds":{"type":"number","description":"Additional seconds to wait (max 10)."}},"required":["seconds"],"additionalProperties":false}`)

func (a *AMD) detect(
	ctx context.Context,
	run *amdRun,
	transcript string,
	generation uint64,
	extensionCount int,
	speechDuration time.Duration,
	delay time.Duration,
) (amdDetection, error) {
	save, err := llm.NewTool(llm.FunctionToolOptions[amdSavePredictionInput, string]{
		Name: "save_prediction", Description: "Save the AMD prediction to the verdict.",
		Parameters: amdSavePredictionSchema,
		Execute:    func(context.Context, amdSavePredictionInput, llm.ToolOptions) (string, error) { return "saved", nil },
	})
	if err != nil {
		return amdDetection{}, err
	}
	entries := []any{save}
	if extensionCount < DefaultAMDMaxExtensions {
		postpone, toolErr := llm.NewTool(llm.FunctionToolOptions[amdPostponeInput, string]{
			Name:        "postpone_termination",
			Description: "Postpone the termination of the classification task. Use when the transcript is ambiguous and more audio is expected.",
			Parameters:  amdPostponeSchema,
			Execute: func(_ context.Context, input amdPostponeInput, _ llm.ToolOptions) (string, error) {
				seconds := input.Seconds
				if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
					seconds = 0
				}
				seconds = min(max(seconds, 0), DefaultAMDMaxExtension.Seconds())
				return fmt.Sprintf("waiting %.1fs for more audio", seconds), nil
			},
		})
		if toolErr != nil {
			return amdDetection{}, toolErr
		}
		entries = append(entries, postpone)
	}
	tools, err := llm.NewToolContext(entries...)
	if err != nil {
		return amdDetection{}, err
	}
	chat := llm.EmptyChatContext()
	if _, err := chat.AddMessage(llm.RoleSystem, a.opts.prompt); err != nil {
		return amdDetection{}, err
	}
	if _, err := chat.AddMessage(llm.RoleUser, transcript); err != nil {
		return amdDetection{}, err
	}

	var modelStream llm.LLMStream
	err = guardAMD("LLM chat", func() (chatErr error) {
		modelStream, chatErr = a.llm.Chat(ctx, llm.ChatOptions{
			ChatContext: chat, ToolContext: tools,
			ToolChoice: llm.ToolChoice{Kind: llm.ToolChoiceRequired},
		})
		return chatErr
	})
	if err != nil {
		return amdDetection{}, err
	}
	if modelStream == nil {
		return amdDetection{}, errors.New("AMD LLM returned a nil stream")
	}
	if !run.setLLMStream(modelStream) {
		_ = modelStream.Close()
		return amdDetection{}, context.Cause(ctx)
	}
	defer func() {
		run.clearLLMStream(modelStream)
		_ = modelStream.Close()
	}()

	var content strings.Builder
	var calls []*llm.FunctionCall
	for {
		var chunk llm.ChatChunk
		recvErr := guardAMD("LLM stream receive", func() (err error) {
			chunk, err = modelStream.Recv(ctx)
			return err
		})
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				break
			}
			return amdDetection{}, recvErr
		}
		if chunk.Delta == nil {
			continue
		}
		content.WriteString(chunk.Delta.Content)
		calls = append(calls, chunk.Delta.ToolCalls...)
	}
	raw := content.String()
	result := amdDetection{
		category: AMDCategoryUncertain, reason: "llm", raw: raw,
		transcript: transcript, toolCalls: len(calls) > 0,
		speechMS: ceilMilliseconds(speechDuration), delayMS: ceilMilliseconds(delay),
	}
	for _, call := range calls {
		if call == nil {
			continue
		}
		switch call.Name {
		case "save_prediction":
			var input amdSavePredictionInput
			if json.Unmarshal([]byte(call.Arguments), &input) != nil {
				continue
			}
			if !isAMDCategory(input.Label) {
				input.Label = AMDCategoryUncertain
			}
			if input.Label != AMDCategoryUncertain {
				result.category = input.Label
				result.saved = true
			}
		case "postpone_termination":
			if extensionCount+len(result.extensions) >= DefaultAMDMaxExtensions {
				continue
			}
			var input amdPostponeInput
			if json.Unmarshal([]byte(call.Arguments), &input) != nil {
				input.Seconds = 0
			}
			milliseconds := input.Seconds * float64(time.Second/time.Millisecond)
			if math.IsNaN(milliseconds) || math.IsInf(milliseconds, 0) {
				milliseconds = 0
			}
			milliseconds = min(max(milliseconds, 0), float64(DefaultAMDMaxExtension/time.Millisecond))
			result.extensions = append(result.extensions, time.Duration(milliseconds)*time.Millisecond)
		}
	}
	if result.saved || result.toolCalls {
		return result, nil
	}
	category, reason := parseAMDDetection(raw)
	result.category, result.reason = category, reason
	return result, nil
}

func parseAMDDetection(raw string) (AMDCategory, string) {
	normalized := strings.TrimSpace(raw)
	chunk := normalized
	start, end := strings.Index(normalized, "{"), strings.LastIndex(normalized, "}")
	if start >= 0 && end >= start {
		chunk = normalized[start : end+1]
	}
	var value struct {
		Category AMDCategory `json:"category"`
		Reason   string      `json:"reason"`
	}
	if err := json.Unmarshal([]byte(chunk), &value); err == nil {
		if !isAMDCategory(value.Category) {
			value.Category = AMDCategoryUncertain
		}
		reason := strings.TrimSpace(value.Reason)
		if reason == "" {
			reason = "No reason provided."
		}
		return value.Category, reason
	}
	if normalized == "" {
		normalized = "Failed to parse AMD model response."
	}
	return AMDCategoryUncertain, normalized
}

func (r *amdRun) setLLMStream(value llm.LLMStream) bool {
	r.resourceMu.Lock()
	defer r.resourceMu.Unlock()
	if r.stopped {
		return false
	}
	r.llmStream = value
	return true
}

func (r *amdRun) clearLLMStream(value llm.LLMStream) {
	r.resourceMu.Lock()
	if r.llmStream == value {
		r.llmStream = nil
	}
	r.resourceMu.Unlock()
}

func (r *amdRun) closeLLMStream() {
	r.resourceMu.Lock()
	value := r.llmStream
	r.llmStream = nil
	r.resourceMu.Unlock()
	if value != nil {
		_ = guardAMD("close LLM stream", value.Close)
	}
}

func (r *amdRun) closeResources() {
	r.resourceMu.Lock()
	if r.stopped {
		r.resourceMu.Unlock()
		return
	}
	r.stopped = true
	modelStream, speechStream, audio := r.llmStream, r.sttStream, r.audio
	r.llmStream, r.sttStream, r.audio = nil, nil, nil
	subscription := r.subscription
	r.subscription = nil
	r.resourceMu.Unlock()
	if modelStream != nil {
		_ = guardAMD("close LLM stream", modelStream.Close)
	}
	if speechStream != nil {
		_ = guardAMD("close STT stream", speechStream.Close)
	}
	if audio != nil {
		_ = guardAMD("close AMD audio subscription", audio.Close)
	}
	if subscription != nil {
		_ = subscription.Close()
	}
}

func guardAMDValue[T any](operation string, callback func() (T, error)) (value T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &AMDPanicError{Operation: operation, Value: recovered, Stack: debug.Stack()}
		}
	}()
	return callback()
}
