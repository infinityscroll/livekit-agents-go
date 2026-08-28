// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/tts"
)

const maxGeneratedTextBytes = 4 << 20

type generationResult struct {
	err     error
	handoff any
}

type generationSegment struct {
	text     *stream.Channel[string]
	audio    stream.Reader[agents.AudioFrame]
	realtime *realtimeSegment
	played   chan segmentPlayback
	timedMu  sync.Mutex
	timed    []agents.TimedString
}

type realtimeSegment struct {
	messageID  string
	responseID string
	modalities []llm.Modality
}

type segmentPlayback struct {
	spoken   string
	playable bool
	started  time.Time
	playback PlaybackFinishedEvent
	err      error
}

func newGenerationSegment() *generationSegment {
	return &generationSegment{text: stream.NewChannel[string](16), played: make(chan segmentPlayback, 1)}
}

func (s *generationSegment) appendTimed(value agents.TimedString) {
	s.timedMu.Lock()
	s.timed = append(s.timed, value)
	s.timedMu.Unlock()
}

func (s *generationSegment) timedSnapshot() []agents.TimedString {
	s.timedMu.Lock()
	values := append([]agents.TimedString(nil), s.timed...)
	s.timedMu.Unlock()
	return values
}

func (a *agentActivity[UserData]) generationWorker() {
	defer a.workersWG.Done()
	for {
		job, err := a.generation.Recv(a.ctx)
		if err != nil {
			return
		}
		if job.handle.Interrupted() {
			_ = job.segments.Close()
			job.setResult(generationResult{err: context.Canceled})
			continue
		}
		jobCtx, release := speechContext(a.ctx, job.handle)
		result := a.prepareGenerationGuarded(jobCtx, job)
		release()
		_ = job.segments.Close()
		job.setResult(result)
	}
}

func (a *agentActivity[UserData]) prepareGenerationGuarded(ctx context.Context, job *speechJob) (result generationResult) {
	err := guardedActivityCall("reply generation", func() error {
		result = a.prepareGeneration(ctx, job)
		return result.err
	})
	if err != nil {
		result.err = err
	}
	return result
}

func (a *agentActivity[UserData]) prepareGeneration(ctx context.Context, job *speechJob) generationResult {
	options := job.reply
	working := options.ChatContext
	if working == nil {
		working = a.session.ChatContext()
	} else {
		working = working.Copy(llm.CopyOptions{})
	}

	var userMessage *llm.ChatMessage
	switch {
	case options.UserMessage != nil:
		userMessage = options.UserMessage.Clone()
	case options.UserInput != "":
		userMessage = llm.NewChatMessage(llm.RoleUser, options.UserInput)
	}
	if userMessage != nil {
		if err := working.Insert(userMessage); err != nil {
			return generationResult{err: fmt.Errorf("insert generated-reply user message: %w", err)}
		}
		if !job.preemptive {
			a.session.commitItems(job.handle, userMessage)
			job.prepMu.Lock()
			job.userCommitted = true
			job.prepMu.Unlock()
		}
		if err := a.agent.runOnUserTurnCompleted(ctx, working, userMessage); err != nil {
			if IsStopResponse(err) {
				return generationResult{}
			}
			return generationResult{err: fmt.Errorf("agent %q onUserTurnCompleted: %w", a.agent.ID(), err)}
		}
	}

	tools := a.toolsSnapshot()
	toolChoice := options.ToolChoice
	if toolChoice.Kind == "" {
		toolChoice = llm.ToolChoice{Kind: llm.ToolChoiceAuto}
	}
	if realtime := a.realtimeSnapshot(); realtime != nil {
		// Base instructions are already installed on the realtime session. Extra
		// reply instructions belong only to response.create; inserting either into
		// synced chat duplicates prompting and persists a one-shot instruction.
		return a.prepareRealtimeGeneration(ctx, job, realtime, working, tools, toolChoice)
	}
	instructions := a.instructionsSnapshot()
	if options.Instructions != nil {
		instructions = instructions.Concat(*options.Instructions)
	}
	if instructions.Value() != "" {
		message := llm.NewChatMessage(llm.RoleDeveloper, "")
		message.Content = []llm.Content{llm.InstructionContent{Instructions: instructions.AsModality(toLLMModality(job.handle.InputDetails().Modality))}}
		if err := working.Insert(message); err != nil {
			return generationResult{err: fmt.Errorf("insert agent instructions: %w", err)}
		}
	}
	for step := 0; step <= a.session.opts.maxToolSteps; step++ {
		segment := newGenerationSegment()
		if err := job.segments.Send(ctx, segment); err != nil {
			return generationResult{err: err}
		}
		segmentFinished := false
		commitPlayback := func(playback segmentPlayback, generationErr error) {
			if strings.TrimSpace(playback.spoken) == "" {
				return
			}
			assistant := llm.NewChatMessage(llm.RoleAssistant, playback.spoken)
			assistant.Interrupted = job.handle.Interrupted() || playback.err != nil || generationErr != nil
			if !playback.started.IsZero() {
				assistant.CreatedAt = playback.started
				assistant.Metrics.StartedSpeakingAt = playback.started
			}
			_ = working.Insert(assistant)
			a.session.commitItems(job.handle, assistant)
		}
		finishSegment := func(generationErr error) error {
			if segmentFinished {
				return nil
			}
			segmentFinished = true
			if generationErr != nil {
				_ = segment.text.Abort(generationErr)
			} else {
				_ = segment.text.Close()
			}
			playback, waitErr := a.awaitSegmentPlayback(ctx, job, segment)
			if waitErr != nil {
				return errors.Join(generationErr, waitErr)
			}
			commitPlayback(playback, generationErr)
			return playback.err
		}
		sendText := func(text string) error { return segment.text.Send(ctx, text) }
		flushText := func() error {
			if err := finishSegment(nil); err != nil {
				return err
			}
			next := newGenerationSegment()
			if err := job.segments.Send(ctx, next); err != nil {
				return err
			}
			segment, segmentFinished = next, false
			return nil
		}
		response, err := a.runLLMStep(ctx, working, tools, ModelSettings{ToolChoice: toolChoice}, sendText, flushText)
		playbackErr := finishSegment(err)
		if IsStopResponse(err) {
			return generationResult{}
		}
		if err != nil {
			return generationResult{err: errors.Join(err, playbackErr)}
		}
		if playbackErr != nil {
			return generationResult{err: playbackErr}
		}
		if len(response.calls) == 0 {
			return generationResult{}
		}
		if step == a.session.opts.maxToolSteps {
			return generationResult{err: fmt.Errorf("maximum tool steps (%d) exceeded", a.session.opts.maxToolSteps)}
		}

		commit := make([]llm.ChatItem, 0, len(response.calls)*2)
		for _, call := range response.calls {
			commit = append(commit, call)
			_ = working.Insert(call)
		}
		// Preemptive/paused replies may speculate on LLM text, but tool side
		// effects are never allowed before the reply is authorized.
		if err := job.handle.WaitForAuthorization(ctx); err != nil {
			return generationResult{err: err}
		}
		results, err := a.executeFunctionTools(ctx, job.handle, response.calls, tools)
		if err != nil {
			return generationResult{err: fmt.Errorf("execute function tools: %w", err)}
		}
		calls := make([]*llm.FunctionCall, 0, len(results))
		outputs := make([]*llm.FunctionCallOutput, 0, len(results))
		var handoff any
		for _, result := range results {
			if result.Call != nil {
				calls = append(calls, result.Call)
				working.Remove(result.Call.ItemID())
				_ = working.Insert(result.Call)
			}
			if result.Output != nil {
				if target, returns, ok := handoffValue(result.Value); ok {
					handoff = target
					if encoded, encodeErr := encodeHandoffReturn(returns); encodeErr == nil {
						result.Output.Output = encoded
					}
				}
				outputs = append(outputs, result.Output)
				commit = append(commit, result.Output)
				_ = working.Insert(result.Output)
			}
		}
		a.session.commitItems(job.handle, commit...)
		_ = a.session.events.Publish(a.ctx, FunctionToolsExecutedEvent{
			EventBase: newEventBase(EventFunctionToolsExecuted, time.Now()), FunctionCalls: calls, FunctionCallOutputs: outputs,
		})
		if handoff != nil {
			return generationResult{handoff: handoff}
		}
		toolChoice = llm.ToolChoice{Kind: llm.ToolChoiceAuto}
	}
	return generationResult{err: errors.New("unreachable generation state")}
}

func (a *agentActivity[UserData]) enqueueRealtimeGeneration(event llm.GenerationCreatedEvent) error {
	if event.UserInitiated {
		return nil
	}
	handle := NewSpeechHandle(SpeechHandleOptions{
		AllowInterruptions: true, AllowInterruptionsSet: true,
		InputDetails: InputDetails{Modality: InputModalityAudio},
	})
	eventCopy := event
	job := &speechJob{
		kind: speechJobReply, source: SpeechSourceGenerateReply, handle: handle,
		realtimeEvent: &eventCopy, ready: make(chan struct{}), segments: stream.NewChannel[*generationSegment](2),
	}
	a.mu.RLock()
	paused := a.authPaused != 0
	a.mu.RUnlock()
	if !paused {
		handle.AuthorizeGeneration()
	}
	if err := a.acceptJob(a.ctx, job, SpeechPriorityNormal, false); err != nil {
		return err
	}
	if err := a.generation.Send(a.ctx, job); err != nil {
		_ = job.segments.Abort(err)
		job.setResult(generationResult{err: err})
		a.finishJob(job, err)
		return err
	}
	return nil
}

type realtimeFunctionResult struct {
	calls []*llm.FunctionCall
	err   error
}

func (a *agentActivity[UserData]) prepareRealtimeGeneration(
	ctx context.Context,
	job *speechJob,
	realtime llm.RealtimeSession,
	working *llm.ChatContext,
	tools *llm.Context,
	toolChoice llm.ToolChoice,
) (result generationResult) {
	model := a.modelsSnapshot().realtime
	if model == nil {
		return generationResult{err: errors.New("realtime session has no selected model")}
	}
	var event llm.GenerationCreatedEvent
	resetToolChoice := false
	defer func() {
		if !resetToolChoice {
			return
		}
		resetCtx, cancel := context.WithTimeout(a.ctx, a.session.opts.shutdownTimeout)
		defer cancel()
		if err := realtime.UpdateOptions(resetCtx, llm.RealtimeUpdateOptions{ToolChoice: agents.Disable[llm.ToolChoice]()}); err != nil && context.Cause(a.ctx) == nil {
			result.err = errors.Join(result.err, fmt.Errorf("reset realtime tool choice: %w", err))
		}
	}()
	if job.realtimeEvent != nil {
		event = *job.realtimeEvent
	} else {
		requiresChatSync := job.reply.ChatContext != nil || job.reply.UserMessage != nil || job.reply.UserInput != ""
		if requiresChatSync && !realtimeChatEquivalent(realtime.ChatContext(), working) {
			if !model.Capabilities().MidSessionChatContextUpdate {
				return generationResult{err: errors.New("realtime model does not support the chat-context update required for this reply")}
			}
			if err := realtime.UpdateChatContext(ctx, working); err != nil {
				return generationResult{err: fmt.Errorf("sync realtime chat context: %w", err)}
			}
		}
		if toolChoice.Kind != "" && toolChoice.Kind != llm.ToolChoiceAuto {
			if !model.Capabilities().PerResponseToolChoice {
				return generationResult{err: errors.New("realtime model does not support per-response tool choice")}
			}
			if err := realtime.UpdateOptions(ctx, llm.RealtimeUpdateOptions{ToolChoice: agents.Use(toolChoice)}); err != nil {
				return generationResult{err: fmt.Errorf("update realtime tool choice: %w", err)}
			}
			resetToolChoice = true
		}
		instructions := ""
		if job.reply.Instructions != nil {
			instructions = job.reply.Instructions.Value()
		}
		var err error
		event, err = realtime.GenerateReply(ctx, llm.GenerateRealtimeReplyOptions{Instructions: instructions})
		if err != nil {
			return generationResult{err: fmt.Errorf("generate realtime reply: %w", err)}
		}
	}

	for step := 0; step <= a.session.opts.maxToolSteps; step++ {
		if event.MessageStream == nil {
			return generationResult{err: errors.New("realtime generation returned a nil message stream")}
		}
		callsDone := make(chan realtimeFunctionResult, 1)
		go collectRealtimeFunctions(ctx, event.FunctionStream, callsDone)

		for {
			message, err := event.MessageStream.Recv(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return generationResult{err: fmt.Errorf("read realtime messages: %w", err)}
			}
			segment := newGenerationSegment()
			modalities, err := realtimeModalities(ctx, message)
			if err != nil {
				return generationResult{err: err}
			}
			segment.realtime = &realtimeSegment{messageID: message.MessageID, responseID: event.ResponseID, modalities: modalities}
			if slices.Contains(modalities, llm.ModalityAudio) {
				if message.AudioStream == nil {
					return generationResult{err: errors.New("realtime audio modality returned a nil audio stream")}
				}
				segment.audio = message.AudioStream
			}
			if err := job.segments.Send(ctx, segment); err != nil {
				return generationResult{err: err}
			}
			textErr := forwardRealtimeText(ctx, message.TextStream, segment, slices.Contains(modalities, llm.ModalityText))
			if textErr != nil {
				_ = segment.text.Abort(textErr)
			} else {
				_ = segment.text.Close()
			}
			playback, playbackWaitErr := a.awaitSegmentPlayback(ctx, job, segment)
			if playbackWaitErr != nil {
				return generationResult{err: errors.Join(textErr, playbackWaitErr)}
			}
			if strings.TrimSpace(playback.spoken) != "" {
				assistant := llm.NewChatMessage(llm.RoleAssistant, playback.spoken)
				if message.MessageID != "" {
					assistant.ID = message.MessageID
				}
				assistant.Interrupted = job.handle.Interrupted() || playback.err != nil
				if event.ResponseID != "" {
					assistant.Metrics.ProviderRequestIDs = []string{event.ResponseID}
				}
				if !playback.started.IsZero() {
					assistant.CreatedAt = playback.started
					assistant.Metrics.StartedSpeakingAt = playback.started
				}
				_ = working.Insert(assistant)
				a.session.commitItems(job.handle, assistant)
			}
			if job.handle.Interrupted() && playback.playable && model.Capabilities().MessageTruncation {
				if err := realtime.Truncate(a.ctx, llm.TruncateRealtimeMessageOptions{
					MessageID: message.MessageID, AudioEnd: max(0, playback.playback.PlaybackPosition),
					Modalities: llm.CloneModalities(modalities), AudioTranscript: playback.spoken,
				}); err != nil && context.Cause(a.ctx) == nil {
					a.session.emitError(fmt.Errorf("truncate realtime message: %w", err), realtime)
				}
			}
			if textErr != nil {
				return generationResult{err: textErr}
			}
			if playback.err != nil {
				return generationResult{err: playback.err}
			}
		}

		var functionResult realtimeFunctionResult
		select {
		case <-ctx.Done():
			return generationResult{err: context.Cause(ctx)}
		case functionResult = <-callsDone:
		}
		if functionResult.err != nil {
			return generationResult{err: functionResult.err}
		}
		if len(functionResult.calls) == 0 {
			return generationResult{}
		}
		if step == a.session.opts.maxToolSteps {
			return generationResult{err: fmt.Errorf("maximum tool steps (%d) exceeded", a.session.opts.maxToolSteps)}
		}
		for _, call := range functionResult.calls {
			_ = working.Insert(call)
		}
		results, err := a.executeFunctionTools(ctx, job.handle, functionResult.calls, tools)
		if err != nil {
			return generationResult{err: fmt.Errorf("execute realtime function tools: %w", err)}
		}
		calls, outputs := make([]*llm.FunctionCall, 0, len(results)), make([]*llm.FunctionCallOutput, 0, len(results))
		var handoff any
		for _, result := range results {
			if result.Call != nil {
				calls = append(calls, result.Call)
				working.Remove(result.Call.ItemID())
				_ = working.Insert(result.Call)
			}
			if result.Output != nil {
				if target, returns, ok := handoffValue(result.Value); ok {
					handoff = target
					if encoded, encodeErr := encodeHandoffReturn(returns); encodeErr == nil {
						result.Output.Output = encoded
					}
				}
				outputs = append(outputs, result.Output)
				_ = working.Insert(result.Output)
			}
		}
		a.session.commitItems(job.handle, chatItemsFromCalls(calls)...)
		a.session.commitItems(job.handle, chatItemsFromOutputs(outputs)...)
		_ = a.session.events.Publish(a.ctx, FunctionToolsExecutedEvent{
			EventBase: newEventBase(EventFunctionToolsExecuted, time.Now()), FunctionCalls: calls, FunctionCallOutputs: outputs,
		})
		if !model.Capabilities().MidSessionChatContextUpdate {
			return generationResult{err: errors.New("realtime model does not support syncing function outputs into chat context")}
		}
		if err := realtime.UpdateChatContext(ctx, working); err != nil {
			return generationResult{err: fmt.Errorf("sync realtime tool outputs: %w", err)}
		}
		if handoff != nil {
			return generationResult{handoff: handoff}
		}
		if model.Capabilities().AutoToolReplyGeneration {
			return generationResult{}
		}
		event, err = realtime.GenerateReply(ctx, llm.GenerateRealtimeReplyOptions{})
		if err != nil {
			return generationResult{err: fmt.Errorf("generate realtime tool reply: %w", err)}
		}
	}
	return generationResult{err: errors.New("unreachable realtime generation state")}
}

func (a *agentActivity[UserData]) awaitSegmentPlayback(ctx context.Context, job *speechJob, segment *generationSegment) (segmentPlayback, error) {
	select {
	case playback := <-segment.played:
		return playback, nil
	case <-ctx.Done():
		if !job.handle.Interrupted() {
			return segmentPlayback{}, context.Cause(ctx)
		}
	}
	// Interruption cancels both generation and playout contexts. The playout
	// side still sends a buffered final acknowledgment containing the partial
	// transcript and playback position needed for history and truncation.
	cleanupCtx, cancel := context.WithTimeout(a.ctx, a.session.opts.shutdownTimeout)
	defer cancel()
	select {
	case playback := <-segment.played:
		return playback, nil
	case <-cleanupCtx.Done():
		return segmentPlayback{}, context.Cause(cleanupCtx)
	}
}

func realtimeModalities(ctx context.Context, message llm.MessageGeneration) ([]llm.Modality, error) {
	if message.Modalities == nil {
		modalities := make([]llm.Modality, 0, 2)
		if message.TextStream != nil {
			modalities = append(modalities, llm.ModalityText)
		}
		if message.AudioStream != nil {
			modalities = append(modalities, llm.ModalityAudio)
		}
		return modalities, nil
	}
	modalities, err := message.Modalities(ctx)
	if err != nil {
		return nil, fmt.Errorf("read realtime message modalities: %w", err)
	}
	return llm.CloneModalities(modalities), nil
}

func forwardRealtimeText(ctx context.Context, source stream.Reader[llm.RealtimeText], segment *generationSegment, required bool) error {
	if source == nil {
		if required {
			return errors.New("realtime text modality returned a nil text stream")
		}
		return nil
	}
	for {
		value, err := source.Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if value.Text != "" {
			if value.Timed != nil {
				segment.appendTimed(*value.Timed)
			}
			if err := segment.text.Send(ctx, value.Text); err != nil {
				return err
			}
		}
	}
}

func collectRealtimeFunctions(ctx context.Context, source stream.Reader[*llm.FunctionCall], result chan<- realtimeFunctionResult) {
	value := realtimeFunctionResult{}
	if source == nil {
		result <- value
		return
	}
	for {
		call, err := source.Recv(ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				value.err = fmt.Errorf("read realtime function calls: %w", err)
			}
			result <- value
			return
		}
		if call != nil {
			value.calls = append(value.calls, call.Clone())
		}
	}
}

func chatItemsFromCalls(calls []*llm.FunctionCall) []llm.ChatItem {
	items := make([]llm.ChatItem, len(calls))
	for index, call := range calls {
		items[index] = call
	}
	return items
}

func chatItemsFromOutputs(outputs []*llm.FunctionCallOutput) []llm.ChatItem {
	items := make([]llm.ChatItem, len(outputs))
	for index, output := range outputs {
		items[index] = output
	}
	return items
}

func toLLMModality(modality InputModality) llm.Modality {
	if modality == InputModalityText {
		return llm.ModalityText
	}
	return llm.ModalityAudio
}

type llmStepResult struct {
	calls []*llm.FunctionCall
}

func (a *agentActivity[UserData]) runLLMStep(
	ctx context.Context,
	chat *llm.ChatContext,
	tools *llm.Context,
	settings ModelSettings,
	sendText func(string) error,
	flushText func() error,
) (llmStepResult, error) {
	var reader stream.Reader[LLMNodeItem]
	custom, used, err := a.agent.runLLMNode(ctx, chat.Copy(llm.CopyOptions{}), tools.Copy(), settings)
	if err != nil {
		return llmStepResult{}, err
	}
	if used {
		reader = custom
	} else {
		model := a.modelsSnapshot().llm
		if model == nil {
			return llmStepResult{}, errors.New("generate reply requires an LLM or Agent.LLMNode hook")
		}
		modelStream, err := model.Chat(ctx, llm.ChatOptions{
			ChatContext: chat, ToolContext: tools, ConnectOptions: a.session.opts.connect.LLM,
			ParallelToolCalls: true, ToolChoice: settings.ToolChoice,
		})
		if err != nil {
			return llmStepResult{}, err
		}
		defer modelStream.Close()
		reader = &llmNodeStream{source: modelStream}
	}

	var generatedBytes int
	result := llmStepResult{}
	for {
		item, err := reader.Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return result, nil
			}
			return llmStepResult{}, err
		}
		if item.Chunk != nil && item.Chunk.Delta != nil {
			delta := item.Chunk.Delta
			if delta.Content != "" {
				if generatedBytes+len(delta.Content) > maxGeneratedTextBytes {
					return llmStepResult{}, errors.New("LLM response exceeds the 4 MiB text limit")
				}
				generatedBytes += len(delta.Content)
				if err := sendText(delta.Content); err != nil {
					return llmStepResult{}, err
				}
			}
			for _, call := range delta.ToolCalls {
				if call != nil {
					result.calls = append(result.calls, call.Clone())
				}
			}
		}
		if item.Text != "" {
			if generatedBytes+len(item.Text) > maxGeneratedTextBytes {
				return llmStepResult{}, errors.New("LLM response exceeds the 4 MiB text limit")
			}
			generatedBytes += len(item.Text)
			if err := sendText(item.Text); err != nil {
				return llmStepResult{}, err
			}
		}
		if item.Flush {
			if err := flushText(); err != nil {
				return llmStepResult{}, err
			}
		}
	}
}

type llmNodeStream struct{ source llm.LLMStream }

func (s *llmNodeStream) Recv(ctx context.Context) (LLMNodeItem, error) {
	chunk, err := s.source.Recv(ctx)
	if err != nil {
		return LLMNodeItem{}, err
	}
	return LLMChunkItem(chunk), nil
}

func handoffValue(value any) (target, returns any, ok bool) {
	switch handoff := value.(type) {
	case llm.AgentHandoff:
		return handoff.Agent, handoff.Returns, true
	case *llm.AgentHandoff:
		if handoff != nil {
			return handoff.Agent, handoff.Returns, true
		}
	}
	return nil, nil, false
}

func encodeHandoffReturn(value any) (string, error) {
	if value == nil {
		return "", nil
	}
	if text, ok := value.(string); ok {
		return text, nil
	}
	encoded, err := json.Marshal(value)
	return string(encoded), err
}

func (a *agentActivity[UserData]) playJob(ctx context.Context, job *speechJob) error {
	if job.kind == speechJobReply {
		a.session.setAgentState(AgentStateThinking)
		if err := job.handle.WaitForAuthorization(ctx); err != nil {
			a.rejectGenerationSegments(job, err)
			return err
		}
		if err := a.commitReplyUser(job); err != nil {
			return err
		}
		var playbackErr error
		for {
			segment, err := job.segments.Recv(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				playbackErr = err
				break
			}
			playback := a.playGenerationSegment(ctx, segment)
			segment.played <- playback
			if playback.err != nil {
				playbackErr = playback.err
				break
			}
		}
		if err := a.awaitGenerationReady(ctx, job); err != nil {
			return errors.Join(playbackErr, err)
		}
		result := job.getResult()
		_ = job.handle.MarkGenerationDone()
		if result.err != nil {
			return errors.Join(playbackErr, result.err)
		}
		if playbackErr != nil {
			return playbackErr
		}
		if result.handoff != nil {
			target, ok := result.handoff.(*Agent[UserData])
			if !ok {
				return fmt.Errorf("tool handoff target has type %T, want *voice.Agent", result.handoff)
			}
			return a.session.scheduleHandoff(ctx, target)
		}
		return nil
	}

	spoken, playable, _, err := a.performSpeech(ctx, job.text, job.audio, true)
	addToChat := true
	if job.say.AddToChatContext != nil {
		addToChat = *job.say.AddToChatContext
	}
	if addToChat && playable && strings.TrimSpace(spoken) != "" {
		message := llm.NewChatMessage(llm.RoleAssistant, spoken)
		message.Interrupted = job.handle.Interrupted() || err != nil
		a.session.commitItems(job.handle, message)
	}
	return err
}

func (a *agentActivity[UserData]) awaitGenerationReady(ctx context.Context, job *speechJob) error {
	select {
	case <-job.ready:
		return nil
	case <-ctx.Done():
		if !job.handle.Interrupted() {
			return context.Cause(ctx)
		}
	}
	cleanupCtx, cancel := context.WithTimeout(a.ctx, a.session.opts.shutdownTimeout)
	defer cancel()
	select {
	case <-job.ready:
		return nil
	case <-cleanupCtx.Done():
		return context.Cause(cleanupCtx)
	}
}

func (a *agentActivity[UserData]) rejectGenerationSegments(job *speechJob, cause error) {
	cleanupCtx, cancel := context.WithTimeout(a.ctx, a.session.opts.shutdownTimeout)
	defer cancel()
	for {
		segment, err := job.segments.Recv(cleanupCtx)
		if err != nil {
			return
		}
		segment.played <- segmentPlayback{err: cause}
	}
}

func (a *agentActivity[UserData]) playGenerationSegment(ctx context.Context, segment *generationSegment) (result segmentPlayback) {
	var output AudioOutput
	var capturedBefore uint64
	if segment.realtime != nil && a.session.output.AudioEnabled() {
		output = a.session.output.Audio()
		if output != nil {
			capturedBefore = output.CapturedPlayoutSegments()
		}
	}
	err := guardedActivityCall("generation segment playout", func() error {
		result.spoken, result.playable, result.started, result.err = a.performSpeech(ctx, segment.text, segment.audio, segment.realtime == nil)
		return result.err
	})
	if err != nil {
		result.err = err
	}
	timed := segment.timedSnapshot()
	if output != nil && output.CapturedPlayoutSegments() > capturedBefore {
		waitCtx, cancel := context.WithTimeout(a.ctx, a.session.opts.shutdownTimeout)
		playback, waitErr := output.WaitForPlayout(waitCtx)
		cancel()
		if waitErr == nil {
			result.playback = playback
			if playback.SynchronizedTranscript != nil {
				result.spoken = strings.TrimSpace(*playback.SynchronizedTranscript)
			} else if playback.Interrupted && segment.realtime != nil && slices.Contains(segment.realtime.modalities, llm.ModalityAudio) {
				// The text stream may run far ahead of audio. Commit only timing entries
				// known to have completed by the heard playback position. When the
				// provider supplies neither synchronized nor timed text, an empty partial
				// is safer than poisoning history with an unheard tail.
				timed, result.spoken = timedTranscriptThrough(timed, playback.PlaybackPosition)
			}
		} else if result.err == nil {
			result.err = waitErr
		}
	}
	if segment.realtime != nil {
		var transcriptErr error
		switch {
		case result.playback.SynchronizedTranscript != nil:
			transcriptErr = a.emitPlainTranscription(ctx, result.spoken)
		case len(timed) != 0:
			transcriptErr = a.emitTimedTranscription(ctx, timed)
		default:
			transcriptErr = a.emitPlainTranscription(ctx, result.spoken)
		}
		result.err = errors.Join(result.err, transcriptErr)
	}
	return result
}

func timedTranscriptThrough(values []agents.TimedString, position time.Duration) ([]agents.TimedString, string) {
	heard := make([]agents.TimedString, 0, len(values))
	var text strings.Builder
	for _, value := range values {
		if value.EndTime == nil || *value.EndTime > position {
			continue
		}
		heard = append(heard, value)
		text.WriteString(value.Text)
	}
	return heard, strings.TrimSpace(text.String())
}

func (a *agentActivity[UserData]) commitReplyUser(job *speechJob) error {
	job.prepMu.Lock()
	if job.userCommitted {
		job.prepMu.Unlock()
		return nil
	}
	job.userCommitted = true
	job.prepMu.Unlock()
	var message *llm.ChatMessage
	if job.reply.UserMessage != nil {
		message = job.reply.UserMessage.Clone()
	} else if job.reply.UserInput != "" {
		message = llm.NewChatMessage(llm.RoleUser, job.reply.UserInput)
	}
	if message != nil {
		a.session.commitItems(job.handle, message)
	}
	return nil
}

// performSpeech consumes text exactly once, forwards optional supplied audio or
// synthesized audio with bounded idle reads, and reports whether any text/audio
// became externally observable (and is therefore safe to commit to history).
func (a *agentActivity[UserData]) performSpeech(ctx context.Context, text stream.Reader[string], suppliedAudio stream.Reader[agents.AudioFrame], emitTranscript bool) (string, bool, time.Time, error) {
	if suppliedAudio != nil {
		recorder := newRecordingTextReader(text)
		childCtx, childCancel := context.WithCancelCause(ctx)
		defer childCancel(nil)
		textDone := make(chan error, 1)
		go func() {
			for {
				_, err := recorder.Recv(childCtx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}
					textDone <- err
					return
				}
			}
		}()
		played, started, err := a.forwardAudio(childCtx, suppliedAudio)
		// Audio and transcript streams are independent for realtime providers.
		// Keep consuming the bounded text stream after audio EOF; cancel it only
		// on an actual playout failure. Otherwise the producer can deadlock once
		// the text buffer fills while waiting for this segment's acknowledgment.
		if err != nil {
			childCancel(err)
		}
		var textErr error
		select {
		case textErr = <-textDone:
		case <-ctx.Done():
			childCancel(context.Cause(ctx))
			textErr = context.Cause(ctx)
		}
		if errors.Is(textErr, io.EOF) {
			textErr = nil
		}
		spoken := recorder.Text()
		if textErr == nil && emitTranscript {
			textErr = a.emitPlainTranscription(ctx, spoken)
		}
		if started.IsZero() {
			started = recorder.FirstAt()
		}
		return spoken, played || spoken != "", started, errors.Join(err, textErr)
	}

	recorder := newRecordingTextReader(text)
	models := a.modelsSnapshot()
	customAudio, used, err := a.agent.runTTSNode(ctx, recorder, ModelSettings{})
	if err != nil {
		return recorder.Text(), false, recorder.FirstAt(), err
	}
	if used {
		played, started, playErr := a.forwardAudio(ctx, customAudio)
		spoken := recorder.Text()
		if !emitTranscript {
			return spoken, played || spoken != "", started, playErr
		}
		return spoken, played || spoken != "", started, errors.Join(playErr, a.emitPlainTranscription(ctx, spoken))
	}
	if models.tts == nil {
		if err := drainText(ctx, recorder); err != nil {
			return recorder.Text(), false, recorder.FirstAt(), err
		}
		spoken := recorder.Text()
		if !emitTranscript {
			return spoken, spoken != "", recorder.FirstAt(), nil
		}
		return spoken, spoken != "", recorder.FirstAt(), a.emitPlainTranscription(ctx, spoken)
	}

	played, started, aligned, err := a.synthesizeAndForward(ctx, models.tts, recorder)
	spoken := recorder.Text()
	var transcriptErr error
	if emitTranscript {
		if a.useAligned && len(aligned) != 0 {
			transcriptErr = a.emitTimedTranscription(ctx, aligned)
		} else {
			transcriptErr = a.emitPlainTranscription(ctx, spoken)
		}
	}
	return spoken, played || spoken != "", started, errors.Join(err, transcriptErr)
}

type recordingTextReader struct {
	source stream.Reader[string]
	mu     sync.Mutex
	parts  []string
	bytes  int
	first  time.Time
}

func newRecordingTextReader(source stream.Reader[string]) *recordingTextReader {
	return &recordingTextReader{source: source}
}

func (r *recordingTextReader) Recv(ctx context.Context) (string, error) {
	value, err := r.source.Recv(ctx)
	if err == nil && value != "" {
		r.mu.Lock()
		if r.bytes+len(value) > maxGeneratedTextBytes {
			r.mu.Unlock()
			return "", errors.New("speech text exceeds the 4 MiB limit")
		}
		r.parts = append(r.parts, value)
		r.bytes += len(value)
		if r.first.IsZero() {
			r.first = time.Now()
		}
		r.mu.Unlock()
	}
	return value, err
}

func (r *recordingTextReader) FirstAt() time.Time {
	r.mu.Lock()
	value := r.first
	r.mu.Unlock()
	return value
}

func (r *recordingTextReader) Text() string {
	r.mu.Lock()
	text := strings.TrimSpace(strings.Join(r.parts, ""))
	r.mu.Unlock()
	return text
}

func drainText(ctx context.Context, source stream.Reader[string]) error {
	for {
		_, err := source.Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (a *agentActivity[UserData]) synthesizeAndForward(ctx context.Context, model tts.TTS, text *recordingTextReader) (bool, time.Time, []agents.TimedString, error) {
	pipelineCtx, pipelineCancel := context.WithCancelCause(ctx)
	defer pipelineCancel(nil)
	var source stream.Reader[tts.SynthesizedAudio]
	var closeSource func() error
	var feederDone chan error
	if model.Capabilities().Streaming {
		synthesis, err := model.Stream(pipelineCtx, tts.StreamOptions{ConnectOptions: a.session.opts.connect.TTS})
		if err != nil {
			return false, time.Time{}, nil, err
		}
		source, closeSource = synthesis, synthesis.Close
		feederDone = make(chan error, 1)
		go func() {
			for {
				chunk, err := text.Recv(pipelineCtx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						err = synthesis.EndInput()
					}
					feederDone <- err
					return
				}
				if err := synthesis.PushText(pipelineCtx, chunk); err != nil {
					feederDone <- err
					return
				}
			}
		}()
	} else {
		if err := drainText(pipelineCtx, text); err != nil {
			return false, time.Time{}, nil, err
		}
		synthesis, err := model.Synthesize(pipelineCtx, text.Text(), tts.SynthesizeOptions{ConnectOptions: a.session.opts.connect.TTS})
		if err != nil {
			return false, time.Time{}, nil, err
		}
		source, closeSource = synthesis, synthesis.Close
	}
	var closeOnce sync.Once
	closePipeline := func() { closeOnce.Do(func() { _ = closeSource() }) }
	defer closePipeline()

	adapter := &synthesizedAudioReader{source: source, timeout: a.session.opts.ttsReadIdleTimeout}
	if feederDone == nil {
		played, started, playErr := a.forwardAudio(pipelineCtx, adapter)
		return played, started, adapter.Timed(), playErr
	}

	// PushText/EndInput failures must cancel an audio stream immediately. Running
	// the consumer concurrently avoids waiting for the audio idle timeout when a
	// provider stops accepting text before it closes its output stream.
	type audioResult struct {
		played  bool
		started time.Time
		err     error
	}
	audioDone := make(chan audioResult, 1)
	go func() {
		played, started, err := a.forwardAudio(pipelineCtx, adapter)
		audioDone <- audioResult{played: played, started: started, err: err}
	}()

	feederFinished := false
	var feederErr error
	var audio audioResult
	select {
	case audio = <-audioDone:
		pipelineCancel(audio.err)
		closePipeline()
		select {
		case feederErr = <-feederDone:
			feederFinished = true
		case <-ctx.Done():
			feederErr = context.Cause(ctx)
		}
	case feederErr = <-feederDone:
		feederFinished = true
		if feederErr != nil && !errors.Is(feederErr, io.EOF) {
			pipelineCancel(feederErr)
			closePipeline()
		}
		select {
		case audio = <-audioDone:
		case <-ctx.Done():
			pipelineCancel(context.Cause(ctx))
			closePipeline()
			audio.err = context.Cause(ctx)
		}
	case <-ctx.Done():
		feederErr = context.Cause(ctx)
		pipelineCancel(feederErr)
		closePipeline()
		audio = <-audioDone
	}
	if !feederFinished && feederErr == nil {
		feederErr = context.Cause(pipelineCtx)
	}
	if errors.Is(feederErr, context.Canceled) || errors.Is(feederErr, io.EOF) || errors.Is(feederErr, stream.ErrClosed) {
		feederErr = nil
	}
	return audio.played, audio.started, adapter.Timed(), errors.Join(audio.err, feederErr)
}

type synthesizedAudioReader struct {
	source  stream.Reader[tts.SynthesizedAudio]
	timeout time.Duration
	mu      sync.Mutex
	timed   []agents.TimedString
}

func (r *synthesizedAudioReader) Recv(ctx context.Context) (agents.AudioFrame, error) {
	readCtx := ctx
	cancel := func() {}
	if r.timeout > 0 {
		readCtx, cancel = context.WithTimeout(ctx, r.timeout)
	}
	defer cancel()
	value, err := r.source.Recv(readCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && context.Cause(ctx) == nil {
			return agents.AudioFrame{}, fmt.Errorf("TTS audio stream idle for %s: %w", r.timeout, err)
		}
		return agents.AudioFrame{}, err
	}
	if len(value.TimedTranscripts) != 0 {
		r.mu.Lock()
		r.timed = append(r.timed, value.TimedTranscripts...)
		r.mu.Unlock()
	}
	return value.Frame, nil
}

func (r *synthesizedAudioReader) Timed() []agents.TimedString {
	r.mu.Lock()
	result := append([]agents.TimedString(nil), r.timed...)
	r.mu.Unlock()
	return result
}

func (a *agentActivity[UserData]) forwardAudio(ctx context.Context, source stream.Reader[agents.AudioFrame]) (bool, time.Time, error) {
	if transformed, used, err := a.agent.runRealtimeAudioOutputNode(ctx, source, ModelSettings{}); err != nil {
		return false, time.Time{}, err
	} else if used {
		source = transformed
	}
	output := a.session.output.Audio()
	outputEnabled := a.session.output.AudioEnabled() && output != nil
	played := false
	started := false
	var startedAt time.Time
	defer func() {
		if started {
			a.endpointing.OnEndOfAgentSpeech(time.Now())
		}
	}()
	for {
		readCtx := ctx
		cancel := func() {}
		if a.session.opts.forwardAudioIdleTimeout > 0 {
			readCtx, cancel = context.WithTimeout(ctx, a.session.opts.forwardAudioIdleTimeout)
		}
		frame, err := source.Recv(readCtx)
		cancel()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, context.DeadlineExceeded) && context.Cause(ctx) == nil {
				return played, startedAt, fmt.Errorf("audio forwarding idle for %s: %w", a.session.opts.forwardAudioIdleTimeout, err)
			}
			return played, startedAt, err
		}
		if !started {
			started = true
			startedAt = time.Now()
			a.session.setAgentState(AgentStateSpeaking)
			a.endpointing.OnStartOfAgentSpeech(startedAt)
		}
		if outputEnabled {
			if err := output.CaptureFrame(ctx, frame); err != nil {
				return played, startedAt, err
			}
			played = true
		}
	}
	if outputEnabled && played {
		if err := output.Flush(ctx); err != nil {
			return played, startedAt, err
		}
		if _, err := output.WaitForPlayout(ctx); err != nil {
			return played, startedAt, err
		}
	}
	return played, startedAt, nil
}

func (a *agentActivity[UserData]) emitPlainTranscription(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	return a.emitTranscription(ctx, []TranscriptionNodeItem{TranscriptionTextItem(text)})
}

func (a *agentActivity[UserData]) emitTimedTranscription(ctx context.Context, values []agents.TimedString) error {
	items := make([]TranscriptionNodeItem, len(values))
	for index, value := range values {
		items[index] = TranscriptionTimedItem(value)
	}
	return a.emitTranscription(ctx, items)
}

func (a *agentActivity[UserData]) emitTranscription(ctx context.Context, values []TranscriptionNodeItem) error {
	output := a.session.output.Transcription()
	if output == nil || !a.session.output.TranscriptionEnabled() {
		return nil
	}
	reader, _, err := a.agent.runTranscriptionNode(ctx, stream.FromSlice(values), ModelSettings{})
	if err != nil {
		return err
	}
	for {
		item, err := reader.Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return output.Flush(ctx)
			}
			return err
		}
		value := agents.TimedString{Text: item.Text}
		if item.Timed != nil {
			value = *item.Timed
		}
		if value.Text != "" {
			if err := output.CaptureText(ctx, value); err != nil {
				return err
			}
		}
	}
}
