// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/livekit/agents-go/llm"
)

const DefaultRunContextUpdateCapacity = 64
const DefaultRunContextFillerCapacity = 8

var (
	ErrRunContextDetached       = errors.New("voice run context executor is detached")
	ErrRunContextUpdateOverflow = errors.New("voice run context update capacity exceeded")
)

type RunContextUpdateOptions = llm.RunContextUpdateOptions
type RunContextFillerOptions = llm.RunContextFillerOptions

// FillerContent is one lazily selected filler step. Exactly one of Text or
// Handle may be set; the zero value intentionally skips the step.
type FillerContent struct {
	Text   string
	Handle *SpeechHandle
}

type FillerSource func(context.Context, int) (FillerContent, error)

func TextFiller(text string) FillerSource {
	return func(context.Context, int) (FillerContent, error) { return FillerContent{Text: text}, nil }
}

type ToolUpdate struct {
	Call   *llm.FunctionCall
	Output *llm.FunctionCallOutput
	// Value retains the pre-encoding structured value. Deferred delivery uses
	// Output, while the first progress update keeps ordinary tool return DX.
	Value any
}

func (u ToolUpdate) clone() ToolUpdate {
	result := u
	if u.Call != nil {
		result.Call = u.Call.Clone()
	}
	if u.Output != nil {
		copy := *u.Output
		result.Output = &copy
	}
	return result
}

type runContextAttachment struct {
	executor     *ToolExecutor
	resolveFirst func(ToolUpdate) bool
}

// RunContext is the typed voice facade behind llm.ToolOptions.Context. Its
// executor attachment is intentionally replace-proof and is detached when the
// underlying task ends or is cancelled.
type RunContext[UserData any] struct {
	session *AgentSession[UserData]
	handle  *SpeechHandle
	call    *llm.FunctionCall
	base    *llm.RunContext

	mu         sync.Mutex
	updateMu   sync.Mutex
	attachment *runContextAttachment
	updates    []ToolUpdate
	fillers    map[*fillerScheduler[UserData]]struct{}
}

func NewRunContext[UserData any](session *AgentSession[UserData], handle *SpeechHandle, call *llm.FunctionCall) (*RunContext[UserData], error) {
	if session == nil || handle == nil || call == nil {
		return nil, errors.New("voice RunContext requires session, speech handle, and function call")
	}
	result := &RunContext[UserData]{
		session: session, handle: handle, call: call.Clone(),
		updates: make([]ToolUpdate, 0, DefaultRunContextUpdateCapacity),
		fillers: make(map[*fillerScheduler[UserData]]struct{}),
	}
	result.base = llm.NewRunContext(session.UserData(), result.call, session, result)
	return result, nil
}

func (r *RunContext[UserData]) Session() *AgentSession[UserData] { return r.session }
func (r *RunContext[UserData]) UserData() *UserData              { return r.session.UserData() }
func (r *RunContext[UserData]) SpeechHandle() *SpeechHandle      { return r.handle }
func (r *RunContext[UserData]) FunctionCall() *llm.FunctionCall {
	r.mu.Lock()
	call := r.call.Clone()
	r.mu.Unlock()
	return call
}
func (r *RunContext[UserData]) LLMContext() *llm.RunContext { return r.base }

func (r *RunContext[UserData]) Updates() []ToolUpdate {
	r.mu.Lock()
	result := make([]ToolUpdate, len(r.updates))
	for index := range r.updates {
		result[index] = r.updates[index].clone()
	}
	r.mu.Unlock()
	return result
}

func (r *RunContext[UserData]) WaitForPlayout(context.Context) error {
	// Both ordinary and realtime generation execute tools only after the
	// preceding text/audio segment has reached its playback boundary. Keeping
	// this explicit avoids a circular wait on the full SpeechHandle, whose final
	// generation includes the tool itself.
	return nil
}

func (r *RunContext[UserData]) DisallowInterruptions() error {
	return r.handle.SetAllowInterruptions(false)
}

func (r *RunContext[UserData]) Update(ctx context.Context, message any, options ...RunContextUpdateOptions) error {
	if len(options) > 1 {
		return errors.New("RunContext.Update accepts at most one options value")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	var option RunContextUpdateOptions
	if len(options) == 1 {
		option = options[0]
	}
	r.resetFillerDwell()

	r.mu.Lock()
	if len(r.updates) >= DefaultRunContextUpdateCapacity {
		r.mu.Unlock()
		return ErrRunContextUpdateOverflow
	}
	attachment := r.attachment
	call := r.call.Clone()
	template := option.Template
	templateFunc := option.TemplateFunc
	templateSet := option.TemplateSet || template != "" || templateFunc != nil
	step := len(r.updates)
	r.mu.Unlock()
	if !templateSet && attachment != nil && attachment.executor != nil {
		resolved := attachment.executor.toolOptionsSnapshot()
		template, templateFunc = resolved.UpdateTemplate, resolved.UpdateTemplateFunc
	}

	rendered := message
	if text, ok := message.(string); ok {
		rendered = renderRunContextTemplate(template, templateFunc, call.Name, call.CallID, text)
	}
	suffix := ""
	if step > 0 {
		suffix = fmt.Sprintf("_update_%d", step)
	}
	update, err := r.makeUpdate(rendered, suffix)
	if err != nil {
		return err
	}

	r.mu.Lock()
	if len(r.updates) >= DefaultRunContextUpdateCapacity {
		r.mu.Unlock()
		return ErrRunContextUpdateOverflow
	}
	r.updates = append(r.updates, update)
	attachment = r.attachment
	r.mu.Unlock()
	if attachment == nil || attachment.executor == nil {
		return nil
	}
	if attachment.resolveFirst != nil && attachment.resolveFirst(update) {
		return nil
	}
	return attachment.executor.enqueueReply(ctx, r, update)
}

func (r *RunContext[UserData]) Foreground(ctx context.Context, fn func(context.Context, *Agent[UserData]) error) error {
	if fn == nil {
		return errors.New("RunContext.Foreground callback is required")
	}
	_, err := WithForegroundValue(ctx, r, func(callbackCtx context.Context, agent *Agent[UserData]) (struct{}, error) {
		return struct{}{}, fn(callbackCtx, agent)
	})
	return err
}

// WithForegroundValue is the result-bearing form of RunContext.Foreground.
// Go methods cannot introduce a new result type parameter, so this helper
// preserves the TypeScript/Python generic callback result without reflection.
func WithForegroundValue[UserData, Value any](ctx context.Context, run *RunContext[UserData], fn func(context.Context, *Agent[UserData]) (Value, error)) (result Value, err error) {
	if run == nil || fn == nil {
		return result, errors.New("WithForegroundValue requires a RunContext and callback")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	run.mu.Lock()
	attachment := run.attachment
	run.mu.Unlock()
	if attachment != nil && attachment.executor != nil {
		if err := attachment.executor.WaitForReplies(ctx); err != nil {
			return result, err
		}
	}
	return WaitForIdleAndHoldValue(ctx, run.session, fn)
}

func (r *RunContext[UserData]) Filler(ctx context.Context, source FillerSource, options RunContextFillerOptions, fn func(context.Context) error) error {
	if fn == nil {
		return errors.New("RunContext.Filler callback is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	scheduler, err := r.startFiller(ctx, source, options)
	if err != nil {
		return err
	}
	defer r.stopFiller(scheduler)
	return fn(ctx)
}

func WithFillerValue[UserData, Value any](ctx context.Context, run *RunContext[UserData], source FillerSource, options RunContextFillerOptions, fn func(context.Context) (Value, error)) (result Value, err error) {
	if run == nil || fn == nil {
		return result, errors.New("WithFillerValue requires a RunContext and callback")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	scheduler, err := run.startFiller(ctx, source, options)
	if err != nil {
		return result, err
	}
	defer run.stopFiller(scheduler)
	return fn(ctx)
}

func (r *RunContext[UserData]) attach(attachment *runContextAttachment) error {
	if attachment == nil || attachment.executor == nil || attachment.resolveFirst == nil {
		return errors.New("invalid RunContext executor attachment")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.attachment != nil {
		return errors.New("RunContext executor is already attached")
	}
	r.attachment = attachment
	return nil
}

func (r *RunContext[UserData]) detach(attachment *runContextAttachment) {
	r.mu.Lock()
	if r.attachment == attachment {
		r.attachment = nil
	}
	r.mu.Unlock()
}

func (r *RunContext[UserData]) markNonBlocking() {
	r.mu.Lock()
	if r.call.Extra == nil {
		r.call.Extra = make(map[string]any)
	}
	r.call.Extra["__livekit_agents_tool_non_blocking"] = true
	r.mu.Unlock()
}

func (r *RunContext[UserData]) recordFinal(value any) (ToolUpdate, error) {
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	update, err := r.makeUpdate(value, "_final")
	if err != nil {
		return ToolUpdate{}, err
	}
	r.mu.Lock()
	if len(r.updates) >= DefaultRunContextUpdateCapacity {
		r.mu.Unlock()
		return ToolUpdate{}, ErrRunContextUpdateOverflow
	}
	r.updates = append(r.updates, update)
	r.mu.Unlock()
	return update, nil
}

func (r *RunContext[UserData]) makeUpdate(value any, suffix string) (ToolUpdate, error) {
	encoded, err := encodeToolOutput(value)
	if err != nil {
		return ToolUpdate{}, err
	}
	r.mu.Lock()
	original := r.call.Clone()
	r.mu.Unlock()
	call := llm.NewFunctionCall(original.CallID+suffix, original.Name, original.Arguments)
	call.Extra = cloneAnyMap(original.Extra)
	call.GroupID, call.ThoughtSignature = original.GroupID, original.ThoughtSignature
	return ToolUpdate{Call: call, Output: llm.NewFunctionCallOutput(call.CallID, call.Name, encoded, false), Value: value}, nil
}

func cloneAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func renderRunContextTemplate(template string, templateFunc func(llm.UpdatePromptArgs) string, name, callID, message string) string {
	args := llm.UpdatePromptArgs{FunctionName: name, CallID: callID, Message: message}
	if templateFunc != nil {
		return templateFunc(args)
	}
	if template == "" {
		return message
	}
	result := strings.ReplaceAll(template, "{functionName}", name)
	result = strings.ReplaceAll(result, "{callId}", callID)
	return strings.ReplaceAll(result, "{message}", message)
}

// AsRunContext recovers the typed voice facade supplied to a function tool.
// It avoids reflection and makes user-data access type-safe at the tool boundary.
func AsRunContext[UserData any](context *llm.RunContext) (*RunContext[UserData], bool) {
	if context == nil {
		return nil, false
	}
	run, ok := context.Runtime().(*RunContext[UserData])
	return run, ok
}

func (r *RunContext[UserData]) ToolCurrentSpeechHandle() any { return r.handle }
func (r *RunContext[UserData]) ToolWaitForPlayout(ctx context.Context) error {
	return r.WaitForPlayout(ctx)
}
func (r *RunContext[UserData]) ToolDisallowInterruptions() error {
	return r.DisallowInterruptions()
}
func (r *RunContext[UserData]) ToolUpdate(ctx context.Context, message any, options llm.RunContextUpdateOptions) error {
	return r.Update(ctx, message, options)
}
func (r *RunContext[UserData]) ToolForeground(ctx context.Context, fn func(context.Context, any) error) error {
	return r.Foreground(ctx, func(callbackCtx context.Context, agent *Agent[UserData]) error { return fn(callbackCtx, agent) })
}
func (r *RunContext[UserData]) ToolFiller(ctx context.Context, source any, options llm.RunContextFillerOptions, fn func(context.Context) error) error {
	var resolved FillerSource
	switch value := source.(type) {
	case string:
		resolved = TextFiller(value)
	case FillerSource:
		resolved = value
	default:
		return fmt.Errorf("unsupported filler source %T", source)
	}
	return r.Filler(ctx, resolved, options, fn)
}

func (s *AgentSession[UserData]) idleChangeSignal() <-chan struct{} {
	s.mu.RLock()
	changed := s.idleChanged
	s.mu.RUnlock()
	return changed
}

func (s *AgentSession[UserData]) newToolExecutor(parent context.Context, options *AsyncToolOptions) (*ToolExecutor, error) {
	return NewToolExecutor(ToolExecutorOptions{
		MaxConcurrency: DefaultToolConcurrency, MaxCallsPerBatch: DefaultToolBatchLimit,
		DrainTimeout: s.opts.shutdownTimeout, ParentContext: parent,
		Session: s, UserData: s.UserData(), ToolHandling: options,
		Runtime: s, Registry: s.toolRegistry,
		RunContextFactory: func(handle *SpeechHandle, call *llm.FunctionCall) (*llm.RunContext, error) {
			run, err := NewRunContext(s, handle, call)
			if err != nil {
				return nil, err
			}
			return run.LLMContext(), nil
		},
	})
}

// ensureSessionToolExecutors creates session-owned scopes exactly once. The
// first Start call supplies the final session context (including a worker
// inference executor propagated from the entrypoint), while its cancellation
// still does not own these tasks.
func (s *AgentSession[UserData]) ensureSessionToolExecutors() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, toolset := range s.tools.Toolsets() {
		async, ok := toolset.Async()
		if !ok {
			continue
		}
		if s.sessionToolExecutors[async] != nil {
			continue
		}
		options := async.ToolHandling()
		if options == nil {
			options = cloneToolHandling(s.opts.toolHandling).Async
		}
		executor, err := s.newToolExecutor(s.ctx, options)
		if err != nil {
			return fmt.Errorf("create session async toolset %q executor: %w", toolset.ID(), err)
		}
		s.sessionToolExecutors[async] = executor
	}
	return nil
}

func (s *AgentSession[UserData]) toolWaitForIdle(ctx context.Context) error {
	_, err := s.WaitForIdle(ctx)
	return err
}

func (s *AgentSession[UserData]) toolCommitUpdate(update ToolUpdate) {
	update = update.clone()
	s.commitItems(nil, update.Call, update.Output)
	_ = s.events.Publish(s.ctx, FunctionToolsExecutedEvent{
		EventBase:     newEventBase(EventFunctionToolsExecuted, time.Now()),
		FunctionCalls: []*llm.FunctionCall{update.Call}, FunctionCallOutputs: []*llm.FunctionCallOutput{update.Output},
	})
}

func (s *AgentSession[UserData]) toolGenerateReply(ctx context.Context, instructions string, chat *llm.ChatContext) error {
	value := llm.NewInstructions(instructions, instructions)
	_, err := s.GenerateReply(ctx, GenerateReplyOptions{
		ChatContext: chat, Instructions: &value,
		ToolChoice: llm.ToolChoice{Kind: llm.ToolChoiceNone}, InputModality: InputModalityText,
	})
	return err
}

func (s *AgentSession[UserData]) toolChatContext() *llm.ChatContext { return s.ChatContext() }
func (s *AgentSession[UserData]) toolReportError(err error, source any) {
	if err != nil {
		s.emitError(err, source)
	}
}

// RunningTools returns every user callback that has not settled, including a
// cancelled callback that has ignored its context. The built-in task-list tool
// intentionally exposes only live cancellable entries.
func (s *AgentSession[UserData]) RunningTools() []RunningTool {
	if s == nil || s.toolRegistry == nil {
		return nil
	}
	return s.toolRegistry.running(false)
}

func (s *AgentSession[UserData]) cancellableRunningToolCalls() []*llm.FunctionCall {
	if s == nil || s.toolRegistry == nil {
		return nil
	}
	return s.toolRegistry.cancellableCalls()
}

func (s *AgentSession[UserData]) CancelTool(ctx context.Context, callID string) error {
	if s == nil || s.toolRegistry == nil {
		return errors.New("voice session has no tool task registry")
	}
	if strings.TrimSpace(callID) == "" {
		return errors.New("tool call id is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	return s.toolRegistry.cancel(callID)
}

type fillerScheduler[UserData any] struct {
	run     *RunContext[UserData]
	ctx     context.Context
	cancel  context.CancelCauseFunc
	source  FillerSource
	options RunContextFillerOptions
	reset   chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (r *RunContext[UserData]) startFiller(ctx context.Context, source FillerSource, options RunContextFillerOptions) (*fillerScheduler[UserData], error) {
	if source == nil {
		return nil, errors.New("RunContext filler source is required")
	}
	if options.Delay < 0 || options.Interval != nil && *options.Interval < 0 || options.MaxSteps != nil && *options.MaxSteps < 0 {
		return nil, errors.New("RunContext filler delay, interval, and max steps must not be negative")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workerCtx, cancel := context.WithCancelCause(ctx)
	scheduler := &fillerScheduler[UserData]{
		run: r, ctx: workerCtx, cancel: cancel, source: source, options: options,
		reset: make(chan struct{}, 1), done: make(chan struct{}),
	}
	r.mu.Lock()
	if len(r.fillers) >= DefaultRunContextFillerCapacity {
		r.mu.Unlock()
		cancel(errors.New("RunContext filler scheduler capacity exceeded"))
		return nil, errors.New("RunContext filler scheduler capacity exceeded")
	}
	r.fillers[scheduler] = struct{}{}
	r.mu.Unlock()
	go scheduler.runLoop()
	return scheduler, nil
}

func (r *RunContext[UserData]) stopFiller(scheduler *fillerScheduler[UserData]) {
	if scheduler == nil {
		return
	}
	scheduler.close()
	r.mu.Lock()
	delete(r.fillers, scheduler)
	r.mu.Unlock()
}

func (r *RunContext[UserData]) resetFillerDwell() {
	r.mu.Lock()
	fillers := make([]*fillerScheduler[UserData], 0, len(r.fillers))
	for scheduler := range r.fillers {
		fillers = append(fillers, scheduler)
	}
	r.mu.Unlock()
	for _, scheduler := range fillers {
		select {
		case scheduler.reset <- struct{}{}:
		default:
		}
	}
}

func (s *fillerScheduler[UserData]) close() {
	s.once.Do(func() { s.cancel(context.Canceled) })
	<-s.done
}

func (s *fillerScheduler[UserData]) runLoop() {
	defer close(s.done)
	created := 0
	for {
		if s.options.MaxSteps != nil && created >= *s.options.MaxSteps {
			return
		}
		if _, err := s.run.session.WaitForIdle(s.ctx); err != nil {
			return
		}
		idleChanged := s.run.session.idleChangeSignal()
		timer := time.NewTimer(s.options.Delay)
		ready := false
		select {
		case <-s.ctx.Done():
		case <-s.run.handle.InterruptSignal():
		case <-s.reset:
		case <-idleChanged:
		case <-timer.C:
			ready = true
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if !ready {
			if context.Cause(s.ctx) != nil || s.run.handle.Interrupted() {
				return
			}
			continue
		}
		var content FillerContent
		err := guardedActivityCall("tool filler source", func() error {
			var sourceErr error
			content, sourceErr = s.source(s.ctx, created)
			return sourceErr
		})
		if err != nil {
			s.run.session.emitError(fmt.Errorf("create tool filler: %w", err), s.run.call)
			return
		}
		createdSpeech := false
		switch {
		case content.Handle != nil:
			createdSpeech = true
		case strings.TrimSpace(content.Text) != "":
			if _, err := s.run.session.Say(s.ctx, content.Text, SayOptions{}); err != nil {
				if context.Cause(s.ctx) == nil {
					s.run.session.emitError(fmt.Errorf("speak tool filler: %w", err), s.run.call)
				}
				return
			}
			createdSpeech = true
		}
		if createdSpeech {
			created++
		}
		if s.options.Interval == nil || s.options.MaxSteps != nil && created >= *s.options.MaxSteps {
			return
		}
		interval := time.NewTimer(*s.options.Interval)
		select {
		case <-s.ctx.Done():
			interval.Stop()
			return
		case <-s.run.handle.InterruptSignal():
			interval.Stop()
			return
		case <-interval.C:
		}
	}
}

var _ llm.RunContextRuntime = (*RunContext[struct{}])(nil)
