// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/livekit/agents-go/llm"
)

const (
	DefaultToolConcurrency         = 8
	DefaultToolBatchLimit          = 64
	DefaultToolDrainTimeout        = 5 * time.Second
	DefaultToolUpdateQueueCapacity = 64

	UpdateTemplate                = "The tool `{functionName}` has updated, message: {message}\nThe task is still running, so DON'T make up or give information not included in the message above."
	DuplicateRejectTemplate       = "Same tool `{functionName}` is already running:\n{functionCallsText}\nIf you want to cancel the existing one, call `lk_agents_cancel_task` with call_id."
	DuplicateConfirmTemplate      = "Same tool `{functionName}` is already running:\n{functionCallsText}\nRe-call with confirm duplicate True to run a duplicate if needed,\nor if you want to cancel the existing one, call `lk_agents_cancel_task` with call_id."
	ReplyInstructionsAtTail       = "New results arrived from background tool calls (call_ids: {callIds}).\nSummarize the results naturally. Do NOT repeat information you have already told the user."
	ReplyInstructionsMaybeCovered = "New results arrived from background tool calls (call_ids: {callIds}).\nYou may have already mentioned them in your most recent replies.\nIf you already told the user everything in these results, reply with an empty response (no text at all).\nOtherwise, summarize only what you have not said yet, with a natural transition.\nNever repeat information you have already told the user."
)

var (
	ErrToolExecutorClosed    = errors.New("voice tool executor is closed")
	ErrToolBatchTooLarge     = errors.New("voice tool call batch exceeds configured limit")
	ErrToolTaskNotFound      = errors.New("voice tool task not found")
	ErrToolNotCancellable    = errors.New("voice tool call is not cancellable")
	ErrToolCancellationHeld  = errors.New("voice tool call cancellation is disabled by its speech")
	ErrToolUpdatesBacklogged = errors.New("voice async tool update queue is full")
)

type toolExecutorSession interface {
	toolWaitForIdle(context.Context) error
	toolCommitUpdate(ToolUpdate)
	toolGenerateReply(context.Context, string, *llm.ChatContext) error
	toolChatContext() *llm.ChatContext
	toolReportError(error, any)
}

type toolRunContext interface {
	LLMContext() *llm.RunContext
	SpeechHandle() *SpeechHandle
	FunctionCall() *llm.FunctionCall
	attach(*runContextAttachment) error
	detach(*runContextAttachment)
	markNonBlocking()
	recordFinal(any) (ToolUpdate, error)
}

type ToolExecutorOptions struct {
	MaxConcurrency        int
	MaxCallsPerBatch      int
	DrainTimeout          time.Duration
	PendingUpdateCapacity int
	ParentContext         context.Context
	Session               any
	UserData              any
	ToolHandling          *AsyncToolOptions
	Runtime               toolExecutorSession
	Registry              *toolTaskRegistry
	RunContextFactory     func(*SpeechHandle, *llm.FunctionCall) (*llm.RunContext, error)
}

type ToolExecutionOptions struct{ SpeechHandle *SpeechHandle }

type ToolExecutionResult struct {
	Call        *llm.FunctionCall
	Output      *llm.FunctionCallOutput
	Value       any
	Err         error
	NonBlocking bool
}

type RunningTool struct {
	CallID      string    `json:"call_id"`
	Name        string    `json:"name"`
	Arguments   string    `json:"arguments"`
	StartedAt   time.Time `json:"started_at"`
	Cancellable bool      `json:"cancellable"`
	NonBlocking bool      `json:"non_blocking"`
}

type firstToolResult struct {
	mu       sync.Mutex
	done     chan struct{}
	resolved bool
	result   ToolExecutionResult
}

func newFirstToolResult() *firstToolResult { return &firstToolResult{done: make(chan struct{})} }
func (f *firstToolResult) resolve(result ToolExecutionResult) bool {
	return f.resolveFunc(func() ToolExecutionResult { return result })
}
func (f *firstToolResult) resolveFunc(build func() ToolExecutionResult) bool {
	f.mu.Lock()
	if f.resolved {
		f.mu.Unlock()
		return false
	}
	f.resolved, f.result = true, build()
	close(f.done)
	f.mu.Unlock()
	return true
}
func (f *firstToolResult) wait(ctx context.Context) (ToolExecutionResult, error) {
	select {
	case <-f.done:
		f.mu.Lock()
		result := f.result
		f.mu.Unlock()
		return result, nil
	default:
	}
	select {
	case <-ctx.Done():
		return ToolExecutionResult{}, context.Cause(ctx)
	case <-f.done:
		f.mu.Lock()
		result := f.result
		f.mu.Unlock()
		return result, nil
	}
}

type runningTool struct {
	info       RunningTool
	callMu     sync.Mutex
	call       *llm.FunctionCall
	ctx        context.Context
	cancel     context.CancelCauseFunc
	done       chan struct{}
	detached   chan struct{}
	detachOnce sync.Once
	nonblock   atomic.Bool
	abandoned  atomic.Bool
	first      *firstToolResult
	runCtx     toolRunContext
	attachment *runContextAttachment
	executor   *ToolExecutor
}

func (r *runningTool) detachFromCaller() {
	r.nonblock.Store(true)
	r.detachOnce.Do(func() { close(r.detached) })
}

func (r *runningTool) markCallNonBlocking() {
	r.callMu.Lock()
	if r.call.Extra == nil {
		r.call.Extra = make(map[string]any)
	}
	r.call.Extra["__livekit_agents_tool_non_blocking"] = true
	r.callMu.Unlock()
}

func (r *runningTool) callSnapshot() *llm.FunctionCall {
	r.callMu.Lock()
	call := r.call.Clone()
	r.callMu.Unlock()
	return call
}

type pendingToolUpdate struct {
	update ToolUpdate
	ready  chan struct{}
}

// ToolExecutor owns one duplicate/concurrency scope. ParentContext determines
// whether detached tasks survive a handoff (session AsyncToolset) or end with
// an activity (ordinary and agent-scoped executors).
type ToolExecutor struct {
	options ToolExecutorOptions
	ctx     context.Context
	cancel  context.CancelCauseFunc
	sem     chan struct{}

	mu           sync.Mutex
	running      map[string]*runningTool
	active       map[*runningTool]struct{}
	byName       map[string]map[string]*runningTool
	pending      []pendingToolUpdate
	replyRunning bool
	closed       bool
	changed      chan struct{}
	closeOnce    sync.Once
}

func NewToolExecutor(options ToolExecutorOptions) (*ToolExecutor, error) {
	if options.MaxConcurrency == 0 {
		options.MaxConcurrency = DefaultToolConcurrency
	}
	if options.MaxCallsPerBatch == 0 {
		options.MaxCallsPerBatch = DefaultToolBatchLimit
	}
	if options.DrainTimeout == 0 {
		options.DrainTimeout = DefaultToolDrainTimeout
	}
	if options.PendingUpdateCapacity == 0 {
		options.PendingUpdateCapacity = DefaultToolUpdateQueueCapacity
	}
	if options.MaxConcurrency < 1 || options.MaxCallsPerBatch < 1 || options.DrainTimeout < 0 || options.PendingUpdateCapacity < 1 {
		return nil, errors.New("tool executor limits must be positive")
	}
	parent := options.ParentContext
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	return &ToolExecutor{
		options: options, ctx: ctx, cancel: cancel, sem: make(chan struct{}, options.MaxConcurrency),
		running: make(map[string]*runningTool), active: make(map[*runningTool]struct{}), byName: make(map[string]map[string]*runningTool),
		pending: make([]pendingToolUpdate, 0, options.PendingUpdateCapacity), changed: make(chan struct{}),
	}, nil
}

func resolveAsyncToolOptions(options *AsyncToolOptions) AsyncToolOptions {
	result := AsyncToolOptions{}
	if options != nil {
		result = *options
	}
	if result.UpdateTemplate == "" && !result.UpdateTemplateSet {
		result.UpdateTemplate = UpdateTemplate
	}
	if result.DuplicateRejectTemplate == "" && !result.DuplicateRejectTemplateSet {
		result.DuplicateRejectTemplate = DuplicateRejectTemplate
	}
	if result.DuplicateConfirmTemplate == "" && !result.DuplicateConfirmTemplateSet {
		result.DuplicateConfirmTemplate = DuplicateConfirmTemplate
	}
	if result.ReplyAtTailTemplate == "" && !result.ReplyAtTailTemplateSet {
		result.ReplyAtTailTemplate = ReplyInstructionsAtTail
	}
	if result.ReplyMaybeCoveredTemplate == "" && !result.ReplyMaybeCoveredTemplateSet {
		result.ReplyMaybeCoveredTemplate = ReplyInstructionsMaybeCovered
	}
	return result
}

func (e *ToolExecutor) toolOptionsSnapshot() AsyncToolOptions {
	e.mu.Lock()
	options := resolveAsyncToolOptions(e.options.ToolHandling)
	e.mu.Unlock()
	return options
}

func (e *ToolExecutor) SetToolOptions(options *AsyncToolOptions) {
	e.mu.Lock()
	if options == nil {
		e.options.ToolHandling = nil
	} else {
		copy := *options
		e.options.ToolHandling = &copy
	}
	e.mu.Unlock()
}

func (e *ToolExecutor) Execute(ctx context.Context, calls []*llm.FunctionCall, tools *llm.Context, options ...ToolExecutionOptions) ([]ToolExecutionResult, error) {
	if tools == nil {
		return nil, errors.New("tool context must not be nil")
	}
	if len(options) > 1 {
		return nil, errors.New("ToolExecutor.Execute accepts at most one options value")
	}
	if len(calls) > e.options.MaxCallsPerBatch {
		return nil, fmt.Errorf("%w: got %d, limit %d", ErrToolBatchTooLarge, len(calls), e.options.MaxCallsPerBatch)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var execution ToolExecutionOptions
	if len(options) == 1 {
		execution = options[0]
	}
	results := make([]ToolExecutionResult, len(calls))
	waits := make([]*firstToolResult, len(calls))
	for index, call := range calls {
		if call == nil {
			results[index] = toolFailure(nil, "function call must not be nil", errors.New("nil function call"))
			continue
		}
		if call.CallID == "" || call.Name == "" {
			err := errors.New("function call id and name are required")
			results[index] = toolFailure(call, err.Error(), err)
			continue
		}
		tool, ok := tools.FunctionTool(call.Name)
		if !ok {
			err := fmt.Errorf("tool %q is not registered", call.Name)
			results[index] = toolFailure(call, err.Error(), err)
			continue
		}
		running, arguments, immediate := e.reserve(ctx, execution.SpeechHandle, call, tool)
		if immediate != nil {
			results[index] = *immediate
			continue
		}
		waits[index] = running.first
		go e.run(call.Clone(), tool, running, arguments)
	}
	for index, wait := range waits {
		if wait == nil {
			continue
		}
		result, err := wait.wait(ctx)
		if err != nil {
			e.cancelCalls(calls, err, false)
			return nil, err
		}
		results[index] = result
	}
	return results, nil
}

func (e *ToolExecutor) reserve(parent context.Context, handle *SpeechHandle, call *llm.FunctionCall, tool llm.ExecutableTool) (*runningTool, json.RawMessage, *ToolExecutionResult) {
	arguments := json.RawMessage(call.Arguments)
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	confirmed, stripped, err := stripDuplicateConfirmation(arguments)
	if err != nil {
		result := toolFailure(call, "invalid tool arguments: "+err.Error(), err)
		return nil, nil, &result
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		result := toolFailure(call, ErrToolExecutorClosed.Error(), ErrToolExecutorClosed)
		return nil, nil, &result
	}
	duplicates := e.byName[call.Name]
	if len(duplicates) != 0 {
		options := resolveAsyncToolOptions(e.options.ToolHandling)
		switch tool.OnDuplicate() {
		case llm.DuplicateReject:
			message := renderDuplicateMessage(options.DuplicateRejectTemplate, options.DuplicateRejectTemplateFunc, call.Name, duplicates)
			result := toolImmediate(call, message)
			return nil, nil, &result
		case llm.DuplicateConfirm:
			if !confirmed {
				message := renderDuplicateMessage(options.DuplicateConfirmTemplate, options.DuplicateConfirmTemplateFunc, call.Name, duplicates)
				result := toolImmediate(call, message)
				return nil, nil, &result
			}
			arguments = stripped
		case llm.DuplicateReplace:
			for _, duplicate := range duplicates {
				if !duplicate.info.Cancellable {
					message := fmt.Sprintf("cannot replace duplicate call of `%s`: running call is not cancellable", call.Name)
					result := toolFailure(call, message, &llm.ToolError{Message: message})
					return nil, nil, &result
				}
				if handle := duplicate.runCtx.SpeechHandle(); handle != nil && !handle.AllowInterruptions() {
					message := fmt.Sprintf("Tool call %s is not cancellable because interruptions are disallowed", duplicate.info.CallID)
					result := toolFailure(call, message, &llm.ToolError{Message: message})
					return nil, nil, &result
				}
			}
			for _, duplicate := range duplicates {
				e.abandonLocked(duplicate, errors.New("replaced by a newer duplicate tool call"))
			}
		case llm.DuplicateAllow:
		default:
			message := "unknown duplicate handling mode: " + string(tool.OnDuplicate())
			result := toolFailure(call, message, errors.New(message))
			return nil, nil, &result
		}
	}
	if _, exists := e.running[call.CallID]; exists {
		result := toolFailure(call, "tool call id is already running", errors.New("tool call id is already running"))
		return nil, nil, &result
	}
	toolCtx, cancel := context.WithCancelCause(e.ctx)
	first := newFirstToolResult()
	running := &runningTool{
		info: RunningTool{CallID: call.CallID, Name: call.Name, Arguments: call.Arguments, StartedAt: time.Now(), Cancellable: tool.Flags()&llm.ToolFlagCancellable != 0},
		call: call.Clone(), ctx: toolCtx, cancel: cancel, done: make(chan struct{}), detached: make(chan struct{}), first: first, executor: e,
	}
	runCtx, contextErr := e.newRunContext(handle, call)
	if contextErr != nil {
		cancel(contextErr)
		result := toolFailure(call, contextErr.Error(), contextErr)
		return nil, nil, &result
	}
	running.runCtx = runCtx
	attachment := &runContextAttachment{executor: e}
	running.attachment = attachment
	attachment.resolveFirst = func(update ToolUpdate) bool {
		var output *llm.FunctionCallOutput
		if update.Output != nil {
			copy := *update.Output
			output = &copy
		}
		if first.resolveFunc(func() ToolExecutionResult {
			if call.Extra == nil {
				call.Extra = make(map[string]any)
			}
			call.Extra["__livekit_agents_tool_non_blocking"] = true
			running.markCallNonBlocking()
			running.runCtx.markNonBlocking()
			running.detachFromCaller()
			return ToolExecutionResult{Call: call.Clone(), Output: output, Value: update.Value, NonBlocking: true}
		}) {
			return true
		}
		return false
	}
	if err := runCtx.attach(attachment); err != nil {
		cancel(err)
		result := toolFailure(call, err.Error(), err)
		return nil, nil, &result
	}
	e.running[call.CallID] = running
	e.active[running] = struct{}{}
	if e.byName[call.Name] == nil {
		e.byName[call.Name] = make(map[string]*runningTool)
	}
	e.byName[call.Name][call.CallID] = running
	if e.options.Registry != nil {
		if err := e.options.Registry.register(running); err != nil {
			delete(e.running, call.CallID)
			delete(e.active, running)
			delete(e.byName[call.Name], call.CallID)
			runCtx.detach(attachment)
			cancel(err)
			result := toolFailure(call, err.Error(), err)
			return nil, nil, &result
		}
	}
	e.signalLocked()
	go func() {
		select {
		case <-parent.Done():
			if !running.nonblock.Load() {
				e.abandon(running, context.Cause(parent))
				return
			}
		case <-running.detached:
			// The caller/speech no longer owns this task, but the executor still
			// does. Keep watching its activity/session lifetime below.
		case <-running.done:
			return
		case <-e.ctx.Done():
			e.abandon(running, context.Cause(e.ctx))
			return
		}
		select {
		case <-running.done:
		case <-e.ctx.Done():
			e.abandon(running, context.Cause(e.ctx))
		}
	}()
	return running, arguments, nil
}

func (e *ToolExecutor) newRunContext(handle *SpeechHandle, call *llm.FunctionCall) (toolRunContext, error) {
	if handle == nil {
		handle = NewSpeechHandle(SpeechHandleOptions{})
		handle.MarkDone(nil)
	}
	if e.options.RunContextFactory != nil {
		base, err := e.options.RunContextFactory(handle, call.Clone())
		if err != nil {
			return nil, err
		}
		if base == nil || base.Runtime() == nil {
			return nil, errors.New("tool RunContext factory returned no runtime")
		}
		value, ok := base.Runtime().(toolRunContext)
		if !ok {
			return nil, fmt.Errorf("tool RunContext runtime has type %T", base.Runtime())
		}
		return value, nil
	}
	return newBasicToolRunContext(e.options.UserData, e.options.Session, handle, call), nil
}

func (e *ToolExecutor) run(call *llm.FunctionCall, tool llm.ExecutableTool, running *runningTool, arguments json.RawMessage) {
	defer e.finish(running, call.Name)
	select {
	case e.sem <- struct{}{}:
		defer func() { <-e.sem }()
	case <-running.ctx.Done():
		err := context.Cause(running.ctx)
		running.first.resolve(toolFailure(call, err.Error(), err))
		return
	}

	value, toolErr := executeToolSafely(running.ctx, call, tool, arguments, running.runCtx.LLMContext())
	if context.Cause(running.ctx) != nil {
		err := context.Cause(running.ctx)
		running.first.resolve(toolFailure(call, err.Error(), err))
		return
	}
	if toolErr != nil {
		if running.first.resolve(toolFailure(call, toolErr.Error(), toolErr)) {
			return
		}
		if e.options.Runtime != nil {
			e.options.Runtime.toolReportError(fmt.Errorf("async tool %q failed after progress update: %w", call.Name, toolErr), call)
		}
		return
	}
	encoded, encodeErr := encodeToolOutput(value)
	if encodeErr != nil {
		if running.first.resolve(toolFailure(call, encodeErr.Error(), encodeErr)) {
			return
		}
		if e.options.Runtime != nil {
			e.options.Runtime.toolReportError(encodeErr, call)
		}
		return
	}
	standard := ToolExecutionResult{Call: call.Clone(), Output: llm.NewFunctionCallOutput(call.CallID, call.Name, encoded, false), Value: value}
	if running.first.resolve(standard) {
		return
	}
	if value == nil || context.Cause(running.ctx) != nil {
		return
	}
	update, err := running.runCtx.recordFinal(value)
	if err != nil {
		if e.options.Runtime != nil {
			e.options.Runtime.toolReportError(fmt.Errorf("record async tool final output: %w", err), call)
		}
		return
	}
	if err := e.enqueueReply(running.ctx, running.runCtx, update); err != nil && context.Cause(running.ctx) == nil && e.options.Runtime != nil {
		e.options.Runtime.toolReportError(fmt.Errorf("enqueue async tool final output: %w", err), call)
	}
}

func executeToolSafely(ctx context.Context, call *llm.FunctionCall, tool llm.ExecutableTool, arguments json.RawMessage, runCtx *llm.RunContext) (value any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("tool %q panicked: %v\n%s", call.Name, recovered, debug.Stack())
		}
	}()
	return tool.Execute(ctx, arguments, llm.ToolOptions{Context: runCtx, ToolCallID: call.CallID})
}

func toolFailure(call *llm.FunctionCall, message string, err error) ToolExecutionResult {
	callID, name := "", ""
	var cloned *llm.FunctionCall
	if call != nil {
		callID, name, cloned = call.CallID, call.Name, call.Clone()
	}
	return ToolExecutionResult{Call: cloned, Output: llm.NewFunctionCallOutput(callID, name, message, true), Err: err}
}

func toolImmediate(call *llm.FunctionCall, value string) ToolExecutionResult {
	callID, name := "", ""
	var cloned *llm.FunctionCall
	if call != nil {
		callID, name, cloned = call.CallID, call.Name, call.Clone()
	}
	return ToolExecutionResult{
		Call: cloned, Output: llm.NewFunctionCallOutput(callID, name, value, false), Value: value,
	}
}

func encodeToolOutput(value any) (string, error) {
	if value == nil {
		return "", nil
	}
	if text, ok := value.(string); ok {
		return text, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode tool output: %w", err)
	}
	return string(data), nil
}

func stripDuplicateConfirmation(arguments json.RawMessage) (bool, json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(arguments, &object); err != nil {
		return false, nil, err
	}
	raw, present := object[llm.ConfirmDuplicateParam]
	if !present {
		return false, arguments, nil
	}
	var confirmed bool
	if err := json.Unmarshal(raw, &confirmed); err != nil {
		return false, nil, fmt.Errorf("%s must be boolean", llm.ConfirmDuplicateParam)
	}
	delete(object, llm.ConfirmDuplicateParam)
	stripped, err := json.Marshal(object)
	return confirmed, stripped, err
}

func renderDuplicateMessage(template string, templateFunc func(llm.DuplicatePromptArgs) string, functionName string, running map[string]*runningTool) string {
	items := make([]RunningTool, 0, len(running))
	functionCallsJSON := make([]string, 0, len(running))
	for _, tool := range running {
		item := tool.info
		item.NonBlocking = tool.nonblock.Load()
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CallID < items[j].CallID })
	lines := make([]string, len(items))
	for index, item := range items {
		call := running[item.CallID].callSnapshot()
		encoded, err := json.Marshal(call)
		if err != nil {
			encoded = []byte(fmt.Sprintf(`{"type":"function_call","callId":%q,"name":%q,"arguments":%q}`, item.CallID, item.Name, item.Arguments))
		}
		functionCallsJSON = append(functionCallsJSON, string(encoded))
		lines[index] = string(encoded)
	}
	args := llm.DuplicatePromptArgs{FunctionName: functionName, FunctionCallsJSON: functionCallsJSON, FunctionCallsText: strings.Join(lines, "\n")}
	if templateFunc != nil {
		return templateFunc(args)
	}
	message := strings.ReplaceAll(template, "{functionName}", functionName)
	message = strings.ReplaceAll(message, "{functionCallsText}", strings.Join(lines, "\n"))
	return strings.ReplaceAll(message, "{functionCallsJson}", strings.Join(functionCallsJSON, ","))
}

func (e *ToolExecutor) finish(running *runningTool, name string) {
	e.mu.Lock()
	delete(e.active, running)
	if e.running[running.info.CallID] == running {
		delete(e.running, running.info.CallID)
		if sameName := e.byName[name]; sameName != nil {
			delete(sameName, running.info.CallID)
			if len(sameName) == 0 {
				delete(e.byName, name)
			}
		}
	}
	running.cancel(nil)
	running.runCtx.detach(running.attachment)
	select {
	case <-running.done:
	default:
		close(running.done)
	}
	if e.options.Registry != nil {
		e.options.Registry.unregister(running)
	}
	e.signalLocked()
	e.mu.Unlock()
}

func (e *ToolExecutor) abandon(running *runningTool, cause error) {
	if running == nil {
		return
	}
	e.mu.Lock()
	e.abandonLocked(running, cause)
	e.mu.Unlock()
}

// abandonLocked removes a task from executor-level duplicate/cancellation
// visibility immediately while retaining it in the session registry until the
// user callback actually settles. This permits same-call replacement and makes
// a stashed RunContext inert even when user code ignores cancellation.
func (e *ToolExecutor) abandonLocked(running *runningTool, cause error) {
	if running == nil || running.abandoned.Swap(true) {
		return
	}
	if e.running[running.info.CallID] == running {
		delete(e.running, running.info.CallID)
		if sameName := e.byName[running.info.Name]; sameName != nil {
			delete(sameName, running.info.CallID)
			if len(sameName) == 0 {
				delete(e.byName, running.info.Name)
			}
		}
	}
	running.runCtx.detach(running.attachment)
	if cause == nil {
		cause = context.Canceled
	}
	running.cancel(cause)
	call := running.runCtx.FunctionCall()
	running.first.resolve(toolFailure(call, cause.Error(), cause))
	e.signalLocked()
}

func (e *ToolExecutor) Cancel(callID string) error {
	e.mu.Lock()
	running := e.running[callID]
	if running == nil {
		e.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrToolTaskNotFound, callID)
	}
	if !running.info.Cancellable {
		e.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrToolNotCancellable, callID)
	}
	if handle := running.runCtx.SpeechHandle(); handle != nil && !handle.AllowInterruptions() {
		e.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrToolCancellationHeld, callID)
	}
	e.abandonLocked(running, context.Canceled)
	e.mu.Unlock()
	return nil
}

func (e *ToolExecutor) cancelCalls(calls []*llm.FunctionCall, cause error, requireCancellable bool) {
	e.mu.Lock()
	for _, call := range calls {
		if call == nil {
			continue
		}
		if running := e.running[call.CallID]; running != nil && !running.nonblock.Load() && (!requireCancellable || running.info.Cancellable) {
			e.abandonLocked(running, cause)
		}
	}
	e.mu.Unlock()
}

func (e *ToolExecutor) Running() []RunningTool {
	e.mu.Lock()
	result := make([]RunningTool, 0, len(e.running))
	for _, running := range e.running {
		item := running.info
		item.NonBlocking = running.nonblock.Load()
		result = append(result, item)
	}
	e.mu.Unlock()
	sort.Slice(result, func(i, j int) bool { return result[i].CallID < result[j].CallID })
	return result
}

func (e *ToolExecutor) enqueueReply(ctx context.Context, _ toolRunContext, update ToolUpdate) error {
	if e.options.Runtime == nil {
		return errors.New("async tool replies require an AgentSession runtime")
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
	}
	if update.Call == nil || update.Output == nil {
		return errors.New("async tool update requires a function call/output pair")
	}
	entry := pendingToolUpdate{update: update.clone(), ready: make(chan struct{})}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrToolExecutorClosed
	}
	if len(e.pending) >= e.options.PendingUpdateCapacity {
		e.mu.Unlock()
		return ErrToolUpdatesBacklogged
	}
	e.pending = append(e.pending, entry)
	start := !e.replyRunning
	if start {
		e.replyRunning = true
	}
	e.signalLocked()
	e.mu.Unlock()
	e.options.Runtime.toolCommitUpdate(entry.update)
	close(entry.ready)
	if start {
		go e.deliverReplies()
	}
	return nil
}

func (e *ToolExecutor) deliverReplies() {
	for {
		if err := e.options.Runtime.toolWaitForIdle(e.ctx); err != nil {
			if context.Cause(e.ctx) == nil {
				e.options.Runtime.toolReportError(fmt.Errorf("wait to deliver async tool reply: %w", err), e)
			}
			e.finishReplyLoop()
			return
		}
		e.mu.Lock()
		if len(e.pending) == 0 {
			e.replyRunning = false
			e.signalLocked()
			e.mu.Unlock()
			return
		}
		updates := append([]pendingToolUpdate(nil), e.pending...)
		e.pending = e.pending[:0]
		e.signalLocked()
		e.mu.Unlock()
		for _, update := range updates {
			select {
			case <-e.ctx.Done():
				e.finishReplyLoop()
				return
			case <-update.ready:
			}
		}
		chat := e.options.Runtime.toolChatContext()
		lastID := ""
		if items := chat.Items(); len(items) != 0 {
			lastID = items[len(items)-1].ItemID()
		}
		callIDs := make([]string, 0, len(updates))
		for _, update := range updates {
			callIDs = append(callIDs, update.update.Output.CallID)
		}
		options := e.toolOptionsSnapshot()
		template := options.ReplyMaybeCoveredTemplate
		atTail := false
		if last := updates[len(updates)-1].update.Output; last != nil && last.ItemID() == lastID {
			atTail = true
			template = options.ReplyAtTailTemplate
		}
		var templateFunc func(llm.ReplyPromptArgs) string
		if atTail {
			templateFunc = options.ReplyAtTailTemplateFunc
		} else {
			templateFunc = options.ReplyMaybeCoveredTemplateFunc
		}
		instructions := strings.ReplaceAll(template, "{callIds}", strings.Join(callIDs, ", "))
		if templateFunc != nil {
			instructions = templateFunc(llm.ReplyPromptArgs{CallIDs: append([]string(nil), callIDs...)})
		}
		if err := e.options.Runtime.toolGenerateReply(e.ctx, instructions, chat); err != nil && context.Cause(e.ctx) == nil {
			e.options.Runtime.toolReportError(fmt.Errorf("generate async tool reply: %w", err), e)
		}
	}
}

func (e *ToolExecutor) finishReplyLoop() {
	e.mu.Lock()
	e.replyRunning = false
	e.signalLocked()
	e.mu.Unlock()
}

func (e *ToolExecutor) WaitForReplies(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		e.mu.Lock()
		if !e.replyRunning && len(e.pending) == 0 {
			e.mu.Unlock()
			return nil
		}
		changed := e.changed
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-e.ctx.Done():
			return context.Cause(e.ctx)
		case <-changed:
		}
	}
}

func (e *ToolExecutor) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if e.options.DrainTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.options.DrainTimeout)
		defer cancel()
	}
	e.beginDrain()
	for {
		e.mu.Lock()
		if len(e.active) == 0 && !e.replyRunning && len(e.pending) == 0 {
			e.mu.Unlock()
			return nil
		}
		changed := e.changed
		e.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func (e *ToolExecutor) beginDrain() {
	e.mu.Lock()
	for running := range e.active {
		if running.info.Cancellable {
			e.abandonLocked(running, context.Canceled)
		}
	}
	e.mu.Unlock()
}

func (e *ToolExecutor) Close(ctx context.Context) error {
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		e.pending = e.pending[:0]
		for running := range e.active {
			e.abandonLocked(running, ErrToolExecutorClosed)
		}
		e.signalLocked()
		e.mu.Unlock()
		e.cancel(ErrToolExecutorClosed)
	})
	return e.Drain(ctx)
}

func (e *ToolExecutor) signalLocked() {
	close(e.changed)
	e.changed = make(chan struct{})
}

type toolTaskRegistry struct {
	mu    sync.Mutex
	tasks map[string]*runningTool
}

func newToolTaskRegistry() *toolTaskRegistry {
	return &toolTaskRegistry{tasks: make(map[string]*runningTool)}
}
func (r *toolTaskRegistry) register(task *runningTool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.tasks[task.info.CallID]; existing != nil && !existing.abandoned.Load() {
		return fmt.Errorf("tool task %q is already registered", task.info.CallID)
	}
	r.tasks[task.info.CallID] = task
	return nil
}
func (r *toolTaskRegistry) unregister(task *runningTool) {
	r.mu.Lock()
	if r.tasks[task.info.CallID] == task {
		delete(r.tasks, task.info.CallID)
	}
	r.mu.Unlock()
}
func (r *toolTaskRegistry) running(cancellableOnly bool) []RunningTool {
	r.mu.Lock()
	result := make([]RunningTool, 0, len(r.tasks))
	for _, task := range r.tasks {
		if cancellableOnly && (!task.info.Cancellable || task.abandoned.Load()) {
			continue
		}
		item := task.info
		item.NonBlocking = task.nonblock.Load()
		result = append(result, item)
	}
	r.mu.Unlock()
	sort.Slice(result, func(i, j int) bool { return result[i].CallID < result[j].CallID })
	return result
}

func (r *toolTaskRegistry) cancellableCalls() []*llm.FunctionCall {
	r.mu.Lock()
	result := make([]*llm.FunctionCall, 0, len(r.tasks))
	for _, task := range r.tasks {
		if !task.info.Cancellable || task.abandoned.Load() {
			continue
		}
		result = append(result, task.callSnapshot())
	}
	r.mu.Unlock()
	sort.Slice(result, func(i, j int) bool { return result[i].CallID < result[j].CallID })
	return result
}
func (r *toolTaskRegistry) cancel(callID string) error {
	r.mu.Lock()
	task := r.tasks[callID]
	r.mu.Unlock()
	if task == nil {
		return fmt.Errorf("%w: %s", ErrToolTaskNotFound, callID)
	}
	return task.executor.Cancel(callID)
}

type cancelTaskInput struct {
	CallID string `json:"call_id"`
}

type taskToolSession interface {
	cancellableRunningToolCalls() []*llm.FunctionCall
	CancelTool(context.Context, string) error
}

// GetRunningTasksTool and CancelTaskTool are automatically advertised whenever
// an activity contains at least one cancellable tool. They remain exported for
// callers that want to include them explicitly in a constrained tool context.
var GetRunningTasksTool = llm.MustTool(llm.FunctionToolOptions[struct{}, []*llm.FunctionCall]{
	Name:        "lk_agents_get_running_tasks",
	Description: "Get the list of running tool calls that are cancellable.",
	Execute: func(_ context.Context, _ struct{}, options llm.ToolOptions) ([]*llm.FunctionCall, error) {
		if options.Context == nil {
			return nil, &llm.ToolError{Message: "running-task tool requires a RunContext"}
		}
		session, ok := options.Context.Session.(taskToolSession)
		if !ok || session == nil {
			return nil, &llm.ToolError{Message: "running-task tool requires an AgentSession"}
		}
		return session.cancellableRunningToolCalls(), nil
	},
})

var CancelTaskTool = llm.MustTool(llm.FunctionToolOptions[cancelTaskInput, string]{
	Name:        "lk_agents_cancel_task",
	Description: "Cancel a running tool call by call_id.",
	Parameters:  json.RawMessage(`{"type":"object","properties":{"call_id":{"type":"string"}},"required":["call_id"],"additionalProperties":false}`),
	Execute: func(ctx context.Context, input cancelTaskInput, options llm.ToolOptions) (string, error) {
		if options.Context == nil {
			return "", &llm.ToolError{Message: "cancel-task tool requires a RunContext"}
		}
		session, ok := options.Context.Session.(taskToolSession)
		if !ok || session == nil {
			return "", &llm.ToolError{Message: "cancel-task tool requires an AgentSession"}
		}
		if err := session.CancelTool(ctx, input.CallID); err != nil {
			message := err.Error()
			switch {
			case errors.Is(err, ErrToolTaskNotFound):
				message = fmt.Sprintf("Task %s not found", input.CallID)
			case errors.Is(err, ErrToolCancellationHeld):
				message = fmt.Sprintf("Tool call %s is not cancellable because interruptions are disallowed", input.CallID)
			case errors.Is(err, ErrToolNotCancellable):
				message = fmt.Sprintf("Tool call %s is not cancellable", input.CallID)
			}
			return "", &llm.ToolError{Message: message}
		}
		return fmt.Sprintf("Task %s cancelled successfully.", input.CallID), nil
	},
})

// TaskManagementTools returns a fresh slice containing the built-in task tools.
func TaskManagementTools() []llm.Tool {
	return []llm.Tool{CancelTaskTool, GetRunningTasksTool}
}

func hasCancellableTool(tools *llm.Context) bool {
	if tools == nil {
		return false
	}
	for _, tool := range tools.SortedFunctionTools() {
		if tool.Flags()&llm.ToolFlagCancellable != 0 {
			return true
		}
	}
	return false
}

func augmentTaskManagementTools(tools *llm.Context) (*llm.Context, error) {
	if !hasCancellableTool(tools) {
		return tools, nil
	}
	entries := make([]any, 0, len(tools.Flatten())+len(tools.Toolsets())+2)
	for _, tool := range tools.Flatten() {
		entries = append(entries, tool)
	}
	for _, toolset := range tools.Toolsets() {
		entries = append(entries, toolset)
	}
	entries = append(entries, CancelTaskTool, GetRunningTasksTool)
	return llm.NewToolContext(entries...)
}

type basicToolRunContext struct {
	base       *llm.RunContext
	handle     *SpeechHandle
	call       *llm.FunctionCall
	mu         sync.Mutex
	attachment *runContextAttachment
}

func newBasicToolRunContext(userData, session any, handle *SpeechHandle, call *llm.FunctionCall) *basicToolRunContext {
	result := &basicToolRunContext{handle: handle, call: call.Clone()}
	result.base = llm.NewRunContext(userData, result.call, session, nil)
	return result
}
func (r *basicToolRunContext) LLMContext() *llm.RunContext { return r.base }
func (r *basicToolRunContext) SpeechHandle() *SpeechHandle { return r.handle }
func (r *basicToolRunContext) FunctionCall() *llm.FunctionCall {
	r.mu.Lock()
	call := r.call.Clone()
	r.mu.Unlock()
	return call
}
func (r *basicToolRunContext) attach(value *runContextAttachment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.attachment != nil {
		return errors.New("RunContext executor is already attached")
	}
	r.attachment = value
	return nil
}
func (r *basicToolRunContext) detach(value *runContextAttachment) {
	r.mu.Lock()
	if value == nil || r.attachment == value {
		r.attachment = nil
	}
	r.mu.Unlock()
}
func (r *basicToolRunContext) markNonBlocking() {
	r.mu.Lock()
	if r.call.Extra == nil {
		r.call.Extra = make(map[string]any)
	}
	r.call.Extra["__livekit_agents_tool_non_blocking"] = true
	r.mu.Unlock()
}
func (r *basicToolRunContext) recordFinal(value any) (ToolUpdate, error) {
	encoded, err := encodeToolOutput(value)
	if err != nil {
		return ToolUpdate{}, err
	}
	r.mu.Lock()
	original := r.call.Clone()
	r.mu.Unlock()
	call := llm.NewFunctionCall(original.CallID+"_final", original.Name, original.Arguments)
	return ToolUpdate{Call: call, Output: llm.NewFunctionCallOutput(call.CallID, call.Name, encoded, false), Value: value}, nil
}
