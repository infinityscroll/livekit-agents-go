// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/llm"
)

type runContextTestRuntime struct {
	mu           sync.Mutex
	chat         *llm.ChatContext
	idle         <-chan struct{}
	commits      []ToolUpdate
	instructions []string
	errs         []error
}

func newRunContextTestRuntime() *runContextTestRuntime {
	return &runContextTestRuntime{chat: llm.NewChatContext()}
}

func (r *runContextTestRuntime) toolWaitForIdle(ctx context.Context) error {
	if r.idle == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.idle:
		return nil
	}
}
func (r *runContextTestRuntime) toolCommitUpdate(update ToolUpdate) {
	update = update.clone()
	r.mu.Lock()
	r.commits = append(r.commits, update)
	_ = r.chat.Insert(update.Call, update.Output)
	r.mu.Unlock()
}
func (r *runContextTestRuntime) toolGenerateReply(_ context.Context, instructions string, _ *llm.ChatContext) error {
	r.mu.Lock()
	r.instructions = append(r.instructions, instructions)
	r.mu.Unlock()
	return nil
}
func (r *runContextTestRuntime) toolChatContext() *llm.ChatContext {
	r.mu.Lock()
	chat := r.chat.Copy(llm.CopyOptions{})
	r.mu.Unlock()
	return chat
}
func (r *runContextTestRuntime) toolReportError(err error, _ any) {
	r.mu.Lock()
	r.errs = append(r.errs, err)
	r.mu.Unlock()
}

func waitForNextSpeech(t *testing.T, subscription *EventSubscription, excludedID string) *SpeechHandle {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		event, err := subscription.Recv(ctx)
		if err != nil {
			t.Fatalf("wait for speech event: %v", err)
		}
		created, ok := event.(SpeechCreatedEvent)
		if !ok || created.SpeechHandle == nil || created.SpeechHandle.ID() == excludedID {
			continue
		}
		return created.SpeechHandle
	}
}

func TestSessionAsyncToolsetSurvivesAgentHandoffAndDeliversFinalReply(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	cancelled := make(chan error, 1)
	tool := llm.MustTool(llm.FunctionToolOptions[struct{}, string]{
		Name: "background_lookup", Flags: llm.ToolFlagCancellable,
		Execute: func(ctx context.Context, _ struct{}, options llm.ToolOptions) (string, error) {
			if options.Context == nil {
				return "", errors.New("missing RunContext")
			}
			if err := options.Context.Update(ctx, "started"); err != nil {
				return "", err
			}
			close(started)
			select {
			case <-release:
				return "final result", nil
			case <-ctx.Done():
				cancelled <- context.Cause(ctx)
				return "", context.Cause(ctx)
			}
		},
	})
	async := llm.MustAsyncToolset(llm.AsyncToolsetOptions{ID: "session_background", Tools: []llm.Tool{tool}})
	tools, err := llm.NewToolContext(async)
	if err != nil {
		t.Fatal(err)
	}
	call := llm.NewFunctionCall("background-call", "background_lookup", `{}`)
	model := newSessionTestLLM([]sessionLLMResponse{
		{calls: []*llm.FunctionCall{call}},
		{text: "I started the lookup."},
		{text: "The final result arrived."},
	})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		LLM: model, Tools: tools, DisableUserAwayTimeout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	first := MustAgent(AgentOptions[struct{}]{ID: "first"})
	second := MustAgent(AgentOptions[struct{}]{ID: "second"})
	if err := session.Start(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	subscription, err := session.Subscribe(EventSubscriptionOptions{Capacity: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("async tool did not report its first update")
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.UpdateAgent(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if running := session.RunningTools(); len(running) != 1 || !running[0].NonBlocking {
		t.Fatalf("running tools after handoff = %#v", running)
	}
	select {
	case err := <-cancelled:
		t.Fatalf("session async task was cancelled by handoff: %v", err)
	default:
	}
	close(release)
	deferred := waitForNextSpeech(t, subscription, handle.ID())
	if err := deferred.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if model.Calls() != 3 {
		t.Fatalf("LLM calls = %d, want initial, progress, and deferred final", model.Calls())
	}
	if running := session.RunningTools(); len(running) != 0 {
		t.Fatalf("running tools after completion = %#v", running)
	}
	var firstOutput, finalCall, finalOutput bool
	for _, item := range session.ChatContext().Items() {
		switch value := item.(type) {
		case *llm.FunctionCall:
			if value.CallID == "background-call_final" {
				finalCall = true
			}
			if value.CallID == "background-call" && value.Extra["__livekit_agents_tool_non_blocking"] != true {
				t.Fatalf("initial call is not marked non-blocking: %#v", value.Extra)
			}
		case *llm.FunctionCallOutput:
			switch value.CallID {
			case "background-call":
				firstOutput = true
			case "background-call_final":
				finalOutput = value.Output == "final result"
			}
		}
	}
	if !firstOutput || !finalCall || !finalOutput {
		t.Fatalf("history firstOutput=%v finalCall=%v finalOutput=%v", firstOutput, finalCall, finalOutput)
	}
}

func TestAgentAsyncToolsetStopsAtHandoff(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan error, 1)
	tool := llm.MustTool(llm.FunctionToolOptions[struct{}, string]{
		Name: "activity_lookup", Flags: llm.ToolFlagCancellable,
		Execute: func(ctx context.Context, _ struct{}, options llm.ToolOptions) (string, error) {
			if err := options.Context.Update(ctx, "started"); err != nil {
				return "", err
			}
			close(started)
			<-ctx.Done()
			stopped <- context.Cause(ctx)
			return "", context.Cause(ctx)
		},
	})
	async := llm.MustAsyncToolset(llm.AsyncToolsetOptions{ID: "activity_background", Tools: []llm.Tool{tool}})
	agentTools, _ := llm.NewToolContext(async)
	model := newSessionTestLLM([]sessionLLMResponse{
		{calls: []*llm.FunctionCall{llm.NewFunctionCall("activity-call", "activity_lookup", `{}`)}},
		{text: "Working."},
	})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	first := MustAgent(AgentOptions[struct{}]{ID: "first", Tools: agentTools})
	if err := session.Start(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("activity async tool did not start")
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.UpdateAgent(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "second"})); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatal("activity async tool stopped without a cancellation cause")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("activity async tool survived its owning handoff")
	}
}

func TestRunContextCurrentSpeechPlayoutAndInterruptionControl(t *testing.T) {
	type userData struct{ Account string }
	seen := make(chan *RunContext[userData], 1)
	release := make(chan struct{})
	tool := llm.MustTool(llm.FunctionToolOptions[struct{}, string]{
		Name: "inspect_context",
		Execute: func(ctx context.Context, _ struct{}, options llm.ToolOptions) (string, error) {
			run, ok := AsRunContext[userData](options.Context)
			if !ok {
				return "", errors.New("typed RunContext unavailable")
			}
			if err := run.WaitForPlayout(ctx); err != nil {
				return "", err
			}
			if err := run.DisallowInterruptions(); err != nil {
				return "", err
			}
			seen <- run
			select {
			case <-release:
				return "done", nil
			case <-ctx.Done():
				return "", context.Cause(ctx)
			}
		},
	})
	tools, _ := llm.NewToolContext(tool)
	model := newSessionTestLLM([]sessionLLMResponse{
		{calls: []*llm.FunctionCall{llm.NewFunctionCall("inspect-call", "inspect_context", `{}`)}},
		{text: "Done."},
	})
	session, err := NewAgentSession(AgentSessionOptions[userData]{
		LLM: model, Tools: tools, UserData: userData{Account: "A-1"}, DisableUserAwayTimeout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	if err := session.Start(t.Context(), MustAgent(AgentOptions[userData]{ID: "context"})); err != nil {
		t.Fatal(err)
	}
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var run *RunContext[userData]
	select {
	case run = <-seen:
	case <-time.After(2 * time.Second):
		t.Fatal("tool did not receive RunContext")
	}
	if run.SpeechHandle() != handle || run.LLMContext().CurrentSpeechHandle() != handle {
		t.Fatal("RunContext did not expose the current speech handle")
	}
	if run.UserData().Account != "A-1" {
		t.Fatalf("user data = %#v", run.UserData())
	}
	if handle.AllowInterruptions() {
		t.Fatal("DisallowInterruptions did not update the current speech")
	}
	close(release)
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRunContextConcurrentUpdatesAreSerializedBoundedAndTemplated(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close(context.Background()) }()
	runtime := newRunContextTestRuntime()
	var updateArgsMu sync.Mutex
	var updateArgs []llm.UpdatePromptArgs
	var replyCalls atomic.Int64
	executor, err := NewToolExecutor(ToolExecutorOptions{
		ParentContext: t.Context(), Runtime: runtime,
		PendingUpdateCapacity: DefaultToolUpdateQueueCapacity,
		ToolHandling: &AsyncToolOptions{
			UpdateTemplateFunc: func(args llm.UpdatePromptArgs) string {
				updateArgsMu.Lock()
				updateArgs = append(updateArgs, args)
				updateArgsMu.Unlock()
				return fmt.Sprintf("%s:%s:%s", args.FunctionName, args.CallID, args.Message)
			},
			ReplyAtTailTemplateFunc: func(args llm.ReplyPromptArgs) string {
				replyCalls.Add(1)
				return "reply for " + strings.Join(args.CallIDs, ",")
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close(context.Background()) }()
	run, err := NewRunContext(session, NewSpeechHandle(SpeechHandleOptions{}), llm.NewFunctionCall("progress", "slow_lookup", `{"query":"flights"}`))
	if err != nil {
		t.Fatal(err)
	}
	var first atomic.Bool
	firstUpdate := make(chan ToolUpdate, 1)
	attachment := &runContextAttachment{executor: executor}
	attachment.resolveFirst = func(update ToolUpdate) bool {
		if first.CompareAndSwap(false, true) {
			firstUpdate <- update.clone()
			return true
		}
		return false
	}
	if err := run.attach(attachment); err != nil {
		t.Fatal(err)
	}
	if err := run.Update(t.Context(), "started"); err != nil {
		t.Fatal(err)
	}
	initial := <-firstUpdate
	if initial.Output == nil || initial.Output.Output != "slow_lookup:progress:started" || initial.Value != "slow_lookup:progress:started" || initial.Call.CallID != "progress" {
		t.Fatalf("first templated update = %#v", initial)
	}

	const concurrent = 32
	errs := make(chan error, concurrent)
	var workers sync.WaitGroup
	workers.Add(concurrent)
	for index := 0; index < concurrent; index++ {
		go func(index int) {
			defer workers.Done()
			errs <- run.Update(t.Context(), fmt.Sprintf("step-%02d", index))
		}(index)
	}
	workers.Wait()
	close(errs)
	for updateErr := range errs {
		if updateErr != nil {
			t.Fatal(updateErr)
		}
	}
	if err := executor.WaitForReplies(t.Context()); err != nil {
		t.Fatal(err)
	}
	updates := run.Updates()
	if len(updates) != concurrent+1 {
		t.Fatalf("updates = %d, want %d", len(updates), concurrent+1)
	}
	ids := make([]string, 0, concurrent)
	for _, update := range updates[1:] {
		ids = append(ids, update.Call.CallID)
	}
	sort.Strings(ids)
	want := make([]string, concurrent)
	for index := range want {
		want[index] = fmt.Sprintf("progress_update_%d", index+1)
	}
	sort.Strings(want)
	for index := range want {
		if ids[index] != want[index] {
			t.Fatalf("update IDs = %v", ids)
		}
	}
	updateArgsMu.Lock()
	argsCount := len(updateArgs)
	updateArgsMu.Unlock()
	if argsCount != concurrent+1 {
		t.Fatalf("template callback calls = %d, want %d", argsCount, concurrent+1)
	}
	runtime.mu.Lock()
	commits, generated, runtimeErrs := len(runtime.commits), len(runtime.instructions), append([]error(nil), runtime.errs...)
	runtime.mu.Unlock()
	if commits != concurrent || generated == 0 || replyCalls.Load() == 0 || len(runtimeErrs) != 0 {
		t.Fatalf("commits=%d generated=%d reply callbacks=%d errors=%v", commits, generated, replyCalls.Load(), runtimeErrs)
	}
}

func TestRunContextAndDeferredReplyQueuesAreBounded(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close(context.Background()) }()
	idle := make(chan struct{})
	runtime := newRunContextTestRuntime()
	runtime.idle = idle
	executor, err := NewToolExecutor(ToolExecutorOptions{
		ParentContext: t.Context(), Runtime: runtime, PendingUpdateCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := NewRunContext(session, NewSpeechHandle(SpeechHandleOptions{}), llm.NewFunctionCall("bounded", "slow", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	var first atomic.Bool
	attachment := &runContextAttachment{executor: executor}
	attachment.resolveFirst = func(ToolUpdate) bool { return first.CompareAndSwap(false, true) }
	if err := run.attach(attachment); err != nil {
		t.Fatal(err)
	}
	if err := run.Update(t.Context(), map[string]any{"stage": 0}); err != nil {
		t.Fatal(err)
	}
	if err := run.Update(t.Context(), map[string]any{"stage": 1}); err != nil {
		t.Fatal(err)
	}
	if err := run.Update(t.Context(), map[string]any{"stage": 2}); !errors.Is(err, ErrToolUpdatesBacklogged) {
		t.Fatalf("deferred queue overflow = %v, want %v", err, ErrToolUpdatesBacklogged)
	}
	close(idle)
	if err := executor.WaitForReplies(t.Context()); err != nil {
		t.Fatal(err)
	}
	run.detach(attachment)
	for index := len(run.Updates()); index < DefaultRunContextUpdateCapacity; index++ {
		if err := run.Update(t.Context(), index); err != nil {
			t.Fatalf("fill local update %d: %v", index, err)
		}
	}
	if err := run.Update(t.Context(), "overflow"); !errors.Is(err, ErrRunContextUpdateOverflow) {
		t.Fatalf("RunContext overflow = %v, want %v", err, ErrRunContextUpdateOverflow)
	}
	if err := executor.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRunContextFillerIsLazyBoundedAndContextOwned(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "filler"})); err != nil {
		t.Fatal(err)
	}
	handle := NewSpeechHandle(SpeechHandleOptions{})
	run, err := NewRunContext(session, handle, llm.NewFunctionCall("filler-call", "slow", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	called := make(chan int, 2)
	max := 1
	err = run.Filler(t.Context(), func(_ context.Context, step int) (FillerContent, error) {
		called <- step
		filler := NewSpeechHandle(SpeechHandleOptions{})
		filler.MarkDone(nil)
		return FillerContent{Handle: filler}, nil
	}, RunContextFillerOptions{MaxSteps: &max}, func(context.Context) error {
		select {
		case step := <-called:
			if step != 0 {
				t.Fatalf("filler step = %d", step)
			}
			return nil
		case <-time.After(2 * time.Second):
			return errors.New("filler source was not invoked")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case step := <-called:
		t.Fatalf("bounded filler ran an extra step %d", step)
	default:
	}

	zero := 0
	var mu sync.Mutex
	invocations := 0
	if err := run.Filler(t.Context(), func(context.Context, int) (FillerContent, error) {
		mu.Lock()
		invocations++
		mu.Unlock()
		return FillerContent{}, nil
	}, RunContextFillerOptions{MaxSteps: &zero}, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if invocations != 0 {
		t.Fatalf("zero-step filler invoked source %d times", invocations)
	}
}

func TestTaskToolsAutoAdvertiseCancelAndDetachLateUpdates(t *testing.T) {
	started := make(chan *RunContext[struct{}], 1)
	releaseLate := make(chan struct{})
	lateReturned := make(chan struct{})
	tool := llm.MustTool(llm.FunctionToolOptions[struct{}, string]{
		Name: "cancellable_background", Flags: llm.ToolFlagCancellable,
		Execute: func(ctx context.Context, _ struct{}, options llm.ToolOptions) (string, error) {
			run, ok := AsRunContext[struct{}](options.Context)
			if !ok {
				return "", errors.New("typed RunContext unavailable")
			}
			if err := run.Update(ctx, "started"); err != nil {
				return "", err
			}
			started <- run
			<-releaseLate // Deliberately ignore cancellation to exercise detachment.
			if err := run.Update(context.Background(), "late"); err != nil {
				return "", err
			}
			close(lateReturned)
			return "too late", nil
		},
	})
	tools, _ := llm.NewToolContext(tool)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{Tools: tools, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "tasks"})); err != nil {
		t.Fatal(err)
	}
	activity, err := session.currentActivity()
	if err != nil {
		t.Fatal(err)
	}
	visible := activity.toolsSnapshot()
	if !visible.HasTool(CancelTaskTool.Name()) || !visible.HasTool(GetRunningTasksTool.Name()) {
		t.Fatalf("task tools were not auto-advertised: %v", visible.SortedToolNames())
	}
	call := llm.NewFunctionCall("background-task", tool.Name(), `{}`)
	resultDone := make(chan []ToolExecutionResult, 1)
	go func() {
		results, _ := activity.executeFunctionTools(t.Context(), NewSpeechHandle(SpeechHandleOptions{}), []*llm.FunctionCall{call}, visible)
		resultDone <- results
	}()
	var run *RunContext[struct{}]
	select {
	case run = <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("cancellable background tool did not start")
	}
	select {
	case results := <-resultDone:
		if len(results) != 1 || !results[0].NonBlocking {
			t.Fatalf("first update result = %#v", results)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first update did not release dispatch")
	}

	managementRun, err := NewRunContext(session, NewSpeechHandle(SpeechHandleOptions{}), llm.NewFunctionCall("manage", GetRunningTasksTool.Name(), `{}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := GetRunningTasksTool.Execute(t.Context(), json.RawMessage(`{}`), llm.ToolOptions{Context: managementRun.LLMContext()})
	if err != nil {
		t.Fatal(err)
	}
	running, ok := raw.([]*llm.FunctionCall)
	if !ok || len(running) != 1 || running[0].CallID != "background-task" {
		t.Fatalf("running-task tool output = %#v", raw)
	}

	cancelRun, err := NewRunContext(session, NewSpeechHandle(SpeechHandleOptions{}), llm.NewFunctionCall("cancel", CancelTaskTool.Name(), `{"call_id":"background-task"}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, err = CancelTaskTool.Execute(t.Context(), json.RawMessage(`{"call_id":"background-task"}`), llm.ToolOptions{Context: cancelRun.LLMContext()})
	if err != nil || raw != "Task background-task cancelled successfully." {
		t.Fatalf("cancel-task output = %#v, %v", raw, err)
	}
	raw, err = GetRunningTasksTool.Execute(t.Context(), json.RawMessage(`{}`), llm.ToolOptions{Context: managementRun.LLMContext()})
	if err != nil {
		t.Fatal(err)
	}
	if afterCancel := raw.([]*llm.FunctionCall); len(afterCancel) != 0 {
		t.Fatalf("cancelled task remained cancellable: %#v", afterCancel)
	}
	if running := session.RunningTools(); len(running) != 1 {
		t.Fatalf("non-cooperative callback lost settlement visibility: %#v", running)
	}
	activity.toolExecutor.mu.Lock()
	settled := activity.toolExecutor.changed
	activity.toolExecutor.mu.Unlock()
	close(releaseLate)
	select {
	case <-lateReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("non-cooperative callback did not return")
	}
	select {
	case <-settled:
	case <-time.After(2 * time.Second):
		t.Fatal("settled task did not notify the executor")
	}
	if running := session.RunningTools(); len(running) != 0 {
		t.Fatalf("settled task remained visible: %#v", running)
	}
	if updates := run.Updates(); len(updates) != 2 {
		t.Fatalf("detached context did not retain its local update trail: %#v", updates)
	}
	for _, item := range session.ChatContext().Items() {
		if output, ok := item.(*llm.FunctionCallOutput); ok && output.CallID == "background-task_update_1" {
			t.Fatal("late update escaped a cancelled RunContext into session history")
		}
	}
}

func TestTaskToolsStayHiddenWithoutCancellableTools(t *testing.T) {
	tool := llm.MustTool(llm.FunctionToolOptions[struct{}, string]{
		Name: "blocking_only", Execute: func(context.Context, struct{}, llm.ToolOptions) (string, error) { return "ok", nil },
	})
	tools, _ := llm.NewToolContext(tool)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{Tools: tools, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "blocking"})); err != nil {
		t.Fatal(err)
	}
	activity, err := session.currentActivity()
	if err != nil {
		t.Fatal(err)
	}
	visible := activity.toolsSnapshot()
	if visible.HasTool(CancelTaskTool.Name()) || visible.HasTool(GetRunningTasksTool.Name()) {
		t.Fatalf("task tools unexpectedly advertised: %v", visible.SortedToolNames())
	}
}

func TestAsyncToolOptionPrecedenceByExecutorScope(t *testing.T) {
	makeTool := func(name string) llm.Tool {
		return llm.MustTool(llm.FunctionToolOptions[struct{}, string]{
			Name: name, Execute: func(context.Context, struct{}, llm.ToolOptions) (string, error) { return "ok", nil },
		})
	}
	sessionInherited := llm.MustAsyncToolset(llm.AsyncToolsetOptions{ID: "session_inherited", Tools: []llm.Tool{makeTool("session_inherited_tool")}})
	sessionSpecific := llm.MustAsyncToolset(llm.AsyncToolsetOptions{
		ID: "session_specific", Tools: []llm.Tool{makeTool("session_specific_tool")},
		ToolHandling: &llm.AsyncToolOptions{UpdateTemplate: "scope"},
	})
	sessionTools, _ := llm.NewToolContext(sessionInherited, sessionSpecific)
	agentInherited := llm.MustAsyncToolset(llm.AsyncToolsetOptions{ID: "agent_inherited", Tools: []llm.Tool{makeTool("agent_inherited_tool")}})
	agentTools, _ := llm.NewToolContext(agentInherited)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		Tools: sessionTools, ToolHandling: ToolHandlingOptions{Async: &AsyncToolOptions{UpdateTemplate: "session"}},
		DisableUserAwayTimeout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	agent := MustAgent(AgentOptions[struct{}]{
		ID: "options", Tools: agentTools,
		ToolHandling: ToolHandlingOptions{Async: &AsyncToolOptions{UpdateTemplate: "agent"}},
	})
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	activity, err := session.currentActivity()
	if err != nil {
		t.Fatal(err)
	}
	if got := activity.toolExecutor.toolOptionsSnapshot().UpdateTemplate; got != "agent" {
		t.Fatalf("default activity options = %q", got)
	}
	if got := activity.asyncExecutors[agentInherited].toolOptionsSnapshot().UpdateTemplate; got != "agent" {
		t.Fatalf("agent async options = %q", got)
	}
	if got := session.sessionToolExecutors[sessionInherited].toolOptionsSnapshot().UpdateTemplate; got != "session" {
		t.Fatalf("session inherited options = %q", got)
	}
	if got := session.sessionToolExecutors[sessionSpecific].toolOptionsSnapshot().UpdateTemplate; got != "scope" {
		t.Fatalf("session scope override = %q", got)
	}
}

func TestAgentUpdateToolsRebuildsAsyncRoutingAndClosesRemovedScope(t *testing.T) {
	var setupCalls, closeCalls int
	tool := llm.MustTool(llm.FunctionToolOptions[struct{}, string]{
		Name: "dynamic_async", Execute: func(context.Context, struct{}, llm.ToolOptions) (string, error) { return "ok", nil },
	})
	set := llm.MustAsyncToolset(llm.AsyncToolsetOptions{
		ID: "dynamic", Tools: []llm.Tool{tool},
		Setup: func(context.Context, llm.ToolsetContext) error { setupCalls++; return nil },
		Close: func(context.Context) error { closeCalls++; return nil },
	})
	withAsync, _ := llm.NewToolContext(set)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	agent := MustAgent(AgentOptions[struct{}]{ID: "dynamic"})
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	if err := agent.UpdateTools(t.Context(), withAsync); err != nil {
		t.Fatal(err)
	}
	activity, err := session.currentActivity()
	if err != nil {
		t.Fatal(err)
	}
	activity.mu.RLock()
	executor := activity.asyncExecutors[set]
	routed := activity.executorByTool[tool.ID()]
	activity.mu.RUnlock()
	if executor == nil || routed != executor || setupCalls != 1 {
		t.Fatalf("executor=%p routed=%p setupCalls=%d", executor, routed, setupCalls)
	}
	if err := agent.UpdateTools(t.Context(), llm.EmptyToolContext()); err != nil {
		t.Fatal(err)
	}
	executor.mu.Lock()
	closed := executor.closed
	executor.mu.Unlock()
	if !closed || closeCalls != 1 {
		t.Fatalf("removed executor closed=%v toolset closes=%d", closed, closeCalls)
	}
}
