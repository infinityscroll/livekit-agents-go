// SPDX-License-Identifier: Apache-2.0

package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/livekit/agents-go/llm"
)

type taskGroupTestTask struct {
	mu    sync.Mutex
	chat  *llm.ChatContext
	tools *llm.Context
	run   func(context.Context, *taskGroupTestTask) (any, error)
}

func newTaskGroupTestTask(run func(context.Context, *taskGroupTestTask) (any, error)) *taskGroupTestTask {
	return &taskGroupTestTask{chat: llm.EmptyChatContext(), tools: llm.EmptyToolContext(), run: run}
}
func (t *taskGroupTestTask) Run(ctx context.Context) (any, error) { return t.run(ctx, t) }
func (t *taskGroupTestTask) ChatContext() *llm.ChatContext {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.chat.Copy(llm.CopyOptions{})
}
func (t *taskGroupTestTask) UpdateChatContext(_ context.Context, chat *llm.ChatContext) error {
	t.mu.Lock()
	t.chat = chat.Copy(llm.CopyOptions{})
	t.mu.Unlock()
	return nil
}
func (t *taskGroupTestTask) ToolContext() *llm.Context {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tools.Copy()
}
func (t *taskGroupTestTask) UpdateTools(_ context.Context, tools *llm.Context) error {
	t.mu.Lock()
	t.tools = tools.Copy()
	t.mu.Unlock()
	return nil
}

func boolPointer(value bool) *bool { return &value }

func TestTaskGroupRunsInOrderMergesChatAndCallsCallback(t *testing.T) {
	group, err := NewTaskGroup(TaskGroupOptions{
		SummarizeChatCtx: boolPointer(false), ChatCtx: llm.NewChatContext(llm.NewChatMessage(llm.RoleUser, "start")),
	})
	if err != nil {
		t.Fatal(err)
	}
	var order, callbacks []string
	group.callback = func(_ context.Context, event TaskCompletedEvent) error {
		callbacks = append(callbacks, event.TaskID)
		return nil
	}
	for _, id := range []string{"first", "second"} {
		id := id
		group.Add(func() Task {
			return newTaskGroupTestTask(func(_ context.Context, task *taskGroupTestTask) (any, error) {
				order = append(order, id)
				if task.ChatContext().Len() != len(order) {
					t.Fatalf("%s did not receive merged context", id)
				}
				_, _ = task.chat.AddMessage(llm.RoleAssistant, id)
				return id + "-result", nil
			})
		}, TaskRegistration{ID: id, Description: id})
	}
	result, err := group.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"first", "second"}) || !reflect.DeepEqual(callbacks, order) {
		t.Fatalf("order=%v callbacks=%v", order, callbacks)
	}
	if result.TaskResults["first"] != "first-result" || result.TaskResults["second"] != "second-result" {
		t.Fatalf("unexpected result %#v", result)
	}
	if group.ChatContext().Len() != 3 {
		t.Fatalf("merged chat length=%d", group.ChatContext().Len())
	}
	result.TaskResults["first"] = "mutated"
	stored, err := group.Result()
	if err != nil {
		t.Fatal(err)
	}
	if stored.TaskResults["first"] != "first-result" {
		t.Fatal("Result exposed mutable map")
	}
	if _, err = group.Run(t.Context()); !errors.Is(err, ErrTaskGroupAlreadyStarted) {
		t.Fatalf("second Run error=%v", err)
	}
}

func TestTaskGroupOutOfScopeRegressesVisitedTasksInRequestedOrder(t *testing.T) {
	group, err := NewTaskGroup(TaskGroupOptions{SummarizeChatCtx: boolPointer(false), MaxExecutions: 8})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var order []string
	counts := map[string]int{}
	for _, id := range []string{"name", "email", "confirm"} {
		id := id
		group.Add(func() Task {
			return newTaskGroupTestTask(func(ctx context.Context, task *taskGroupTestTask) (any, error) {
				mu.Lock()
				counts[id]++
				count := counts[id]
				order = append(order, id)
				mu.Unlock()
				if id == "confirm" && count == 1 {
					tool, ok := task.ToolContext().FunctionTool("out_of_scope")
					if !ok {
						t.Fatal("missing out_of_scope")
					}
					_, execErr := tool.Execute(ctx, json.RawMessage(`{"task_ids":["email","name"]}`), llm.ToolOptions{})
					if execErr != nil {
						return nil, execErr
					}
					<-ctx.Done()
					return nil, context.Cause(ctx)
				}
				return id + "-" + string(rune('0'+count)), nil
			})
		}, TaskRegistration{ID: id, Description: "collect " + id})
	}
	result, err := group.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"name", "email", "confirm", "email", "name", "confirm"}) {
		t.Fatalf("execution order=%v", order)
	}
	if result.TaskResults["name"] != "name-2" || result.TaskResults["email"] != "email-2" || result.TaskResults["confirm"] != "confirm-2" {
		t.Fatalf("unexpected results %#v", result.TaskResults)
	}
}

func TestTaskGroupReturnExceptionsAndExecutionBound(t *testing.T) {
	want := errors.New("task failed")
	group, _ := NewTaskGroup(TaskGroupOptions{SummarizeChatCtx: boolPointer(false), ReturnExceptions: true})
	group.Add(func() Task {
		return newTaskGroupTestTask(func(context.Context, *taskGroupTestTask) (any, error) { return nil, want })
	}, TaskRegistration{ID: "bad"})
	group.Add(func() Task {
		return newTaskGroupTestTask(func(context.Context, *taskGroupTestTask) (any, error) { return 42, nil })
	}, TaskRegistration{ID: "good"})
	result, err := group.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(result.TaskResults["bad"].(error), want) || result.TaskResults["good"] != 42 {
		t.Fatalf("unexpected %#v", result.TaskResults)
	}

	bounded, _ := NewTaskGroup(TaskGroupOptions{SummarizeChatCtx: boolPointer(false), MaxExecutions: 2})
	bounded.Add(func() Task {
		return newTaskGroupTestTask(func(context.Context, *taskGroupTestTask) (any, error) { return nil, nil })
	}, TaskRegistration{ID: "a"})
	bounded.Add(func() Task {
		return newTaskGroupTestTask(func(ctx context.Context, task *taskGroupTestTask) (any, error) {
			tool, _ := task.ToolContext().FunctionTool("out_of_scope")
			_, _ = tool.Execute(ctx, json.RawMessage(`{"task_ids":["a"]}`), llm.ToolOptions{})
			<-ctx.Done()
			return nil, context.Cause(ctx)
		})
	}, TaskRegistration{ID: "b"})
	if _, err = bounded.Run(t.Context()); !errors.Is(err, ErrTaskGroupExecutionLimit) {
		t.Fatalf("bound error=%v", err)
	}
}

func TestTaskGroupSummarizerAndInstructionFiltering(t *testing.T) {
	childSystem := llm.NewChatMessage(llm.RoleSystem, "secret child prompt")
	childUser := llm.NewChatMessage(llm.RoleUser, "kept")
	var summarized *llm.ChatContext
	group, err := NewTaskGroup(TaskGroupOptions{
		Summarizer: ChatSummarizerFunc(func(_ context.Context, chat *llm.ChatContext) (*llm.ChatContext, error) {
			summarized = chat.Copy(llm.CopyOptions{})
			return llm.NewChatContext(llm.NewChatMessage(llm.RoleAssistant, "summary")), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	group.Add(func() Task {
		return newTaskGroupTestTask(func(_ context.Context, task *taskGroupTestTask) (any, error) {
			task.chat = llm.NewChatContext(childSystem, childUser)
			return true, nil
		})
	}, TaskRegistration{ID: "one"})
	if _, err = group.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if summarized == nil || summarized.Len() != 1 {
		t.Fatalf("summarizer input %#v", summarized)
	}
	if message := summarized.Items()[0].(*llm.ChatMessage); message.Role != llm.RoleUser {
		t.Fatalf("child instructions leaked: %#v", message)
	}
	if text, _ := group.ChatContext().Items()[0].(*llm.ChatMessage).TextContent(); text != "summary" {
		t.Fatalf("summary=%q", text)
	}
}

func TestTaskGroupCallbackPanicIsError(t *testing.T) {
	group, _ := NewTaskGroup(TaskGroupOptions{SummarizeChatCtx: boolPointer(false), OnTaskCompleted: func(context.Context, TaskCompletedEvent) error { panic("boom") }})
	group.Add(func() Task {
		return newTaskGroupTestTask(func(context.Context, *taskGroupTestTask) (any, error) { return true, nil })
	}, TaskRegistration{ID: "task"})
	if _, err := group.Run(t.Context()); err == nil || !stringsContains(err.Error(), "panicked") {
		t.Fatalf("callback error=%v", err)
	}
}

func stringsContains(value, part string) bool {
	for index := 0; index+len(part) <= len(value); index++ {
		if value[index:index+len(part)] == part {
			return true
		}
	}
	return false
}
