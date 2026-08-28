// SPDX-License-Identifier: Apache-2.0

package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/voice"
)

const DefaultTaskGroupMaxExecutions = 1024

var (
	ErrTaskGroupAlreadyStarted   = errors.New("workflows: task group has already started")
	ErrTaskGroupNotComplete      = errors.New("workflows: task group is not complete")
	ErrTaskGroupExecutionLimit   = errors.New("workflows: task group execution limit exceeded")
	ErrTaskGroupSummarizerNeeded = errors.New("workflows: summarizeChatCtx requires a standard LLM or summarizer")
)

// Task is the small surface TaskGroup needs from an agent task. The adapter
// returned by AdaptAgentTask makes every *voice.AgentTask usable here while a
// custom implementation is useful for non-voice or deterministic tasks.
type Task interface {
	Run(context.Context) (any, error)
	ChatContext() *llm.ChatContext
	UpdateChatContext(context.Context, *llm.ChatContext) error
	ToolContext() *llm.Context
	UpdateTools(context.Context, *llm.Context) error
}

// TaskRunner starts a voice task in its owning session and waits for its typed
// result. It is separate from voice.AgentTask.Run because the latter only waits
// for completion; the session/runtime owns the foreground handoff.
type TaskRunner[Result, UserData any] func(context.Context, *voice.AgentTask[Result, UserData]) (Result, error)

// AgentTaskAdapter erases a voice task's result type without erasing errors or
// its chat/tool context.
type AgentTaskAdapter[Result, UserData any] struct {
	task   *voice.AgentTask[Result, UserData]
	runner TaskRunner[Result, UserData]
}

// AdaptAgentTask wraps a typed voice.AgentTask for TaskGroup. If runner is nil,
// Run waits on task.Run; callers that need the group to perform the foreground
// handoff should supply their session's task runner.
func AdaptAgentTask[Result, UserData any](task *voice.AgentTask[Result, UserData], runner TaskRunner[Result, UserData]) (*AgentTaskAdapter[Result, UserData], error) {
	if task == nil {
		return nil, errors.New("workflows: agent task is required")
	}
	return &AgentTaskAdapter[Result, UserData]{task: task, runner: runner}, nil
}

func (a *AgentTaskAdapter[Result, UserData]) Run(ctx context.Context) (any, error) {
	if a.runner != nil {
		return a.runner(ctx, a.task)
	}
	return a.task.Run(ctx)
}
func (a *AgentTaskAdapter[Result, UserData]) ChatContext() *llm.ChatContext {
	return a.task.ChatContext()
}
func (a *AgentTaskAdapter[Result, UserData]) UpdateChatContext(ctx context.Context, chat *llm.ChatContext) error {
	return a.task.UpdateChatContext(ctx, chat)
}
func (a *AgentTaskAdapter[Result, UserData]) ToolContext() *llm.Context {
	return a.task.ToolContext()
}
func (a *AgentTaskAdapter[Result, UserData]) UpdateTools(ctx context.Context, tools *llm.Context) error {
	return a.task.UpdateTools(ctx, tools)
}
func (a *AgentTaskAdapter[Result, UserData]) AgentTask() *voice.AgentTask[Result, UserData] {
	return a.task
}

// TaskGroupResult contains the last successful/error value for every completed
// task ID. A fresh map is returned to every observer.
type TaskGroupResult struct {
	TaskResults map[string]any `json:"taskResults"`
}

// TaskCompletedEvent is emitted after a child completes and its chat context
// has been merged into the group.
type TaskCompletedEvent struct {
	AgentTask Task
	TaskID    string
	Result    any
}

// ChatSummarizer replaces a chat history with its bounded summary.
type ChatSummarizer interface {
	SummarizeChatContext(context.Context, *llm.ChatContext) (*llm.ChatContext, error)
}

// ChatSummarizerFunc adapts a function to ChatSummarizer.
type ChatSummarizerFunc func(context.Context, *llm.ChatContext) (*llm.ChatContext, error)

func (f ChatSummarizerFunc) SummarizeChatContext(ctx context.Context, chat *llm.ChatContext) (*llm.ChatContext, error) {
	return f(ctx, chat)
}

type TaskGroupOptions struct {
	// Nil preserves the agents-js default (true).
	SummarizeChatCtx *bool
	ReturnExceptions bool
	ChatCtx          *llm.ChatContext
	OnTaskCompleted  func(context.Context, TaskCompletedEvent) error
	// PreserveFunctionCallHistory is exposed for parity with AgentTask even
	// though TaskGroup always retains tool results until optional summarization.
	PreserveFunctionCallHistory bool

	// LLM supplies the agents-js standard-LLM summarizer. Summarizer overrides
	// LLM and is useful for custom/realtime model stacks.
	LLM        llm.LLM
	Summarizer ChatSummarizer

	// MaxExecutions bounds regressions caused by out_of_scope. Zero selects
	// DefaultTaskGroupMaxExecutions.
	MaxExecutions int
}

type TaskFactory func() Task

type TaskRegistration struct {
	ID          string
	Description string
}

type taskFactoryInfo struct {
	factory     TaskFactory
	id          string
	description string
}

// TaskGroup runs registered tasks sequentially and lets later tasks regress to
// already-visited tasks through a generated out_of_scope tool. It is one-shot.
type TaskGroup struct {
	mu sync.RWMutex

	summarize       bool
	returnErrors    bool
	preserveHistory bool
	callback        func(context.Context, TaskCompletedEvent) error
	llm             llm.LLM
	summarizer      ChatSummarizer
	maxExecutions   int

	chat       *llm.ChatContext
	registered map[string]taskFactoryInfo
	order      []string
	configErr  error
	started    bool
	complete   bool
	result     TaskGroupResult
	err        error
}

func NewTaskGroup(options TaskGroupOptions) (*TaskGroup, error) {
	summarize := true
	if options.SummarizeChatCtx != nil {
		summarize = *options.SummarizeChatCtx
	}
	maxExecutions := options.MaxExecutions
	if maxExecutions == 0 {
		maxExecutions = DefaultTaskGroupMaxExecutions
	}
	if maxExecutions < 1 {
		return nil, errors.New("workflows: task group MaxExecutions must be positive")
	}
	chat := options.ChatCtx
	if chat == nil {
		chat = llm.EmptyChatContext()
	} else {
		chat = chat.Copy(llm.CopyOptions{})
	}
	return &TaskGroup{
		summarize: summarize, returnErrors: options.ReturnExceptions,
		preserveHistory: options.PreserveFunctionCallHistory,
		callback:        options.OnTaskCompleted, llm: options.LLM, summarizer: options.Summarizer,
		maxExecutions: maxExecutions, chat: chat, registered: make(map[string]taskFactoryInfo),
	}, nil
}

func MustTaskGroup(options TaskGroupOptions) *TaskGroup {
	group, err := NewTaskGroup(options)
	if err != nil {
		panic(err)
	}
	return group
}

// Add registers or replaces a task and returns the group for fluent setup. As
// in JavaScript Map.set, replacing an ID preserves its original ordering. Bad
// registrations are reported by Run; AddTask is the eager-validation variant.
func (g *TaskGroup) Add(factory TaskFactory, registration TaskRegistration) *TaskGroup {
	if err := g.AddTask(factory, registration); err != nil {
		g.mu.Lock()
		g.configErr = errors.Join(g.configErr, err)
		g.mu.Unlock()
	}
	return g
}

func (g *TaskGroup) AddTask(factory TaskFactory, registration TaskRegistration) error {
	if g == nil {
		return errors.New("workflows: nil task group")
	}
	if factory == nil {
		return errors.New("workflows: task factory is required")
	}
	if registration.ID == "" {
		return errors.New("workflows: task ID must not be empty")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.started {
		return ErrTaskGroupAlreadyStarted
	}
	if _, exists := g.registered[registration.ID]; !exists {
		g.order = append(g.order, registration.ID)
	}
	g.registered[registration.ID] = taskFactoryInfo{factory: factory, id: registration.ID, description: registration.Description}
	return nil
}

func (g *TaskGroup) PreserveFunctionCallHistory() bool {
	g.mu.RLock()
	value := g.preserveHistory
	g.mu.RUnlock()
	return value
}

func (g *TaskGroup) ChatContext() *llm.ChatContext {
	g.mu.RLock()
	chat := g.chat.Copy(llm.CopyOptions{})
	g.mu.RUnlock()
	return chat
}

func (g *TaskGroup) Done() bool {
	g.mu.RLock()
	done := g.complete
	g.mu.RUnlock()
	return done
}

func (g *TaskGroup) Result() (TaskGroupResult, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.complete {
		return TaskGroupResult{}, ErrTaskGroupNotComplete
	}
	return cloneTaskGroupResult(g.result), g.err
}

func cloneTaskGroupResult(result TaskGroupResult) TaskGroupResult {
	copy := TaskGroupResult{TaskResults: make(map[string]any, len(result.TaskResults))}
	for key, value := range result.TaskResults {
		copy.TaskResults[key] = value
	}
	return copy
}

func (g *TaskGroup) Run(ctx context.Context) (TaskGroupResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	g.mu.Lock()
	if g.started {
		g.mu.Unlock()
		return TaskGroupResult{}, ErrTaskGroupAlreadyStarted
	}
	g.started = true
	configErr := g.configErr
	stack := slices.Clone(g.order)
	registered := make(map[string]taskFactoryInfo, len(g.registered))
	for id, info := range g.registered {
		registered[id] = info
	}
	g.mu.Unlock()
	if configErr != nil {
		return g.finish(TaskGroupResult{}, configErr)
	}

	results := make(map[string]any, len(stack))
	visited := make(map[string]struct{}, len(stack))
	executions := 0
	for len(stack) != 0 {
		if err := context.Cause(ctx); err != nil {
			return g.finish(TaskGroupResult{TaskResults: results}, err)
		}
		executions++
		if executions > g.maxExecutions {
			return g.finish(TaskGroupResult{TaskResults: results}, ErrTaskGroupExecutionLimit)
		}
		taskID := stack[0]
		stack = stack[1:]
		info, ok := registered[taskID]
		if !ok {
			return g.finish(TaskGroupResult{TaskResults: results}, fmt.Errorf("workflows: task %q is not registered", taskID))
		}
		task, factoryErr := invokeTaskFactory(info.factory)
		if factoryErr != nil {
			return g.finish(TaskGroupResult{TaskResults: results}, fmt.Errorf("workflows: task factory %q: %w", taskID, factoryErr))
		}
		if task == nil {
			return g.finish(TaskGroupResult{TaskResults: results}, fmt.Errorf("workflows: task factory %q returned nil", taskID))
		}
		if err := task.UpdateChatContext(ctx, g.ChatContext()); err != nil {
			return g.finish(TaskGroupResult{TaskResults: results}, fmt.Errorf("workflows: update task %q chat context: %w", taskID, err))
		}

		taskCtx, cancelTask := context.WithCancelCause(ctx)
		if tool, err := buildOutOfScopeTool(taskID, visited, registered, cancelTask); err != nil {
			cancelTask(err)
			return g.finish(TaskGroupResult{TaskResults: results}, err)
		} else if tool != nil {
			tools := task.ToolContext()
			if tools == nil {
				tools = llm.EmptyToolContext()
			}
			entries := make([]any, 0, len(tools.Flatten())+1)
			for _, entry := range tools.Flatten() {
				entries = append(entries, entry)
			}
			entries = append(entries, tool)
			updated, updateErr := llm.NewToolContext(entries...)
			if updateErr != nil {
				cancelTask(updateErr)
				return g.finish(TaskGroupResult{TaskResults: results}, fmt.Errorf("workflows: build task %q tool context: %w", taskID, updateErr))
			}
			if updateErr = task.UpdateTools(ctx, updated); updateErr != nil {
				cancelTask(updateErr)
				return g.finish(TaskGroupResult{TaskResults: results}, fmt.Errorf("workflows: update task %q tools: %w", taskID, updateErr))
			}
		}

		visited[taskID] = struct{}{}
		value, runErr := invokeTaskRun(taskCtx, task)
		cause := context.Cause(taskCtx)
		cancelTask(nil)
		var scope *outOfScopeError
		if errors.As(cause, &scope) || errors.As(runErr, &scope) {
			stack = append([]string{taskID}, stack...)
			for index := len(scope.targetTaskIDs) - 1; index >= 0; index-- {
				stack = append([]string{scope.targetTaskIDs[index]}, stack...)
			}
			continue
		}

		if runErr == nil {
			g.mergeChat(task.ChatContext())
			results[taskID] = value
			if g.callback != nil {
				runErr = invokeTaskCompleted(ctx, g.callback, TaskCompletedEvent{AgentTask: task, TaskID: taskID, Result: value})
			}
		}
		if runErr != nil {
			if g.returnErrors {
				results[taskID] = runErr
				continue
			}
			return g.finish(TaskGroupResult{TaskResults: results}, runErr)
		}
	}

	if g.summarize {
		chat, err := g.summarizeChat(ctx)
		if err != nil {
			return g.finish(TaskGroupResult{TaskResults: results}, fmt.Errorf("failed to summarize the chat_ctx: %w", err))
		}
		if chat == nil {
			return g.finish(TaskGroupResult{TaskResults: results}, errors.New("failed to summarize the chat_ctx: summarizer returned nil"))
		}
		g.mu.Lock()
		g.chat = chat.Copy(llm.CopyOptions{})
		g.mu.Unlock()
	}
	return g.finish(TaskGroupResult{TaskResults: results}, nil)
}

func (g *TaskGroup) finish(result TaskGroupResult, err error) (TaskGroupResult, error) {
	result = cloneTaskGroupResult(result)
	g.mu.Lock()
	g.complete, g.result, g.err = true, result, err
	g.mu.Unlock()
	return cloneTaskGroupResult(result), err
}

func (g *TaskGroup) mergeChat(other *llm.ChatContext) {
	if other == nil {
		return
	}
	filtered := copyWithoutInstructions(other)
	g.mu.Lock()
	g.chat = g.chat.Merge(filtered, llm.CopyOptions{})
	g.mu.Unlock()
}

func copyWithoutInstructions(chat *llm.ChatContext) *llm.ChatContext {
	result := llm.EmptyChatContext()
	for _, item := range chat.Items() {
		if message, ok := item.(*llm.ChatMessage); ok {
			if message.Role == llm.RoleSystem || message.Role == llm.RoleDeveloper {
				continue
			}
			clone := message.Clone()
			clone.Content = slices.DeleteFunc(clone.Content, func(content llm.Content) bool {
				_, instructions := content.(llm.InstructionContent)
				return instructions
			})
			item = clone
		}
		_ = result.Insert(item)
	}
	return result
}

func invokeTaskCompleted(ctx context.Context, callback func(context.Context, TaskCompletedEvent) error, event TaskCompletedEvent) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("workflows: task-completed callback panicked: %v", recovered)
		}
	}()
	return callback(ctx, event)
}

func invokeTaskFactory(factory TaskFactory) (task Task, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panicked: %v", recovered)
		}
	}()
	return factory(), nil
}

func invokeTaskRun(ctx context.Context, task Task) (value any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("workflows: task panicked: %v", recovered)
		}
	}()
	return task.Run(ctx)
}

type outOfScopeError struct{ targetTaskIDs []string }

func (e *outOfScopeError) Error() string { return "out_of_scope" }

type outOfScopeInput struct {
	TaskIDs []string `json:"task_ids"`
}

func buildOutOfScopeTool(activeID string, visited map[string]struct{}, registered map[string]taskFactoryInfo, cancel context.CancelCauseFunc) (llm.Tool, error) {
	regressionIDs := make([]string, 0, len(visited))
	taskDescriptions := make(map[string]string, len(visited))
	for id, info := range registered {
		if id == activeID {
			continue
		}
		if _, ok := visited[id]; ok {
			regressionIDs = append(regressionIDs, id)
			taskDescriptions[id] = info.description
		}
	}
	if len(regressionIDs) == 0 {
		return nil, nil
	}
	slices.Sort(regressionIDs)
	descriptionJSON, _ := json.Marshal(taskDescriptions)
	description := "Call to regress to other tasks according to what the user requested to modify, return the corresponding task ids. " +
		"For example, if the user wants to change their email and there is a task with id \"email_task\" with a description of \"Collect the user's email\", return the id (\"get_email_task\"). " +
		"If the user requests to regress to multiple tasks, such as changing their phone number and email, return both task ids in the order they were requested. " +
		"The following are the IDs and their corresponding task description. " + string(descriptionJSON)
	schema, _ := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{"task_ids": map[string]any{
			"type": "array", "items": map[string]any{"type": "string", "enum": regressionIDs},
			"description": "The IDs of the tasks requested",
		}},
		"required":             []string{"task_ids"},
		"additionalProperties": false,
	})
	return llm.NewTool(llm.FunctionToolOptions[outOfScopeInput, any]{
		Name: "out_of_scope", Description: description, Parameters: schema,
		Flags: llm.ToolFlagIgnoreOnEnter,
		Validate: func(input *outOfScopeInput) error {
			if len(input.TaskIDs) == 0 {
				return errors.New("task_ids must not be empty")
			}
			for _, id := range input.TaskIDs {
				if _, ok := registered[id]; !ok {
					return fmt.Errorf("unable to regress, invalid task id %s", id)
				}
				if _, ok := visited[id]; !ok || id == activeID {
					return fmt.Errorf("unable to regress, invalid task id %s", id)
				}
			}
			return nil
		},
		Execute: func(_ context.Context, input outOfScopeInput, _ llm.ToolOptions) (any, error) {
			cancel(&outOfScopeError{targetTaskIDs: slices.Clone(input.TaskIDs)})
			return nil, nil
		},
	})
}

func (g *TaskGroup) summarizeChat(ctx context.Context) (*llm.ChatContext, error) {
	chat := g.ChatContext()
	if g.summarizer != nil {
		return invokeSummarizer(ctx, g.summarizer, chat)
	}
	if g.llm == nil {
		return nil, ErrTaskGroupSummarizerNeeded
	}
	return SummarizeChatContext(ctx, g.llm, chat)
}

func invokeSummarizer(ctx context.Context, summarizer ChatSummarizer, chat *llm.ChatContext) (result *llm.ChatContext, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("workflows: chat summarizer panicked: %v", recovered)
		}
	}()
	return summarizer.SummarizeChatContext(ctx, chat)
}

const taskGroupSummarySystemPrompt = `Compress older conversation history into a short, faithful summary.

The conversation is formatted as XML. Here is how to read it:
- <user>...</user>  - something the user said.
- <assistant>...</assistant>  - something the assistant said.
- <function_call name="..." call_id="...">...</function_call>  - the assistant invoked an action.
- <function_call_output name="..." call_id="..."></function_call_output>  - the result of that action. May contain <error>...</error> if it failed.

Guidelines:
- Distill the information learned from function call outputs into the summary. Do not mention that a tool or function was called; just preserve the knowledge gained.
- Focus on user goals, constraints, decisions, key facts, preferences, entities, and any pending or unresolved tasks.
- Omit greetings, filler, and chit-chat.
- Be concise.`

// SummarizeChatContext applies the same keepLastTurns=0 summarization used by
// agents-js TaskGroup. The source context is never mutated.
func SummarizeChatContext(ctx context.Context, model llm.LLM, chat *llm.ChatContext) (*llm.ChatContext, error) {
	if model == nil {
		return nil, ErrTaskGroupSummarizerNeeded
	}
	if chat == nil {
		chat = llm.EmptyChatContext()
	}
	items := chat.Items()
	var source strings.Builder
	preserved := make([]llm.ChatItem, 0, len(items))
	for _, item := range items {
		switch value := item.(type) {
		case *llm.ChatMessage:
			if value.Role != llm.RoleUser && value.Role != llm.RoleAssistant {
				preserved = append(preserved, item)
				continue
			}
			if summary, _ := value.Extra["is_summary"].(bool); summary {
				continue
			}
			text, ok := value.TextContent()
			text = strings.TrimSpace(text)
			if ok && text != "" {
				fmt.Fprintf(&source, "<%s>\n%s\n</%s>\n", value.Role, html.EscapeString(text), value.Role)
			}
		case *llm.FunctionCall:
			fmt.Fprintf(&source, "<function_call name=\"%s\" call_id=\"%s\">\n%s\n</function_call>\n", html.EscapeString(value.Name), html.EscapeString(value.CallID), html.EscapeString(value.Arguments))
		case *llm.FunctionCallOutput:
			body := html.EscapeString(value.Output)
			if value.IsError {
				body = "<error>" + body + "</error>"
			}
			fmt.Fprintf(&source, "<function_call_output name=\"%s\" call_id=\"%s\">\n%s\n</function_call_output>\n", html.EscapeString(value.Name), html.EscapeString(value.CallID), body)
		default:
			preserved = append(preserved, item)
		}
	}
	text := strings.TrimSpace(source.String())
	if text == "" {
		return chat.Copy(llm.CopyOptions{}), nil
	}
	prompt := llm.EmptyChatContext()
	_, _ = prompt.AddMessage(llm.RoleSystem, taskGroupSummarySystemPrompt)
	_, _ = prompt.AddMessage(llm.RoleUser, "Conversation to summarize:\n\n"+text)
	stream, err := model.Chat(ctx, llm.ChatOptions{ChatContext: prompt, ToolChoice: llm.ToolChoice{Kind: llm.ToolChoiceNone}})
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	response, err := stream.Collect(ctx)
	if err != nil {
		return nil, err
	}
	summary := strings.TrimSpace(response.Text)
	if summary == "" {
		return chat.Copy(llm.CopyOptions{}), nil
	}
	createdAt := time.Now()
	if len(items) != 0 {
		createdAt = items[len(items)-1].ItemCreatedAt().Add(time.Microsecond)
	}
	message := llm.NewChatMessage(llm.RoleAssistant, "<chat_history_summary>\n"+summary+"\n</chat_history_summary>")
	message.CreatedAt = createdAt
	message.Extra["is_summary"] = true
	preserved = append(preserved, message)
	return llm.NewChatContext(preserved...), nil
}
