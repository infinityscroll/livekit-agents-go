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

	"github.com/livekit/agents-go/llm"
	voicetest "github.com/livekit/agents-go/voice/testing"
)

const (
	DefaultRunOutputRetries     = 2
	DefaultRunSettleDelay       = 5 * time.Millisecond
	DefaultRunEventCapacity     = 256
	DefaultRunRetryInstructions = "You have not provided the final output yet. Call the appropriate function to do so; a plain text response alone is not enough."
)

var (
	ErrNestedRun               = errors.New("voice nested runs are not supported")
	ErrUnexpectedModelBehavior = errors.New("voice unexpected model behavior")
)

// UnexpectedModelBehaviorError reports a missing or invalid structured test
// output after the configured retries have been exhausted.
type UnexpectedModelBehaviorError struct {
	Message string
	Cause   error
}

func (e *UnexpectedModelBehaviorError) Error() string {
	if e == nil || e.Message == "" {
		return ErrUnexpectedModelBehavior.Error()
	}
	return e.Message
}

func (e *UnexpectedModelBehaviorError) Unwrap() []error {
	if e == nil || e.Cause == nil {
		return []error{ErrUnexpectedModelBehavior}
	}
	return []error{ErrUnexpectedModelBehavior, e.Cause}
}

type RunOutputOptions struct {
	// MaxRetriesSet distinguishes an intentional zero from the default of two.
	MaxRetriesSet     bool
	MaxRetries        int
	RetryInstructions string
}

type RunOptions struct {
	UserInput     string
	InputModality InputModality
	Output        *RunOutputOptions
	EventCapacity int
	SettleDelay   time.Duration
}

type StructuredRunOptions[Output any] struct {
	RunOptions
	// Validate may coerce a task result. Nil performs a strict Go type check.
	Validate func(any) (Output, error)
}

type activeSessionRun interface {
	Watch(*SpeechHandle)
	Done() bool
	Reject(error)
}

type watchedRunHandle struct {
	handle     *SpeechHandle
	done       bool
	removeItem func()
	removeDone func()
}

type sessionRun[Output, UserData any] struct {
	session   *AgentSession[UserData]
	result    *voicetest.RunResult[Output]
	expected  bool
	validate  func(any) (Output, error)
	retries   int
	retryText string
	delay     time.Duration

	ctx    context.Context
	cancel context.CancelCauseFunc
	sub    *EventSubscription

	mu         sync.Mutex
	handles    map[string]*watchedRunHandle
	version    uint64
	finished   bool
	lastAgent  *Agent[UserData]
	finishOnce sync.Once
}

// Run starts one deterministic test turn and records messages, function calls,
// function outputs, and handoffs in creation order. It is safe without a room.
func (s *AgentSession[UserData]) Run(ctx context.Context, options RunOptions) (*voicetest.RunResult[any], error) {
	return startSessionRun[any](ctx, s, options, false, nil)
}

// RunStructured is the typed Go counterpart of run({outputType}). The current
// AgentTask result is validated after speech/tool completion; only a missing
// result is re-prompted.
func RunStructured[Output, UserData any](ctx context.Context, session *AgentSession[UserData], options StructuredRunOptions[Output]) (*voicetest.RunResult[Output], error) {
	validator := options.Validate
	if validator == nil {
		validator = strictOutputValidator[Output]
	}
	return startSessionRun(ctx, session, options.RunOptions, true, validator)
}

func strictOutputValidator[Output any](value any) (Output, error) {
	if typed, ok := value.(Output); ok {
		return typed, nil
	}
	var zero Output
	want := reflect.TypeFor[Output]()
	return zero, fmt.Errorf("expected output %v, got %T", want, value)
}

func startSessionRun[Output, UserData any](ctx context.Context, session *AgentSession[UserData], options RunOptions, expected bool, validate func(any) (Output, error)) (*voicetest.RunResult[Output], error) {
	if session == nil {
		return nil, errors.New("voice run requires a session")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if options.InputModality == "" {
		options.InputModality = InputModalityText
	}
	if options.InputModality != InputModalityText && options.InputModality != InputModalityAudio {
		return nil, errors.New("voice run input modality must be text or audio")
	}
	capacity := options.EventCapacity
	if capacity <= 0 {
		capacity = DefaultRunEventCapacity
	}
	delay := options.SettleDelay
	if delay == 0 {
		delay = DefaultRunSettleDelay
	}
	if delay < 0 {
		return nil, errors.New("voice run settle delay must not be negative")
	}
	retries, retryText := 0, DefaultRunRetryInstructions
	if expected {
		retries = DefaultRunOutputRetries
		if options.Output != nil {
			if options.Output.MaxRetriesSet {
				retries = options.Output.MaxRetries
			} else if options.Output.MaxRetries != 0 {
				retries = options.Output.MaxRetries
			}
			if options.Output.RetryInstructions != "" {
				retryText = options.Output.RetryInstructions
			}
		}
		if retries < 0 {
			return nil, errors.New("voice run output retries must not be negative")
		}
	}

	runCtx, cancel := context.WithCancelCause(session.ctx)
	run := &sessionRun[Output, UserData]{
		session: session, result: voicetest.NewRunResult[Output](options.UserInput),
		expected: expected, validate: validate, retries: retries, retryText: retryText,
		delay: delay, ctx: runCtx, cancel: cancel, handles: make(map[string]*watchedRunHandle),
		lastAgent: session.Agent(),
	}

	session.runMu.Lock()
	if session.run != nil && !session.run.Done() {
		session.runMu.Unlock()
		cancel(ErrNestedRun)
		return nil, ErrNestedRun
	}
	session.run = run
	session.runMu.Unlock()

	subscription, err := session.Subscribe(EventSubscriptionOptions{Capacity: capacity})
	if err != nil {
		run.finish(zeroValue[Output](), false, err)
		return nil, err
	}
	run.sub = subscription
	go run.consumeEvents()

	handle, err := session.GenerateReply(ctx, GenerateReplyOptions{UserInput: options.UserInput, InputModality: options.InputModality})
	if err != nil {
		run.finish(zeroValue[Output](), false, err)
		return run.result, err
	}
	run.Watch(handle)
	return run.result, nil
}

func zeroValue[T any]() (value T) { return value }

func (s *AgentSession[UserData]) watchActiveRun(handle *SpeechHandle) {
	if handle == nil {
		return
	}
	s.runMu.Lock()
	run := s.run
	s.runMu.Unlock()
	if run != nil && !run.Done() {
		run.Watch(handle)
	}
}

func (r *sessionRun[Output, UserData]) Done() bool { return r.result.Done() }
func (r *sessionRun[Output, UserData]) Reject(err error) {
	r.finish(zeroValue[Output](), false, err)
}

func (r *sessionRun[Output, UserData]) Watch(handle *SpeechHandle) {
	if handle == nil {
		return
	}
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return
	}
	if _, exists := r.handles[handle.ID()]; exists {
		r.mu.Unlock()
		return
	}
	r.version++
	tracked := &watchedRunHandle{handle: handle}
	r.handles[handle.ID()] = tracked
	tracked.removeItem = handle.AddItemCallback(r.recordItem)
	tracked.removeDone = handle.AddDoneCallback(func(value *SpeechHandle) { r.handleDone(value) })
	r.mu.Unlock()

	for _, item := range handle.ChatItems() {
		r.recordItem(item)
	}
}

func (r *sessionRun[Output, UserData]) recordItem(item llm.ChatItem) {
	if handoff, ok := item.(*llm.AgentHandoffItem); ok {
		r.recordHandoff(handoff)
		return
	}
	r.result.Record(item)
}

func (r *sessionRun[Output, UserData]) recordHandoff(item *llm.AgentHandoffItem) {
	if item == nil {
		return
	}
	r.mu.Lock()
	oldAgent := r.lastAgent
	newAgent := r.session.Agent()
	if newAgent != nil {
		r.lastAgent = newAgent
	}
	r.mu.Unlock()
	r.result.RecordHandoff(item, oldAgent, newAgent)
}

func (r *sessionRun[Output, UserData]) consumeEvents() {
	defer func() {
		if !r.Done() && context.Cause(r.ctx) != nil && !errors.Is(context.Cause(r.ctx), context.Canceled) {
			r.Reject(context.Cause(r.ctx))
		}
	}()
	for {
		event, err := r.sub.Recv(r.ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) && !r.Done() && context.Cause(r.ctx) == nil {
				r.Reject(err)
			}
			return
		}
		switch value := event.(type) {
		case ConversationItemAddedEvent:
			r.recordItem(value.Item)
		case *ConversationItemAddedEvent:
			if value != nil {
				r.recordItem(value.Item)
			}
		case SpeechCreatedEvent:
			r.Watch(value.SpeechHandle)
		case *SpeechCreatedEvent:
			if value != nil {
				r.Watch(value.SpeechHandle)
			}
		case CloseEvent:
			if value.Err != nil {
				r.Reject(value.Err)
			} else {
				r.Reject(ErrSessionClosed)
			}
		case *CloseEvent:
			if value != nil && value.Err != nil {
				r.Reject(value.Err)
			} else {
				r.Reject(ErrSessionClosed)
			}
		}
	}
}

func (r *sessionRun[Output, UserData]) handleDone(handle *SpeechHandle) {
	r.mu.Lock()
	tracked := r.handles[handle.ID()]
	if tracked == nil || tracked.done || r.finished {
		r.mu.Unlock()
		return
	}
	tracked.done = true
	r.version++
	version := r.version
	allDone := len(r.handles) != 0
	for _, candidate := range r.handles {
		allDone = allDone && candidate.done
	}
	r.mu.Unlock()
	if allDone {
		go r.settle(version)
	}
}

func (r *sessionRun[Output, UserData]) settle(version uint64) {
	if r.delay > 0 {
		timer := time.NewTimer(r.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.ctx.Done():
			return
		}
	}
	r.mu.Lock()
	if r.finished || r.version != version {
		r.mu.Unlock()
		return
	}
	var handleErr error
	for _, tracked := range r.handles {
		if !tracked.done {
			r.mu.Unlock()
			return
		}
		if err := tracked.handle.Error(); err != nil && !errors.Is(err, ErrSpeechNotDone) {
			handleErr = errors.Join(handleErr, err)
		}
	}
	r.mu.Unlock()
	if handleErr != nil {
		r.Reject(handleErr)
		return
	}

	agent := r.session.Agent()
	var raw any
	var complete bool
	var taskErr error
	if agent != nil {
		raw, complete, taskErr = agent.taskOutput()
	}
	if taskErr != nil {
		r.Reject(taskErr)
		return
	}
	if !r.expected {
		if !complete {
			r.finish(zeroValue[Output](), false, nil)
			return
		}
		output, ok := raw.(Output)
		if !ok {
			r.finish(zeroValue[Output](), false, nil)
			return
		}
		r.finish(output, true, nil)
		return
	}
	if !complete {
		if r.retryMissingOutput() {
			return
		}
		r.Reject(&UnexpectedModelBehaviorError{Message: "expected structured output, but the agent task did not complete"})
		return
	}
	output, err := r.validate(raw)
	if err != nil {
		r.Reject(&UnexpectedModelBehaviorError{Message: "structured output validation failed: " + err.Error(), Cause: err})
		return
	}
	r.finish(output, true, nil)
}

func (r *sessionRun[Output, UserData]) retryMissingOutput() bool {
	r.mu.Lock()
	if r.finished || r.retries <= 0 {
		r.mu.Unlock()
		return false
	}
	r.retries--
	r.version++
	r.mu.Unlock()
	instructions := llm.NewInstructions(r.retryText, "")
	handle, err := r.session.GenerateReply(r.ctx, GenerateReplyOptions{Instructions: &instructions, InputModality: InputModalityText})
	if err != nil {
		r.Reject(&UnexpectedModelBehaviorError{Message: "structured output retry could not start", Cause: err})
		return true
	}
	r.Watch(handle)
	return true
}

func (r *sessionRun[Output, UserData]) finish(output Output, hasOutput bool, err error) {
	r.finishOnce.Do(func() {
		r.mu.Lock()
		r.finished = true
		tracked := make([]*watchedRunHandle, 0, len(r.handles))
		for _, handle := range r.handles {
			tracked = append(tracked, handle)
		}
		r.mu.Unlock()
		for _, handle := range tracked {
			if handle.removeItem != nil {
				handle.removeItem()
			}
			if handle.removeDone != nil {
				handle.removeDone()
			}
		}
		if r.sub != nil {
			_ = r.sub.Close()
		}
		r.cancel(context.Canceled)
		r.session.runMu.Lock()
		if r.session.run == r {
			r.session.run = nil
		}
		r.session.runMu.Unlock()
		r.result.Complete(output, hasOutput, err)
	})
}

var _ activeSessionRun = (*sessionRun[any, struct{}])(nil)
