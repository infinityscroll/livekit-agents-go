// SPDX-License-Identifier: Apache-2.0

package tools

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/voice"
)

const DefaultEndCallReplyTimeout = 5 * time.Second

const EndCallDescription = `
Ends the current call and disconnects immediately.

Call when:
- The user clearly indicates they are done (e.g., "that's all, bye").

Do not call when:
- The user asks to pause, hold, or transfer.
- Intent is unclear.

This is the final action the agent can take.
Once called, no further interaction is possible with the user.
Don't generate any other text or response when the tool is called.
`

// END_CALL_DESCRIPTION is the deprecated TypeScript-style constant alias.
const END_CALL_DESCRIPTION = EndCallDescription

type EndCallToolCalledEvent[UserData any] struct {
	Context   *llm.RunContext
	Arguments struct{}
}

type EndCallToolOutput struct{ Type, Value string }

type EndCallToolCompletedEvent[UserData any] struct {
	Context *llm.RunContext
	Output  *EndCallToolOutput
}

type EndCallJob interface {
	AddShutdownCallback(func(context.Context, string) error) error
	DeleteRoom(context.Context, string) error
	Shutdown(string)
}

type jobContextAdapter[UserData any] struct{ job *agents.JobContext[UserData] }

func AdaptEndCallJobContext[UserData any](job *agents.JobContext[UserData]) EndCallJob {
	if job == nil {
		return nil
	}
	return jobContextAdapter[UserData]{job: job}
}
func (a jobContextAdapter[UserData]) AddShutdownCallback(callback func(context.Context, string) error) error {
	return a.job.AddShutdownCallback(agents.ShutdownCallback(callback))
}
func (a jobContextAdapter[UserData]) DeleteRoom(ctx context.Context, room string) error {
	return a.job.DeleteRoom(ctx, room)
}
func (a jobContextAdapter[UserData]) Shutdown(reason string) { a.job.Shutdown(reason) }

type EndCallSession interface {
	Subscribe(voice.EventSubscriptionOptions) (*voice.EventSubscription, error)
	AgentState() voice.AgentState
	Close(context.Context, ...voice.CloseOptions) error
}

type EndCallToolOptions[UserData any] struct {
	ExtraDescription string
	// Nil preserves the agents-js default (true).
	DeleteRoom *bool
	// Zero inherits "say goodbye to the user". Disable maps to null.
	EndInstructions agents.Override[string]
	IgnoreOnEnter   bool
	OnToolCalled    func(context.Context, EndCallToolCalledEvent[UserData]) error
	OnToolCompleted func(context.Context, EndCallToolCompletedEvent[UserData]) error
	Job             EndCallJob
	ReplyTimeout    time.Duration
	ShutdownTimeout time.Duration
	OnError         func(error)
}

type endCallLifecycle struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	wg      sync.WaitGroup
	onError func(error)
}

func CreateEndCallTool[UserData any](options EndCallToolOptions[UserData]) (*llm.Toolset, error) {
	deleteRoom := true
	if options.DeleteRoom != nil {
		deleteRoom = *options.DeleteRoom
	}
	replyTimeout := options.ReplyTimeout
	if replyTimeout == 0 {
		replyTimeout = DefaultEndCallReplyTimeout
	}
	shutdownTimeout := options.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = voice.DefaultSessionShutdownTimeout
	}
	if replyTimeout < 0 || shutdownTimeout < 0 {
		return nil, errors.New("beta/tools: end-call timeouts must not be negative")
	}
	endInstructions, hasInstructions := options.EndInstructions.Resolve("say goodbye to the user", true)
	lifecycleCtx, cancel := context.WithCancelCause(context.Background())
	lifecycle := &endCallLifecycle{ctx: lifecycleCtx, cancel: cancel, onError: options.OnError}
	flags := llm.ToolFlagNone
	if options.IgnoreOnEnter {
		flags = llm.ToolFlagIgnoreOnEnter
	}
	tool, err := llm.NewTool(llm.FunctionToolOptions[struct{}, any]{
		Name: "end_call", Description: EndCallDescription + "\n" + options.ExtraDescription, Flags: flags,
		Execute: func(ctx context.Context, _ struct{}, toolOptions llm.ToolOptions) (any, error) {
			if toolOptions.Context == nil {
				return nil, errors.New("beta/tools: end_call requires a tool run context")
			}
			session, ok := toolOptions.Context.Session.(EndCallSession)
			if !ok || session == nil {
				return nil, errors.New("beta/tools: end_call requires a compatible AgentSession")
			}
			sub, err := session.Subscribe(voice.EventSubscriptionOptions{Capacity: 16})
			if err != nil {
				return nil, fmt.Errorf("beta/tools: subscribe session events: %w", err)
			}
			armed := make(chan struct{})
			defer close(armed)
			lifecycle.wg.Add(1)
			go func() {
				defer lifecycle.wg.Done()
				defer sub.Close()
				lifecycle.finish(session, sub, options.Job, deleteRoom, hasInstructions, replyTimeout, shutdownTimeout, armed)
			}()
			called := EndCallToolCalledEvent[UserData]{Context: toolOptions.Context}
			if options.OnToolCalled != nil {
				if err = invokeEndCallCallback(func() error { return options.OnToolCalled(ctx, called) }); err != nil {
					return nil, err
				}
			}
			var output *EndCallToolOutput
			if hasInstructions {
				output = &EndCallToolOutput{Type: "output", Value: endInstructions}
			}
			if options.OnToolCompleted != nil {
				completed := EndCallToolCompletedEvent[UserData]{Context: toolOptions.Context, Output: output}
				if err = invokeEndCallCallback(func() error { return options.OnToolCompleted(ctx, completed) }); err != nil {
					return nil, err
				}
			}
			if !hasInstructions {
				return nil, nil
			}
			return endInstructions, nil
		},
	})
	if err != nil {
		cancel(err)
		return nil, err
	}
	return llm.NewToolset(llm.ToolsetOptions{
		ID: "end_call", Tools: []llm.Tool{tool},
		Close: func(ctx context.Context) error {
			cancel(errors.New("beta/tools: end_call toolset closed"))
			done := make(chan struct{})
			go func() { lifecycle.wg.Wait(); close(done) }()
			if ctx == nil {
				ctx = context.Background()
			}
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		},
	})
}

func MustEndCallTool[UserData any](options EndCallToolOptions[UserData]) *llm.Toolset {
	tool, err := CreateEndCallTool(options)
	if err != nil {
		panic(err)
	}
	return tool
}

// NewEndCallTool is the Go constructor alias for CreateEndCallTool.
func NewEndCallTool[UserData any](options EndCallToolOptions[UserData]) (*llm.Toolset, error) {
	return CreateEndCallTool(options)
}

func (l *endCallLifecycle) finish(session EndCallSession, sub *voice.EventSubscription, job EndCallJob, deleteRoom, waitReply bool, replyTimeout, shutdownTimeout time.Duration, armed <-chan struct{}) {
	select {
	case <-armed:
	case <-l.ctx.Done():
		return
	}
	if waitReply {
		ctx, cancel := context.WithTimeout(l.ctx, replyTimeout)
		for {
			event, err := sub.Recv(ctx)
			if err != nil {
				break
			}
			switch value := event.(type) {
			case voice.AgentStateChangedEvent:
				if value.NewState == voice.AgentStateIdle || value.NewState == voice.AgentStateListening {
					cancel()
					goto shutdown
				}
			case voice.CloseEvent:
				cancel()
				l.finishJob(job, deleteRoom, string(value.Reason))
				return
			}
		}
		cancel()
		if context.Cause(l.ctx) != nil {
			return
		}
	}
shutdown:
	closeCtx, closeCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	err := session.Close(closeCtx, voice.CloseOptions{Reason: voice.CloseReasonUserInitiated})
	closeCancel()
	if err != nil {
		l.report(fmt.Errorf("beta/tools: close session: %w", err))
	}
	l.finishJob(job, deleteRoom, string(voice.CloseReasonUserInitiated))
}

func (l *endCallLifecycle) finishJob(job EndCallJob, deleteRoom bool, reason string) {
	if job == nil {
		return
	}
	if deleteRoom {
		if err := job.AddShutdownCallback(func(ctx context.Context, _ string) error { return job.DeleteRoom(ctx, "") }); err != nil {
			l.report(err)
		}
	}
	job.Shutdown(reason)
}

func (l *endCallLifecycle) report(err error) {
	if err == nil || l.onError == nil {
		return
	}
	defer func() { _ = recover() }()
	l.onError(err)
}

func invokeEndCallCallback(callback func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("beta/tools: end-call callback panicked: %v", recovered)
		}
	}()
	return callback()
}
