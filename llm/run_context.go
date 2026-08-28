// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"errors"
	"time"
)

var ErrRunContextUnavailable = errors.New("tool run context is not attached to a voice runtime")

type RunContextUpdateOptions struct {
	Template string
	// TemplateSet distinguishes an explicit empty template (deliver the raw
	// message) from an omitted template (inherit the executor default).
	TemplateSet  bool
	TemplateFunc func(UpdatePromptArgs) string
}

type RunContextFillerOptions struct {
	Delay    time.Duration
	Interval *time.Duration
	MaxSteps *int
}

// RunContextRuntime is implemented by voice.RunContext. It keeps llm free of
// a voice import cycle while preserving direct ToolOptions.Context methods.
type RunContextRuntime interface {
	ToolCurrentSpeechHandle() any
	ToolWaitForPlayout(context.Context) error
	ToolDisallowInterruptions() error
	ToolUpdate(context.Context, any, RunContextUpdateOptions) error
	ToolForeground(context.Context, func(context.Context, any) error) error
	ToolFiller(context.Context, any, RunContextFillerOptions, func(context.Context) error) error
}

func NewRunContext(userData any, call *FunctionCall, session any, runtime RunContextRuntime) *RunContext {
	callID := ""
	if call != nil {
		callID = call.CallID
	}
	return &RunContext{UserData: userData, ToolCallID: callID, Session: session, FunctionCall: call, runtime: runtime}
}

// Runtime exposes the cycle-breaking adapter for SDK integrations. Tool code
// should use the direct RunContext methods instead.
func (r *RunContext) Runtime() RunContextRuntime {
	if r == nil {
		return nil
	}
	return r.runtime
}

func (r *RunContext) CurrentSpeechHandle() any {
	if r == nil || r.runtime == nil {
		return nil
	}
	return r.runtime.ToolCurrentSpeechHandle()
}

func (r *RunContext) WaitForPlayout(ctx context.Context) error {
	if r == nil || r.runtime == nil {
		return ErrRunContextUnavailable
	}
	return r.runtime.ToolWaitForPlayout(ctx)
}

func (r *RunContext) DisallowInterruptions() error {
	if r == nil || r.runtime == nil {
		return ErrRunContextUnavailable
	}
	return r.runtime.ToolDisallowInterruptions()
}

func (r *RunContext) Update(ctx context.Context, message any, options ...RunContextUpdateOptions) error {
	if r == nil || r.runtime == nil {
		return ErrRunContextUnavailable
	}
	if len(options) > 1 {
		return errors.New("RunContext.Update accepts at most one options value")
	}
	var option RunContextUpdateOptions
	if len(options) == 1 {
		option = options[0]
	}
	return r.runtime.ToolUpdate(ctx, message, option)
}

func (r *RunContext) Foreground(ctx context.Context, fn func(context.Context, any) error) error {
	if r == nil || r.runtime == nil {
		return ErrRunContextUnavailable
	}
	if fn == nil {
		return errors.New("RunContext.Foreground callback is required")
	}
	return r.runtime.ToolForeground(ctx, fn)
}

// Filler accepts a string or the voice package's typed FillerSource callback.
func (r *RunContext) Filler(ctx context.Context, source any, options RunContextFillerOptions, fn func(context.Context) error) error {
	if r == nil || r.runtime == nil {
		return ErrRunContextUnavailable
	}
	if fn == nil {
		return errors.New("RunContext.Filler callback is required")
	}
	return r.runtime.ToolFiller(ctx, source, options, fn)
}
