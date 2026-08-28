// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"slices"
)

// AsyncToolOptions contains the conversational templates used when a tool
// detaches from its originating reply after reporting progress. Empty fields
// inherit the voice session/activity defaults.
type AsyncToolOptions struct {
	UpdateTemplate                string
	UpdateTemplateSet             bool
	UpdateTemplateFunc            func(UpdatePromptArgs) string
	DuplicateRejectTemplate       string
	DuplicateRejectTemplateSet    bool
	DuplicateRejectTemplateFunc   func(DuplicatePromptArgs) string
	DuplicateConfirmTemplate      string
	DuplicateConfirmTemplateSet   bool
	DuplicateConfirmTemplateFunc  func(DuplicatePromptArgs) string
	ReplyAtTailTemplate           string
	ReplyAtTailTemplateSet        bool
	ReplyAtTailTemplateFunc       func(ReplyPromptArgs) string
	ReplyMaybeCoveredTemplate     string
	ReplyMaybeCoveredTemplateSet  bool
	ReplyMaybeCoveredTemplateFunc func(ReplyPromptArgs) string
}

// UpdatePromptArgs is supplied to a typed update-template callback. String
// templates use the equivalent {functionName}, {callId}, and {message}
// placeholders.
type UpdatePromptArgs struct {
	FunctionName string
	CallID       string
	Message      string
}

// DuplicatePromptArgs is supplied to duplicate-template callbacks.
type DuplicatePromptArgs struct {
	FunctionName      string
	FunctionCallsJSON []string
	FunctionCallsText string
}

// ReplyPromptArgs is supplied to deferred-reply template callbacks.
type ReplyPromptArgs struct {
	CallIDs []string
}

// AsyncToolsetOptions creates an executor scope. Tools in a session-level
// AsyncToolset share a session-lifetime executor and therefore survive agent
// handoffs; agent-level sets are scoped to that activity.
type AsyncToolsetOptions struct {
	ID           string
	Tools        []Tool
	Setup        func(context.Context, ToolsetContext) error
	Close        func(context.Context) error
	ToolHandling *AsyncToolOptions
}

// AsyncToolset is a Toolset marker with an independent execution scope. The
// voice package supplies the executor so llm remains free of an import cycle.
type AsyncToolset struct {
	*Toolset
	toolHandling *AsyncToolOptions
}

func NewAsyncToolset(options AsyncToolsetOptions) (*AsyncToolset, error) {
	base, err := NewToolset(ToolsetOptions{
		ID: options.ID, Tools: slices.Clone(options.Tools), Setup: options.Setup, Close: options.Close,
	})
	if err != nil {
		return nil, err
	}
	result := &AsyncToolset{Toolset: base, toolHandling: cloneAsyncToolOptions(options.ToolHandling)}
	base.async = result
	return result, nil
}

func MustAsyncToolset(options AsyncToolsetOptions) *AsyncToolset {
	result, err := NewAsyncToolset(options)
	if err != nil {
		panic(err)
	}
	return result
}

// ToolHandling returns an independent copy of this scope's option override.
func (t *AsyncToolset) ToolHandling() *AsyncToolOptions {
	if t == nil {
		return nil
	}
	return cloneAsyncToolOptions(t.toolHandling)
}

func cloneAsyncToolOptions(options *AsyncToolOptions) *AsyncToolOptions {
	if options == nil {
		return nil
	}
	copy := *options
	return &copy
}
