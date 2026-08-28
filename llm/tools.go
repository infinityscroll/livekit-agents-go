// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
)

const ConfirmDuplicateParam = "lk_agents_confirm_duplicate"

type ToolType string

const (
	ToolTypeFunction ToolType = "function"
	ToolTypeProvider ToolType = "provider"
)

type DuplicateMode string

const (
	DuplicateAllow   DuplicateMode = "allow"
	DuplicateReject  DuplicateMode = "reject"
	DuplicateReplace DuplicateMode = "replace"
	DuplicateConfirm DuplicateMode = "confirm"
)

type ToolFlag uint32

const (
	ToolFlagNone          ToolFlag = 0
	ToolFlagIgnoreOnEnter ToolFlag = 1 << 0
	ToolFlagCancellable   ToolFlag = 1 << 1
)

type ToolError struct{ Message string }

func (e *ToolError) Error() string { return e.Message }

type ToolChoiceKind string

const (
	ToolChoiceAuto     ToolChoiceKind = "auto"
	ToolChoiceNone     ToolChoiceKind = "none"
	ToolChoiceRequired ToolChoiceKind = "required"
	ToolChoiceFunction ToolChoiceKind = "function"
)

type ToolChoice struct {
	Kind ToolChoiceKind
	Name string
}

type RunContext struct {
	UserData     any
	ToolCallID   string
	Session      any
	FunctionCall *FunctionCall
	runtime      RunContextRuntime
}

type ToolOptions struct {
	Context    *RunContext
	ToolCallID string
}

type Tool interface {
	Type() ToolType
	ID() string
}

type ExecutableTool interface {
	Tool
	Name() string
	Description() string
	Parameters() json.RawMessage
	Flags() ToolFlag
	OnDuplicate() DuplicateMode
	Execute(context.Context, json.RawMessage, ToolOptions) (any, error)
}

type FunctionTool[I any, O any] struct {
	id          string
	description string
	parameters  json.RawMessage
	flags       ToolFlag
	duplicate   DuplicateMode
	validate    func(*I) error
	execute     func(context.Context, I, ToolOptions) (O, error)
}

type FunctionToolOptions[I any, O any] struct {
	Name        string
	Description string
	Parameters  json.RawMessage
	Flags       ToolFlag
	OnDuplicate DuplicateMode
	Validate    func(*I) error
	Execute     func(context.Context, I, ToolOptions) (O, error)
}

func NewTool[I any, O any](options FunctionToolOptions[I, O]) (*FunctionTool[I, O], error) {
	if options.Name == "" {
		return nil, errors.New("tool name must not be empty")
	}
	if options.Execute == nil {
		return nil, errors.New("tool execute callback is required")
	}
	if options.OnDuplicate == "" {
		options.OnDuplicate = DuplicateAllow
	}
	if len(options.Parameters) == 0 {
		options.Parameters = json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`)
	} else if !json.Valid(options.Parameters) {
		return nil, errors.New("tool parameters must be valid JSON schema")
	}
	return &FunctionTool[I, O]{
		id: options.Name, description: options.Description,
		parameters: slices.Clone(options.Parameters), flags: options.Flags,
		duplicate: options.OnDuplicate, validate: options.Validate, execute: options.Execute,
	}, nil
}

func MustTool[I any, O any](options FunctionToolOptions[I, O]) *FunctionTool[I, O] {
	tool, err := NewTool(options)
	if err != nil {
		panic(err)
	}
	return tool
}

func (*FunctionTool[I, O]) Type() ToolType                { return ToolTypeFunction }
func (t *FunctionTool[I, O]) ID() string                  { return t.id }
func (t *FunctionTool[I, O]) Name() string                { return t.id }
func (t *FunctionTool[I, O]) Description() string         { return t.description }
func (t *FunctionTool[I, O]) Parameters() json.RawMessage { return slices.Clone(t.parameters) }
func (t *FunctionTool[I, O]) Flags() ToolFlag             { return t.flags }
func (t *FunctionTool[I, O]) OnDuplicate() DuplicateMode  { return t.duplicate }

func (t *FunctionTool[I, O]) Execute(ctx context.Context, raw json.RawMessage, options ToolOptions) (any, error) {
	var input I
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return nil, &ToolError{Message: fmt.Sprintf("invalid arguments for %s: %v", t.id, err)}
	}
	if t.validate != nil {
		if err := t.validate(&input); err != nil {
			return nil, &ToolError{Message: fmt.Sprintf("invalid arguments for %s: %v", t.id, err)}
		}
	}
	return t.execute(ctx, input, options)
}

type ProviderTool struct {
	ToolID string
	Data   map[string]any
}

func (*ProviderTool) Type() ToolType { return ToolTypeProvider }
func (t *ProviderTool) ID() string   { return t.ToolID }

type AgentHandoff struct {
	Agent   any
	Returns any
}

func Handoff(agent, returns any) AgentHandoff { return AgentHandoff{Agent: agent, Returns: returns} }

type ToolsetContext interface{ UpdateTools([]Tool) error }

type Toolset struct {
	id    string
	mu    sync.RWMutex
	tools []Tool
	setup func(context.Context, ToolsetContext) error
	close func(context.Context) error
	async *AsyncToolset
}

type ToolsetOptions struct {
	ID    string
	Tools []Tool
	Setup func(context.Context, ToolsetContext) error
	Close func(context.Context) error
}

func NewToolset(options ToolsetOptions) (*Toolset, error) {
	if options.ID == "" {
		return nil, errors.New("toolset id must not be empty")
	}
	return &Toolset{id: options.ID, tools: slices.Clone(options.Tools), setup: options.Setup, close: options.Close}, nil
}
func (t *Toolset) ID() string { return t.id }

// Async returns the async execution-scope marker, when this toolset was
// created with NewAsyncToolset.
func (t *Toolset) Async() (*AsyncToolset, bool) {
	if t == nil || t.async == nil {
		return nil, false
	}
	return t.async, true
}
func (t *Toolset) Tools() []Tool {
	t.mu.RLock()
	result := slices.Clone(t.tools)
	t.mu.RUnlock()
	return result
}
func (t *Toolset) Setup(ctx context.Context, toolContext ToolsetContext) error {
	if t.setup == nil {
		return nil
	}
	return t.setup(ctx, toolContext)
}
func (t *Toolset) Close(ctx context.Context) error {
	if t.close == nil {
		return nil
	}
	return t.close(ctx)
}

type Context struct {
	mu       sync.RWMutex
	tools    map[string]Tool
	toolsets map[string]*Toolset
	ordered  []Tool
}

func NewToolContext(entries ...any) (*Context, error) {
	c := &Context{}
	if err := c.Update(entries...); err != nil {
		return nil, err
	}
	return c, nil
}

func EmptyToolContext() *Context { c, _ := NewToolContext(); return c }

// Copy returns an independent context that retains toolset identity and setup
// hooks while sharing the immutable tool implementations themselves.
func (c *Context) Copy() *Context {
	c.mu.RLock()
	entries := make([]any, 0, len(c.ordered)+len(c.toolsets))
	toolsetTools := make(map[string]struct{})
	toolsets := make([]*Toolset, 0, len(c.toolsets))
	for _, set := range c.toolsets {
		toolsets = append(toolsets, set)
		for _, tool := range set.Tools() {
			toolsetTools[tool.ID()] = struct{}{}
		}
	}
	for _, tool := range c.ordered {
		if _, ownedByToolset := toolsetTools[tool.ID()]; !ownedByToolset {
			entries = append(entries, tool)
		}
	}
	c.mu.RUnlock()
	sort.Slice(toolsets, func(i, j int) bool { return toolsets[i].ID() < toolsets[j].ID() })
	for _, set := range toolsets {
		entries = append(entries, set)
	}
	copy, err := NewToolContext(entries...)
	if err != nil {
		// The source context has already passed the same validation. Reaching
		// this branch indicates an internal invariant violation.
		panic(err)
	}
	return copy
}

func (c *Context) Update(entries ...any) error {
	tools := make(map[string]Tool)
	toolsets := make(map[string]*Toolset)
	ordered := make([]Tool, 0, len(entries))
	var add func(any) error
	add = func(entry any) error {
		switch item := entry.(type) {
		case *AsyncToolset:
			if item == nil || item.Toolset == nil {
				return errors.New("async toolset must not be nil")
			}
			return add(item.Toolset)
		case Tool:
			if item.ID() == "" {
				return errors.New("tool id must not be empty")
			}
			if existing, ok := tools[item.ID()]; ok && !sameTool(existing, item) {
				return fmt.Errorf("duplicate function name: %s", item.ID())
			}
			if _, ok := tools[item.ID()]; !ok {
				tools[item.ID()] = item
				ordered = append(ordered, item)
			}
			return nil
		case *Toolset:
			if _, ok := toolsets[item.ID()]; ok {
				return fmt.Errorf("duplicate toolset id: %s", item.ID())
			}
			toolsets[item.ID()] = item
			for _, tool := range item.Tools() {
				if err := add(tool); err != nil {
					return err
				}
			}
			return nil
		default:
			return fmt.Errorf("unknown tool context entry %T", entry)
		}
	}
	for _, entry := range entries {
		if err := add(entry); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.tools, c.toolsets, c.ordered = tools, toolsets, ordered
	c.mu.Unlock()
	return nil
}

func sameTool(a, b Tool) bool {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	if av.Type() != bv.Type() {
		return false
	}
	switch av.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return av.Pointer() == bv.Pointer()
	default:
		return av.Comparable() && av.Interface() == bv.Interface()
	}
}

func (c *Context) UpdateTools(tools []Tool) error {
	entries := make([]any, len(tools))
	for i := range tools {
		entries[i] = tools[i]
	}
	return c.Update(entries...)
}
func (c *Context) HasTool(id string) bool {
	c.mu.RLock()
	_, ok := c.tools[id]
	c.mu.RUnlock()
	return ok
}
func (c *Context) Tool(id string) (Tool, bool) {
	c.mu.RLock()
	tool, ok := c.tools[id]
	c.mu.RUnlock()
	return tool, ok
}
func (c *Context) FunctionTool(id string) (ExecutableTool, bool) {
	tool, ok := c.Tool(id)
	if !ok {
		return nil, false
	}
	executable, ok := tool.(ExecutableTool)
	return executable, ok
}
func (c *Context) Flatten() []Tool {
	c.mu.RLock()
	result := slices.Clone(c.ordered)
	c.mu.RUnlock()
	return result
}
func (c *Context) Toolsets() []*Toolset {
	c.mu.RLock()
	result := make([]*Toolset, 0, len(c.toolsets))
	for _, set := range c.toolsets {
		result = append(result, set)
	}
	c.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].ID() < result[j].ID() })
	return result
}
func (c *Context) SortedFunctionTools() []ExecutableTool {
	c.mu.RLock()
	result := make([]ExecutableTool, 0, len(c.tools))
	for _, tool := range c.tools {
		if fn, ok := tool.(ExecutableTool); ok {
			result = append(result, fn)
		}
	}
	c.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].Name() < result[j].Name() })
	return result
}
func (c *Context) SortedToolNames() []string {
	tools := c.SortedFunctionTools()
	result := make([]string, len(tools))
	for i := range tools {
		result[i] = tools[i].Name()
	}
	return result
}
