// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
)

type ChatRole string

const (
	RoleDeveloper ChatRole = "developer"
	RoleSystem    ChatRole = "system"
	RoleUser      ChatRole = "user"
	RoleAssistant ChatRole = "assistant"
)

type Modality string

const (
	ModalityAudio Modality = "audio"
	ModalityText  Modality = "text"
)

type Instructions struct {
	Audio string `json:"audio"`
	Text  string `json:"text,omitempty"`
	value string
}

func NewInstructions(audio, text string) Instructions {
	return Instructions{Audio: audio, Text: text, value: audio}
}

func (i Instructions) Value() string {
	if i.value != "" {
		return i.value
	}
	return i.Audio
}

func (i Instructions) TextValue() string {
	if i.Text != "" {
		return i.Text
	}
	return i.Audio
}

func (i Instructions) AsModality(modality Modality) Instructions {
	copy := i
	if modality == ModalityText {
		copy.value = i.TextValue()
	} else {
		copy.value = i.Audio
	}
	return copy
}

func (i Instructions) Concat(other Instructions) Instructions {
	text := ""
	if i.Text != "" || other.Text != "" {
		text = i.TextValue() + other.TextValue()
	}
	return Instructions{Audio: i.Audio + other.Audio, Text: text, value: i.Value() + other.Value()}
}

func (i Instructions) String() string { return i.Value() }

type Content interface{ contentKind() string }

type TextContent string

func (TextContent) contentKind() string { return "text" }

type InstructionContent struct{ Instructions Instructions }

func (InstructionContent) contentKind() string { return "instructions" }

type ImageDetail string

const (
	ImageDetailAuto ImageDetail = "auto"
	ImageDetailHigh ImageDetail = "high"
	ImageDetailLow  ImageDetail = "low"
)

type ImageContent struct {
	ID              string
	Image           any
	InferenceDetail ImageDetail
	InferenceWidth  int
	InferenceHeight int
	MIMEType        string
}

func (ImageContent) contentKind() string { return "image_content" }

func NewImageContent(image any) ImageContent {
	return ImageContent{ID: agents.ShortUUID("img_"), Image: image, InferenceDetail: ImageDetailAuto}
}

type AudioContent struct {
	Frames     []agents.AudioFrame
	Transcript string
}

func (AudioContent) contentKind() string { return "audio_content" }

type MetricsReport struct {
	ProviderRequestIDs       []string
	StartedSpeakingAt        time.Time
	StoppedSpeakingAt        time.Time
	TranscriptionDelay       time.Duration
	EndOfTurnDelay           time.Duration
	OnUserTurnCompletedDelay time.Duration
	LLMNodeTTFT              time.Duration
	TTSNodeTTFB              time.Duration
	PlaybackLatency          time.Duration
	EndToEndLatency          time.Duration
}

type ItemType string

const (
	ItemMessage            ItemType = "message"
	ItemFunctionCall       ItemType = "function_call"
	ItemFunctionCallOutput ItemType = "function_call_output"
	ItemAgentHandoff       ItemType = "agent_handoff"
	ItemAgentConfigUpdate  ItemType = "agent_config_update"
)

type ChatItem interface {
	ItemID() string
	ItemType() ItemType
	ItemCreatedAt() time.Time
	clone() ChatItem
}

type ChatMessage struct {
	ID                   string
	Role                 ChatRole
	Content              []Content
	Interrupted          bool
	TranscriptConfidence *float64
	Extra                map[string]any
	Metrics              MetricsReport
	Hash                 []byte
	CreatedAt            time.Time
}

func NewChatMessage(role ChatRole, text string) *ChatMessage {
	return &ChatMessage{ID: agents.ShortUUID("item_"), Role: role, Content: []Content{TextContent(text)}, Extra: make(map[string]any), CreatedAt: time.Now()}
}
func (m *ChatMessage) ItemID() string           { return m.ID }
func (m *ChatMessage) ItemType() ItemType       { return ItemMessage }
func (m *ChatMessage) ItemCreatedAt() time.Time { return m.CreatedAt }
func (m *ChatMessage) Clone() *ChatMessage      { return m.clone().(*ChatMessage) }
func (m *ChatMessage) clone() ChatItem {
	copy := *m
	copy.Content = slices.Clone(m.Content)
	copy.Extra = cloneMap(m.Extra)
	copy.Hash = slices.Clone(m.Hash)
	copy.Metrics.ProviderRequestIDs = slices.Clone(m.Metrics.ProviderRequestIDs)
	return &copy
}

func (m *ChatMessage) RawTextContent() (string, bool) {
	parts := make([]string, 0, len(m.Content))
	for _, content := range m.Content {
		switch c := content.(type) {
		case TextContent:
			parts = append(parts, string(c))
		case InstructionContent:
			parts = append(parts, c.Instructions.Value())
		}
	}
	return strings.Join(parts, "\n"), len(parts) != 0
}

func (m *ChatMessage) TextContent() (string, bool) {
	text, ok := m.RawTextContent()
	if !ok || m.Role != RoleAssistant {
		return text, ok
	}
	return stripExpressionMarkup(text), true
}

type FunctionCall struct {
	ID               string
	CallID           string
	Name             string
	Arguments        string
	CreatedAt        time.Time
	Extra            map[string]any
	GroupID          string
	ThoughtSignature string
}

func NewFunctionCall(callID, name, arguments string) *FunctionCall {
	return &FunctionCall{ID: agents.ShortUUID("item_"), CallID: callID, Name: name, Arguments: arguments, CreatedAt: time.Now(), Extra: make(map[string]any)}
}
func (f *FunctionCall) ItemID() string           { return f.ID }
func (f *FunctionCall) ItemType() ItemType       { return ItemFunctionCall }
func (f *FunctionCall) ItemCreatedAt() time.Time { return f.CreatedAt }
func (f *FunctionCall) Clone() *FunctionCall     { return f.clone().(*FunctionCall) }
func (f *FunctionCall) clone() ChatItem          { copy := *f; copy.Extra = cloneMap(f.Extra); return &copy }

func (f *FunctionCall) DecodeArguments(target any) error {
	if err := json.Unmarshal([]byte(f.Arguments), target); err != nil {
		return fmt.Errorf("decode arguments for tool %q: %w", f.Name, err)
	}
	return nil
}

type FunctionCallOutput struct {
	ID        string
	CallID    string
	Name      string
	Output    string
	IsError   bool
	CreatedAt time.Time
}

func NewFunctionCallOutput(callID, name, output string, isError bool) *FunctionCallOutput {
	return &FunctionCallOutput{ID: agents.ShortUUID("item_"), CallID: callID, Name: name, Output: output, IsError: isError, CreatedAt: time.Now()}
}
func (f *FunctionCallOutput) ItemID() string           { return f.ID }
func (f *FunctionCallOutput) ItemType() ItemType       { return ItemFunctionCallOutput }
func (f *FunctionCallOutput) ItemCreatedAt() time.Time { return f.CreatedAt }
func (f *FunctionCallOutput) clone() ChatItem          { copy := *f; return &copy }

type AgentHandoffItem struct {
	ID         string
	OldAgentID string
	NewAgentID string
	CreatedAt  time.Time
}

func (a *AgentHandoffItem) ItemID() string           { return a.ID }
func (a *AgentHandoffItem) ItemType() ItemType       { return ItemAgentHandoff }
func (a *AgentHandoffItem) ItemCreatedAt() time.Time { return a.CreatedAt }
func (a *AgentHandoffItem) clone() ChatItem          { copy := *a; return &copy }

type AgentConfigUpdate struct {
	ID           string
	Instructions *Instructions
	ToolsAdded   []string
	ToolsRemoved []string
	CreatedAt    time.Time
}

func (a *AgentConfigUpdate) ItemID() string           { return a.ID }
func (a *AgentConfigUpdate) ItemType() ItemType       { return ItemAgentConfigUpdate }
func (a *AgentConfigUpdate) ItemCreatedAt() time.Time { return a.CreatedAt }
func (a *AgentConfigUpdate) clone() ChatItem {
	copy := *a
	copy.ToolsAdded = slices.Clone(a.ToolsAdded)
	copy.ToolsRemoved = slices.Clone(a.ToolsRemoved)
	return &copy
}

type CopyOptions struct {
	ExcludeFunctionCall bool
	ExcludeInstructions bool
	ExcludeEmptyMessage bool
	ExcludeHandoff      bool
	ExcludeConfigUpdate bool
	// ToolContext removes function calls/outputs for tools that are not
	// currently active, matching agents-js copy({ toolCtx }).
	ToolContext *Context
}

type ChatContext struct {
	mu       sync.RWMutex
	items    []ChatItem
	readonly bool
}

func NewChatContext(items ...ChatItem) *ChatContext {
	ctx := &ChatContext{}
	ctx.items = cloneItems(items)
	return ctx
}
func EmptyChatContext() *ChatContext { return &ChatContext{} }

func (c *ChatContext) Items() []ChatItem {
	c.mu.RLock()
	result := cloneItems(c.items)
	c.mu.RUnlock()
	return result
}
func (c *ChatContext) Len() int       { c.mu.RLock(); n := len(c.items); c.mu.RUnlock(); return n }
func (c *ChatContext) Readonly() bool { c.mu.RLock(); v := c.readonly; c.mu.RUnlock(); return v }

func (c *ChatContext) AsReadonly() *ChatContext {
	copy := c.Copy(CopyOptions{})
	copy.readonly = true
	return copy
}

func (c *ChatContext) Insert(items ...ChatItem) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readonly {
		return errors.New("chat context is readonly")
	}
	known := make(map[string]struct{}, len(c.items)+len(items))
	for _, item := range c.items {
		known[item.ItemID()] = struct{}{}
	}
	for _, item := range items {
		if item == nil || item.ItemID() == "" {
			return errors.New("chat item and id are required")
		}
		if _, exists := known[item.ItemID()]; exists {
			return fmt.Errorf("duplicate chat item id %q", item.ItemID())
		}
		known[item.ItemID()] = struct{}{}
		cloned := item.clone()
		// Insert is always chronological. Walk from the tail because live
		// conversation appends are overwhelmingly already ordered.
		index := len(c.items)
		for index > 0 && c.items[index-1].ItemCreatedAt().After(cloned.ItemCreatedAt()) {
			index--
		}
		c.items = append(c.items, nil)
		copy(c.items[index+1:], c.items[index:])
		c.items[index] = cloned
	}
	return nil
}

func (c *ChatContext) AddMessage(role ChatRole, text string) (*ChatMessage, error) {
	message := NewChatMessage(role, text)
	return message, c.Insert(message)
}

func (c *ChatContext) Remove(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readonly {
		return false
	}
	for i, item := range c.items {
		if item.ItemID() == id {
			copy(c.items[i:], c.items[i+1:])
			c.items[len(c.items)-1] = nil
			c.items = c.items[:len(c.items)-1]
			return true
		}
	}
	return false
}

func (c *ChatContext) GetByID(id string) (ChatItem, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, item := range c.items {
		if item.ItemID() == id {
			return item.clone(), true
		}
	}
	return nil, false
}

func (c *ChatContext) IndexByID(id string) (int, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for i, item := range c.items {
		if item.ItemID() == id {
			return i, true
		}
	}
	return 0, false
}

func (c *ChatContext) Copy(options CopyOptions) *ChatContext {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := &ChatContext{items: make([]ChatItem, 0, len(c.items))}
	for _, item := range c.items {
		switch v := item.(type) {
		case *FunctionCall, *FunctionCallOutput:
			if options.ExcludeFunctionCall {
				continue
			}
			if options.ToolContext != nil {
				name := ""
				switch toolItem := v.(type) {
				case *FunctionCall:
					name = toolItem.Name
				case *FunctionCallOutput:
					name = toolItem.Name
				}
				if !options.ToolContext.HasTool(name) {
					continue
				}
			}
		case *AgentHandoffItem:
			if options.ExcludeHandoff {
				continue
			}
		case *AgentConfigUpdate:
			if options.ExcludeConfigUpdate {
				continue
			}
		case *ChatMessage:
			if options.ExcludeInstructions && (v.Role == RoleSystem || v.Role == RoleDeveloper) {
				continue
			}
			if options.ExcludeEmptyMessage {
				if len(v.Content) == 0 {
					continue
				}
			}
		}
		result.items = append(result.items, item.clone())
	}
	return result
}

func (c *ChatContext) Truncate(maxItems int) *ChatContext {
	if maxItems <= 0 {
		return c
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readonly {
		return c
	}
	var instructions ChatItem
	for _, item := range c.items {
		if message, ok := item.(*ChatMessage); ok && message.Role == RoleSystem {
			instructions = item
			break
		}
	}
	start := max(0, len(c.items)-maxItems)
	newItems := append([]ChatItem(nil), c.items[start:]...)
	for len(newItems) != 0 {
		typeValue := newItems[0].ItemType()
		if typeValue != ItemFunctionCall && typeValue != ItemFunctionCallOutput {
			break
		}
		newItems[0] = nil
		newItems = newItems[1:]
	}
	if instructions != nil {
		contains := false
		for _, item := range newItems {
			if item.ItemID() == instructions.ItemID() {
				contains = true
				break
			}
		}
		if !contains {
			newItems = append([]ChatItem{instructions}, newItems...)
		}
	}
	clear(c.items)
	c.items = newItems
	return c
}

func (c *ChatContext) Merge(other *ChatContext, options CopyOptions) *ChatContext {
	if other == nil {
		return c
	}
	filtered := other.Copy(options)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readonly {
		return c
	}
	seen := make(map[string]struct{}, len(c.items)+filtered.Len())
	for _, item := range c.items {
		seen[item.ItemID()] = struct{}{}
	}
	for _, item := range filtered.Items() {
		if _, ok := seen[item.ItemID()]; !ok {
			cloned := item.clone()
			index := len(c.items)
			for index > 0 && c.items[index-1].ItemCreatedAt().After(cloned.ItemCreatedAt()) {
				index--
			}
			c.items = append(c.items, nil)
			copy(c.items[index+1:], c.items[index:])
			c.items[index] = cloned
			seen[item.ItemID()] = struct{}{}
		}
	}
	return c
}

func (c *ChatContext) IsEquivalent(other *ChatContext) bool {
	a, b := c.Items(), other.Items()
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !chatItemsEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func cloneItems(items []ChatItem) []ChatItem {
	result := make([]ChatItem, 0, len(items))
	for _, item := range items {
		if item != nil {
			result = append(result, item.clone())
		}
	}
	return result
}
func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	result := make(map[string]any, len(input))
	for k, v := range input {
		result[k] = v
	}
	return result
}
func chatItemsEqual(a, b ChatItem) bool {
	if a.ItemType() != b.ItemType() || a.ItemID() != b.ItemID() {
		return false
	}
	aj, _ := ChatItemToJSON(a, true)
	bj, _ := ChatItemToJSON(b, true)
	return slices.Equal(aj, bj)
}

func stripExpressionMarkup(text string) string {
	for {
		start := strings.Index(text, "<"+"expression")
		if start < 0 {
			return text
		}
		end := strings.Index(text[start:], "/>")
		if end < 0 {
			return text
		}
		text = text[:start] + text[start+end+2:]
	}
}
