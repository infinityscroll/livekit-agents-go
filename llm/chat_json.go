// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	agents "github.com/livekit/agents-go"
)

// ChatContextJSONOptions controls the portable chat-history representation.
// The zero value has the same privacy-safe defaults as agents-js: images,
// audio, and timestamps are excluded. Use agents.Use(false) to include one of
// those fields explicitly.
type ChatContextJSONOptions struct {
	ExcludeImage        agents.Override[bool]
	ExcludeAudio        agents.Override[bool]
	ExcludeTimestamp    agents.Override[bool]
	ExcludeFunctionCall bool
	ExcludeConfigUpdate bool
	StripMarkup         bool
}

func resolveJSONDefault(value agents.Override[bool], fallback bool) bool {
	resolved, enabled := value.Resolve(fallback, true)
	if !enabled {
		return fallback
	}
	return resolved
}

// ToJSON returns the agents-js compatible JSON representation. At most one
// options value may be supplied.
func (c *ChatContext) ToJSON(options ...ChatContextJSONOptions) ([]byte, error) {
	if c == nil {
		return []byte("null"), nil
	}
	if len(options) > 1 {
		return nil, errors.New("chat context ToJSON accepts at most one options value")
	}
	var opts ChatContextJSONOptions
	if len(options) == 1 {
		opts = options[0]
	}
	excludeImage := resolveJSONDefault(opts.ExcludeImage, true)
	excludeAudio := resolveJSONDefault(opts.ExcludeAudio, true)
	excludeTimestamp := resolveJSONDefault(opts.ExcludeTimestamp, true)

	items := c.Items()
	raw := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		if opts.ExcludeFunctionCall && (item.ItemType() == ItemFunctionCall || item.ItemType() == ItemFunctionCallOutput) {
			continue
		}
		if opts.ExcludeConfigUpdate && item.ItemType() == ItemAgentConfigUpdate {
			continue
		}
		encoded, err := marshalChatItem(item, chatItemJSONOptions{
			excludeTimestamp: excludeTimestamp,
			excludeImage:     excludeImage,
			excludeAudio:     excludeAudio,
			stripMarkup:      opts.StripMarkup,
		})
		if err != nil {
			return nil, fmt.Errorf("marshal chat item %q: %w", item.ItemID(), err)
		}
		raw = append(raw, encoded)
	}
	return json.Marshal(struct {
		Items []json.RawMessage `json:"items"`
	}{Items: raw})
}

// MarshalJSON uses the same defaults as ChatContext.toJSON() in agents-js.
func (c *ChatContext) MarshalJSON() ([]byte, error) { return c.ToJSON() }

// UnmarshalJSON accepts a complete or privacy-filtered chat context.
func (c *ChatContext) UnmarshalJSON(data []byte) error {
	if c == nil {
		return errors.New("unmarshal chat context into nil receiver")
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		c.mu.Lock()
		c.items = nil
		c.readonly = false
		c.mu.Unlock()
		return nil
	}
	var wire struct {
		Items []json.RawMessage `json:"items"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return fmt.Errorf("decode chat context: %w", err)
	}
	items := make([]ChatItem, 0, len(wire.Items))
	known := make(map[string]struct{}, len(wire.Items))
	for index, raw := range wire.Items {
		item, err := unmarshalChatItem(raw)
		if err != nil {
			return fmt.Errorf("decode chat item %d: %w", index, err)
		}
		if item.ItemID() == "" {
			return fmt.Errorf("decode chat item %d: id is required", index)
		}
		if _, exists := known[item.ItemID()]; exists {
			return fmt.Errorf("decode chat item %d: duplicate id %q", index, item.ItemID())
		}
		known[item.ItemID()] = struct{}{}
		items = append(items, item)
	}
	c.mu.Lock()
	c.items = items
	c.readonly = false
	c.mu.Unlock()
	return nil
}

// ChatItemToJSON serializes a chat item with its native camelCase wire names.
func ChatItemToJSON(item ChatItem, excludeTimestamp bool) ([]byte, error) {
	return marshalChatItem(item, chatItemJSONOptions{excludeTimestamp: excludeTimestamp})
}

type chatItemJSONOptions struct {
	excludeTimestamp bool
	excludeImage     bool
	excludeAudio     bool
	stripMarkup      bool
}

func (m *ChatMessage) MarshalJSON() ([]byte, error) {
	return marshalChatItem(m, chatItemJSONOptions{})
}

func (f *FunctionCall) MarshalJSON() ([]byte, error) {
	return marshalChatItem(f, chatItemJSONOptions{})
}

func (f *FunctionCallOutput) MarshalJSON() ([]byte, error) {
	return marshalChatItem(f, chatItemJSONOptions{})
}

func (a *AgentHandoffItem) MarshalJSON() ([]byte, error) {
	return marshalChatItem(a, chatItemJSONOptions{})
}

func (a *AgentConfigUpdate) MarshalJSON() ([]byte, error) {
	return marshalChatItem(a, chatItemJSONOptions{})
}

func (i Instructions) MarshalJSON() ([]byte, error) {
	type wire struct {
		Type  string `json:"type"`
		Audio string `json:"audio"`
		Text  string `json:"text,omitempty"`
	}
	return json.Marshal(wire{Type: "instructions", Audio: i.Audio, Text: i.Text})
}

func (i *Instructions) UnmarshalJSON(data []byte) error {
	if i == nil {
		return errors.New("unmarshal instructions into nil receiver")
	}
	var wire struct {
		Type  string `json:"type"`
		Audio string `json:"audio"`
		Text  string `json:"text"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.Type != "" && wire.Type != "instructions" {
		return fmt.Errorf("unexpected instructions type %q", wire.Type)
	}
	*i = NewInstructions(wire.Audio, wire.Text)
	return nil
}

func marshalChatItem(item ChatItem, options chatItemJSONOptions) ([]byte, error) {
	if item == nil {
		return nil, errors.New("chat item is nil")
	}
	createdAt := func(t time.Time) *int64 {
		if options.excludeTimestamp {
			return nil
		}
		value := unixMillisecondsOrZero(t)
		return &value
	}
	switch value := item.(type) {
	case *ChatMessage:
		if value == nil {
			return nil, errors.New("chat message is nil")
		}
		content := make([]json.RawMessage, 0, len(value.Content))
		for index, part := range value.Content {
			encoded, include, err := marshalChatContent(part, value.Role, options)
			if err != nil {
				return nil, fmt.Errorf("content %d: %w", index, err)
			}
			if include {
				content = append(content, encoded)
			}
		}
		type messageWire struct {
			ID                   string             `json:"id"`
			Type                 ItemType           `json:"type"`
			Role                 ChatRole           `json:"role"`
			Content              []json.RawMessage  `json:"content"`
			Interrupted          bool               `json:"interrupted"`
			CreatedAt            *int64             `json:"createdAt,omitempty"`
			TranscriptConfidence *float64           `json:"transcriptConfidence,omitempty"`
			Metrics              *metricsReportWire `json:"metrics,omitempty"`
			Extra                map[string]any     `json:"extra,omitempty"`
		}
		return json.Marshal(messageWire{
			ID: value.ID, Type: ItemMessage, Role: value.Role, Content: content,
			Interrupted: value.Interrupted, CreatedAt: createdAt(value.CreatedAt),
			TranscriptConfidence: value.TranscriptConfidence,
			Metrics:              metricsReportToWire(value.Metrics),
			Extra:                cloneMap(value.Extra),
		})
	case *FunctionCall:
		if value == nil {
			return nil, errors.New("function call is nil")
		}
		return json.Marshal(struct {
			ID               string         `json:"id"`
			Type             ItemType       `json:"type"`
			CallID           string         `json:"callId"`
			Name             string         `json:"name"`
			Arguments        string         `json:"args"`
			Extra            map[string]any `json:"extra,omitempty"`
			GroupID          string         `json:"groupId,omitempty"`
			ThoughtSignature string         `json:"thoughtSignature,omitempty"`
			CreatedAt        *int64         `json:"createdAt,omitempty"`
		}{value.ID, ItemFunctionCall, value.CallID, value.Name, value.Arguments, cloneMap(value.Extra), value.GroupID, value.ThoughtSignature, createdAt(value.CreatedAt)})
	case *FunctionCallOutput:
		if value == nil {
			return nil, errors.New("function call output is nil")
		}
		return json.Marshal(struct {
			ID        string   `json:"id"`
			Type      ItemType `json:"type"`
			Name      string   `json:"name"`
			CallID    string   `json:"callId"`
			Output    string   `json:"output"`
			IsError   bool     `json:"isError"`
			CreatedAt *int64   `json:"createdAt,omitempty"`
		}{value.ID, ItemFunctionCallOutput, value.Name, value.CallID, value.Output, value.IsError, createdAt(value.CreatedAt)})
	case *AgentHandoffItem:
		if value == nil {
			return nil, errors.New("agent handoff is nil")
		}
		return json.Marshal(struct {
			ID         string   `json:"id"`
			Type       ItemType `json:"type"`
			OldAgentID string   `json:"oldAgentId,omitempty"`
			NewAgentID string   `json:"newAgentId"`
			CreatedAt  *int64   `json:"createdAt,omitempty"`
		}{value.ID, ItemAgentHandoff, value.OldAgentID, value.NewAgentID, createdAt(value.CreatedAt)})
	case *AgentConfigUpdate:
		if value == nil {
			return nil, errors.New("agent config update is nil")
		}
		return json.Marshal(struct {
			ID           string        `json:"id"`
			Type         ItemType      `json:"type"`
			Instructions *Instructions `json:"instructions,omitempty"`
			ToolsAdded   []string      `json:"toolsAdded,omitempty"`
			ToolsRemoved []string      `json:"toolsRemoved,omitempty"`
			CreatedAt    *int64        `json:"createdAt,omitempty"`
		}{value.ID, ItemAgentConfigUpdate, value.Instructions, value.ToolsAdded, value.ToolsRemoved, createdAt(value.CreatedAt)})
	default:
		return nil, fmt.Errorf("unsupported chat item type %T", item)
	}
}

func marshalChatContent(content Content, role ChatRole, options chatItemJSONOptions) ([]byte, bool, error) {
	if content == nil {
		return nil, false, errors.New("content is nil")
	}
	switch value := content.(type) {
	case TextContent:
		text := string(value)
		if options.stripMarkup && role == RoleAssistant {
			text = stripExpressionMarkup(text)
		}
		encoded, err := json.Marshal(text)
		return encoded, true, err
	case InstructionContent:
		encoded, err := json.Marshal(value.Instructions)
		return encoded, true, err
	case ImageContent:
		if options.excludeImage {
			return nil, false, nil
		}
		encoded, err := json.Marshal(struct {
			ID              string      `json:"id"`
			Type            string      `json:"type"`
			Image           any         `json:"image"`
			InferenceDetail ImageDetail `json:"inferenceDetail"`
			InferenceWidth  int         `json:"inferenceWidth,omitempty"`
			InferenceHeight int         `json:"inferenceHeight,omitempty"`
			MIMEType        string      `json:"mimeType,omitempty"`
		}{value.ID, "image_content", value.Image, value.InferenceDetail, value.InferenceWidth, value.InferenceHeight, value.MIMEType})
		return encoded, true, err
	case AudioContent:
		if options.excludeAudio {
			return nil, false, nil
		}
		encoded, err := json.Marshal(struct {
			Type       string `json:"type"`
			Transcript string `json:"transcript,omitempty"`
		}{"audio_content", value.Transcript})
		return encoded, true, err
	default:
		return nil, false, fmt.Errorf("unsupported content type %T", content)
	}
}

type metricsReportWire struct {
	ProviderRequestIDs       []string `json:"providerRequestIds,omitempty"`
	StartedSpeakingAt        float64  `json:"startedSpeakingAt,omitempty"`
	StoppedSpeakingAt        float64  `json:"stoppedSpeakingAt,omitempty"`
	TranscriptionDelay       float64  `json:"transcriptionDelay,omitempty"`
	EndOfTurnDelay           float64  `json:"endOfTurnDelay,omitempty"`
	OnUserTurnCompletedDelay float64  `json:"onUserTurnCompletedDelay,omitempty"`
	LLMNodeTTFT              float64  `json:"llmNodeTtft,omitempty"`
	TTSNodeTTFB              float64  `json:"ttsNodeTtfb,omitempty"`
	PlaybackLatency          float64  `json:"playbackLatency,omitempty"`
	EndToEndLatency          float64  `json:"e2eLatency,omitempty"`
}

func metricsReportToWire(report MetricsReport) *metricsReportWire {
	wire := metricsReportWire{
		ProviderRequestIDs:       append([]string(nil), report.ProviderRequestIDs...),
		StartedSpeakingAt:        unixSecondsOrZero(report.StartedSpeakingAt),
		StoppedSpeakingAt:        unixSecondsOrZero(report.StoppedSpeakingAt),
		TranscriptionDelay:       durationSeconds(report.TranscriptionDelay),
		EndOfTurnDelay:           durationSeconds(report.EndOfTurnDelay),
		OnUserTurnCompletedDelay: durationSeconds(report.OnUserTurnCompletedDelay),
		LLMNodeTTFT:              durationSeconds(report.LLMNodeTTFT),
		TTSNodeTTFB:              durationSeconds(report.TTSNodeTTFB),
		PlaybackLatency:          durationSeconds(report.PlaybackLatency),
		EndToEndLatency:          durationSeconds(report.EndToEndLatency),
	}
	if len(wire.ProviderRequestIDs) == 0 && wire.StartedSpeakingAt == 0 && wire.StoppedSpeakingAt == 0 &&
		wire.TranscriptionDelay == 0 && wire.EndOfTurnDelay == 0 && wire.OnUserTurnCompletedDelay == 0 &&
		wire.LLMNodeTTFT == 0 && wire.TTSNodeTTFB == 0 && wire.PlaybackLatency == 0 && wire.EndToEndLatency == 0 {
		return nil
	}
	return &wire
}

func metricsReportFromWire(wire *metricsReportWire) MetricsReport {
	if wire == nil {
		return MetricsReport{}
	}
	return MetricsReport{
		ProviderRequestIDs:       append([]string(nil), wire.ProviderRequestIDs...),
		StartedSpeakingAt:        timeFromUnixSeconds(wire.StartedSpeakingAt),
		StoppedSpeakingAt:        timeFromUnixSeconds(wire.StoppedSpeakingAt),
		TranscriptionDelay:       secondsDuration(wire.TranscriptionDelay),
		EndOfTurnDelay:           secondsDuration(wire.EndOfTurnDelay),
		OnUserTurnCompletedDelay: secondsDuration(wire.OnUserTurnCompletedDelay),
		LLMNodeTTFT:              secondsDuration(wire.LLMNodeTTFT),
		TTSNodeTTFB:              secondsDuration(wire.TTSNodeTTFB),
		PlaybackLatency:          secondsDuration(wire.PlaybackLatency),
		EndToEndLatency:          secondsDuration(wire.EndToEndLatency),
	}
}

func unmarshalChatItem(data []byte) (ChatItem, error) {
	var header struct {
		Type ItemType `json:"type"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, err
	}
	switch header.Type {
	case ItemMessage:
		var wire struct {
			ID                   string             `json:"id"`
			Role                 ChatRole           `json:"role"`
			Content              []json.RawMessage  `json:"content"`
			Interrupted          bool               `json:"interrupted"`
			CreatedAt            *int64             `json:"createdAt"`
			TranscriptConfidence *float64           `json:"transcriptConfidence"`
			Metrics              *metricsReportWire `json:"metrics"`
			Extra                map[string]any     `json:"extra"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			return nil, err
		}
		content := make([]Content, 0, len(wire.Content))
		for index, raw := range wire.Content {
			part, err := unmarshalChatContent(raw)
			if err != nil {
				return nil, fmt.Errorf("content %d: %w", index, err)
			}
			content = append(content, part)
		}
		return &ChatMessage{ID: wire.ID, Role: wire.Role, Content: content, Interrupted: wire.Interrupted,
			TranscriptConfidence: wire.TranscriptConfidence, Metrics: metricsReportFromWire(wire.Metrics),
			Extra: wire.Extra, CreatedAt: timeFromUnixMilliseconds(wire.CreatedAt)}, nil
	case ItemFunctionCall:
		var wire struct {
			ID               string         `json:"id"`
			CallID           string         `json:"callId"`
			Name             string         `json:"name"`
			Arguments        string         `json:"args"`
			Extra            map[string]any `json:"extra"`
			GroupID          string         `json:"groupId"`
			ThoughtSignature string         `json:"thoughtSignature"`
			CreatedAt        *int64         `json:"createdAt"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			return nil, err
		}
		return &FunctionCall{ID: wire.ID, CallID: wire.CallID, Name: wire.Name, Arguments: wire.Arguments,
			Extra: wire.Extra, GroupID: wire.GroupID, ThoughtSignature: wire.ThoughtSignature,
			CreatedAt: timeFromUnixMilliseconds(wire.CreatedAt)}, nil
	case ItemFunctionCallOutput:
		var wire struct {
			ID        string `json:"id"`
			Name      string `json:"name"`
			CallID    string `json:"callId"`
			Output    string `json:"output"`
			IsError   bool   `json:"isError"`
			CreatedAt *int64 `json:"createdAt"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			return nil, err
		}
		return &FunctionCallOutput{ID: wire.ID, Name: wire.Name, CallID: wire.CallID, Output: wire.Output,
			IsError: wire.IsError, CreatedAt: timeFromUnixMilliseconds(wire.CreatedAt)}, nil
	case ItemAgentHandoff:
		var wire struct {
			ID         string `json:"id"`
			OldAgentID string `json:"oldAgentId"`
			NewAgentID string `json:"newAgentId"`
			CreatedAt  *int64 `json:"createdAt"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			return nil, err
		}
		return &AgentHandoffItem{ID: wire.ID, OldAgentID: wire.OldAgentID, NewAgentID: wire.NewAgentID,
			CreatedAt: timeFromUnixMilliseconds(wire.CreatedAt)}, nil
	case ItemAgentConfigUpdate:
		var wire struct {
			ID           string        `json:"id"`
			Instructions *Instructions `json:"instructions"`
			ToolsAdded   []string      `json:"toolsAdded"`
			ToolsRemoved []string      `json:"toolsRemoved"`
			CreatedAt    *int64        `json:"createdAt"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			return nil, err
		}
		return &AgentConfigUpdate{ID: wire.ID, Instructions: wire.Instructions,
			ToolsAdded: wire.ToolsAdded, ToolsRemoved: wire.ToolsRemoved,
			CreatedAt: timeFromUnixMilliseconds(wire.CreatedAt)}, nil
	default:
		return nil, fmt.Errorf("unknown chat item type %q", header.Type)
	}
}

func unmarshalChatContent(data []byte) (Content, error) {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		return TextContent(text), nil
	}
	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, err
	}
	switch header.Type {
	case "instructions":
		var instructions Instructions
		if err := json.Unmarshal(data, &instructions); err != nil {
			return nil, err
		}
		return InstructionContent{Instructions: instructions}, nil
	case "image_content":
		var wire struct {
			ID              string          `json:"id"`
			Image           json.RawMessage `json:"image"`
			InferenceDetail ImageDetail     `json:"inferenceDetail"`
			InferenceWidth  int             `json:"inferenceWidth"`
			InferenceHeight int             `json:"inferenceHeight"`
			MIMEType        string          `json:"mimeType"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			return nil, err
		}
		var image any
		if len(wire.Image) != 0 && !bytes.Equal(wire.Image, []byte("null")) {
			decoder := json.NewDecoder(bytes.NewReader(wire.Image))
			decoder.UseNumber()
			if err := decoder.Decode(&image); err != nil {
				return nil, fmt.Errorf("decode image: %w", err)
			}
		}
		return ImageContent{ID: wire.ID, Image: image, InferenceDetail: wire.InferenceDetail,
			InferenceWidth: wire.InferenceWidth, InferenceHeight: wire.InferenceHeight, MIMEType: wire.MIMEType}, nil
	case "audio_content":
		var wire struct {
			Transcript string `json:"transcript"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			return nil, err
		}
		return AudioContent{Transcript: wire.Transcript}, nil
	default:
		return nil, fmt.Errorf("unknown chat content type %q", header.Type)
	}
}

func unixMillisecondsOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}

func unixSecondsOrZero(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.UnixNano()) / float64(time.Second)
}

func durationSeconds(value time.Duration) float64 {
	return float64(value) / float64(time.Second)
}

func secondsDuration(value float64) time.Duration {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return time.Duration(value * float64(time.Second))
}

func timeFromUnixSeconds(value float64) time.Time {
	if value == 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return time.Time{}
	}
	seconds, fraction := math.Modf(value)
	return time.Unix(int64(seconds), int64(fraction*float64(time.Second)))
}

func timeFromUnixMilliseconds(value *int64) time.Time {
	if value == nil || *value == 0 {
		return time.Time{}
	}
	return time.UnixMilli(*value)
}
