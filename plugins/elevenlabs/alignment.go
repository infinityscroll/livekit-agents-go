// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/tokenize"
)

type providerAlignment struct {
	Chars      []string
	StartTimes []int64
	Durations  []int64
}

type ttsProviderEvent struct {
	ContextID           string
	Audio               []byte
	Alignment           *providerAlignment
	NormalizedAlignment *providerAlignment
	Final               bool
	Type                string
	ProviderError       error
}

func decodeTTSProviderEvent(payload []byte) (ttsProviderEvent, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return ttsProviderEvent{}, &ProtocolError{Message: "invalid TTS websocket JSON", Cause: err}
	}
	var event ttsProviderEvent
	decodeStringAliases(raw, &event.ContextID, "context_id", "contextId")
	decodeStringAliases(raw, &event.Type, "type", "message_type", "messageType")
	if value, ok := firstRaw(raw, "is_final", "isFinal"); ok {
		if err := json.Unmarshal(value, &event.Final); err != nil {
			return ttsProviderEvent{}, &ProtocolError{Message: "invalid TTS final flag", Cause: err}
		}
	}
	if value, ok := firstRaw(raw, "audio"); ok && len(value) != 0 && string(value) != "null" {
		var encoded string
		if err := json.Unmarshal(value, &encoded); err != nil {
			return ttsProviderEvent{}, &ProtocolError{Message: "invalid TTS audio field", Cause: err}
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil {
			return ttsProviderEvent{}, &ProtocolError{Message: "invalid base64 TTS audio", Cause: err}
		}
		event.Audio = decoded
	}
	if value, ok := firstRaw(raw, "alignment"); ok && string(value) != "null" {
		alignment, err := decodeProviderAlignment(value)
		if err != nil {
			return ttsProviderEvent{}, err
		}
		event.Alignment = alignment
	}
	if value, ok := firstRaw(raw, "normalized_alignment", "normalizedAlignment"); ok && string(value) != "null" {
		alignment, err := decodeProviderAlignment(value)
		if err != nil {
			return ttsProviderEvent{}, err
		}
		event.NormalizedAlignment = alignment
	}
	if value, ok := firstRaw(raw, "error"); ok && len(value) != 0 && string(value) != "null" {
		var body any
		if err := json.Unmarshal(value, &body); err != nil {
			body = string(value)
		}
		message := fmt.Sprint(body)
		if record, ok := body.(map[string]any); ok {
			if text, ok := record["message"].(string); ok {
				message = text
			}
		}
		event.ProviderError = &ProviderError{Type: "tts_error", Message: message, Body: body}
	}
	if event.ContextID == "" && event.Type != "flush_done" && event.ProviderError == nil {
		return ttsProviderEvent{}, &ProtocolError{Message: "TTS websocket message has no context ID"}
	}
	return event, nil
}

func decodeProviderAlignment(payload json.RawMessage) (*providerAlignment, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, &ProtocolError{Message: "invalid alignment object", Cause: err}
	}
	var result providerAlignment
	chars, charsOK := firstRaw(raw, "chars", "characters")
	starts, startsOK := firstRaw(raw, "char_start_times_ms", "charStartTimesMs", "chars_start_times_ms", "charsStartTimesMs")
	durations, durationsOK := firstRaw(raw, "char_durations_ms", "charDurationsMs", "chars_durations_ms", "charsDurationsMs")
	if !charsOK || !startsOK || !durationsOK {
		return nil, &ProtocolError{Message: "alignment is missing chars, starts, or durations"}
	}
	if err := json.Unmarshal(chars, &result.Chars); err != nil {
		return nil, &ProtocolError{Message: "invalid alignment chars", Cause: err}
	}
	if err := json.Unmarshal(starts, &result.StartTimes); err != nil {
		return nil, &ProtocolError{Message: "invalid alignment start times", Cause: err}
	}
	if err := json.Unmarshal(durations, &result.Durations); err != nil {
		return nil, &ProtocolError{Message: "invalid alignment durations", Cause: err}
	}
	if len(result.Chars) != len(result.StartTimes) || len(result.Chars) != len(result.Durations) {
		return nil, &ProtocolError{Message: "alignment arrays have different lengths"}
	}
	for i := range result.Chars {
		if result.Chars[i] == "" {
			return nil, &ProtocolError{Message: fmt.Sprintf("alignment char %d is empty", i)}
		}
		if result.StartTimes[i] < 0 || result.Durations[i] < 0 {
			return nil, &ProtocolError{Message: "alignment timing cannot be negative"}
		}
	}
	return &result, nil
}

type alignmentState struct {
	text            string
	starts          []int64
	durations       []int64
	firstWordOffset *int64
}

func (s *alignmentState) add(alignment *providerAlignment, flush bool) ([]agents.TimedString, error) {
	if alignment != nil {
		for i, chunk := range alignment.Chars {
			runes := []rune(chunk)
			if len(runes) == 0 {
				return nil, &ProtocolError{Message: "alignment contains an empty character entry"}
			}
			s.text += chunk
			start := alignment.StartTimes[i]
			duration := alignment.Durations[i]
			if s.firstWordOffset == nil && start > 0 {
				copyStart := start
				s.firstWordOffset = &copyStart
			}
			for range len(runes) - 1 {
				s.starts = append(s.starts, start)
				s.durations = append(s.durations, 0)
			}
			s.starts = append(s.starts, start)
			s.durations = append(s.durations, duration)
		}
	}
	if utf8.RuneCountInString(s.text) != len(s.starts) || len(s.starts) != len(s.durations) {
		return nil, &ProtocolError{Message: "alignment text and timing lengths differ"}
	}
	words, consumedRunes, consumedBytes := timedWords(s.text, s.starts, s.durations, flush, valueOrZero(s.firstWordOffset))
	if consumedBytes > 0 {
		s.text = s.text[consumedBytes:]
		s.starts = append([]int64(nil), s.starts[consumedRunes:]...)
		s.durations = append([]int64(nil), s.durations[consumedRunes:]...)
	}
	return words, nil
}

func timedWords(text string, starts, durations []int64, flush bool, offset int64) ([]agents.TimedString, int, int) {
	if text == "" || len(starts) == 0 || len(durations) == 0 {
		return nil, 0, 0
	}
	spans := tokenize.SplitWords(text, false)
	if len(spans) == 0 {
		return nil, 0, 0
	}
	timestamps := make([]int64, len(starts)+1)
	copy(timestamps, starts)
	timestamps[len(starts)] = starts[len(starts)-1] + durations[len(durations)-1]
	limit := len(spans) - 1
	if flush {
		limit = len(spans)
	}
	result := make([]agents.TimedString, 0, limit)
	consumedBytes := 0
	for i := 0; i < limit; i++ {
		span := spans[i]
		endByte := len(text)
		if i+1 < len(spans) {
			endByte = spans[i+1].Start
		}
		startRune := utf8.RuneCountInString(text[:span.Start])
		endRune := utf8.RuneCountInString(text[:endByte])
		startMS := clampTiming(timestamps, startRune) - offset
		endMS := clampTiming(timestamps, endRune) - offset
		if startMS < 0 {
			startMS = 0
		}
		if endMS < 0 {
			endMS = 0
		}
		startDuration := time.Duration(startMS) * time.Millisecond
		endDuration := time.Duration(endMS) * time.Millisecond
		result = append(result, agents.NewTimedString(text[span.Start:endByte], startDuration, endDuration))
		consumedBytes = endByte
	}
	if !flush && len(spans) > 0 {
		consumedBytes = spans[len(spans)-1].Start
	}
	consumedRunes := utf8.RuneCountInString(text[:consumedBytes])
	return result, consumedRunes, consumedBytes
}

func clampTiming(values []int64, index int) int64 {
	if index < 0 || len(values) == 0 {
		return 0
	}
	if index >= len(values) {
		return values[len(values)-1]
	}
	return values[index]
}

func valueOrZero(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

type incrementalTokenizer struct {
	tokenizer TextTokenizer
	buffer    string
}

func (t *incrementalTokenizer) push(text string) []string {
	if text == "" {
		return nil
	}
	t.buffer += text
	spans := t.tokenizer.TokenizeSpans(t.buffer)
	if len(spans) == 0 {
		return nil
	}
	complete := len(spans) - 1
	if lastTokenProvablyComplete(t.tokenizer, t.buffer) {
		complete = len(spans)
	}
	if complete <= 0 {
		return nil
	}
	result := make([]string, 0, complete)
	consume := 0
	for i := 0; i < complete; i++ {
		result = append(result, spans[i].Text)
		consume = spans[i].End
	}
	if complete < len(spans) {
		consume = spans[complete].Start
	}
	if consume > 0 && consume <= len(t.buffer) {
		t.buffer = t.buffer[consume:]
	}
	return result
}

func (t *incrementalTokenizer) flush() []string {
	spans := t.tokenizer.TokenizeSpans(t.buffer)
	result := make([]string, 0, len(spans))
	for _, span := range spans {
		if span.Text != "" {
			result = append(result, span.Text)
		}
	}
	t.buffer = ""
	return result
}

func lastTokenProvablyComplete(tokenizer TextTokenizer, text string) bool {
	if text == "" {
		return false
	}
	switch tokenizer.(type) {
	case *tokenize.WordTokenizer:
		return strings.TrimRight(text, " \t\r\n") != text
	case *tokenize.SentenceTokenizer:
		trimmed := strings.TrimSpace(text)
		return strings.HasSuffix(trimmed, ".") || strings.HasSuffix(trimmed, "!") || strings.HasSuffix(trimmed, "?") || strings.HasSuffix(text, "\n")
	default:
		return false
	}
}
