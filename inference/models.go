// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"strings"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stt"
)

type ModelOptions map[string]any

// ParseSTTModelString parses "provider/model:language" at the final colon.
// The final-colon rule preserves model identifiers which may contain colons.
func ParseSTTModelString(value string) (model string, language agents.LanguageCode) {
	if index := strings.LastIndexByte(value, ':'); index >= 0 {
		return value[:index], agents.NormalizeLanguage(value[index+1:])
	}
	return value, ""
}

// ParseTTSModelString parses "provider/model:voice" at the final colon.
func ParseTTSModelString(value string) (model, voice string) {
	if index := strings.LastIndexByte(value, ':'); index >= 0 {
		return value[:index], value[index+1:]
	}
	return value, ""
}

type STTFallbackModel struct {
	Model string       `json:"model"`
	Extra ModelOptions `json:"extra,omitempty"`
}

// STTFallbackFromString discards a language suffix just like agents-js. The
// primary stream language remains authoritative for all fallback models.
func STTFallbackFromString(value string) STTFallbackModel {
	model, _ := ParseSTTModelString(value)
	return STTFallbackModel{Model: model}
}

type TTSFallbackModel struct {
	Model string       `json:"model"`
	Voice string       `json:"voice"`
	Extra ModelOptions `json:"extra,omitempty"`
}

func TTSFallbackFromString(value string) TTSFallbackModel {
	model, voice := ParseTTSModelString(value)
	return TTSFallbackModel{Model: model, Voice: voice}
}

// TTSHasAlignedTranscript reports whether the configured gateway adapter is
// explicitly requested to emit timestamps. Unknown providers are conservative.
func TTSHasAlignedTranscript(model string, options ModelOptions) bool {
	provider, _, _ := strings.Cut(model, "/")
	switch provider {
	case "cartesia":
		return truthy(options["add_timestamps"])
	case "elevenlabs":
		return truthy(options["sync_alignment"])
	case "inworld":
		value, _ := options["timestamp_type"].(string)
		return value == "WORD" || value == "CHARACTER"
	default:
		return false
	}
}

func truthy(value any) bool {
	switch value := value.(type) {
	case bool:
		return value
	case string:
		return strings.EqualFold(value, "true") || value == "1"
	case int:
		return value != 0
	case float64:
		return value != 0
	default:
		return false
	}
}

func hasWordAlignment(model string) bool {
	switch model {
	case "deepgram/nova-3", "deepgram/nova-3-medical",
		"deepgram/nova-2", "deepgram/nova-2-medical",
		"deepgram/nova-2-conversationalai", "deepgram/nova-2-phonecall",
		"deepgram/flux-general", "deepgram/flux-general-en", "deepgram/flux-general-multi",
		"cartesia/ink-whisper",
		"assemblyai/universal-streaming", "assemblyai/universal-streaming-multilingual",
		"assemblyai/u3-rt-pro", "assemblyai/universal-3-5-pro",
		"elevenlabs/scribe_v2_realtime", "xai/stt-1",
		"speechmatics/enhanced", "speechmatics/standard":
		return true
	default:
		return false
	}
}

func STTAlignedTranscript(models ...string) stt.AlignedTranscript {
	if len(models) == 0 {
		return stt.AlignedTranscriptNone
	}
	for _, model := range models {
		if !hasWordAlignment(model) {
			return stt.AlignedTranscriptNone
		}
	}
	return stt.AlignedTranscriptWord
}

func STTDiarizationEnabled(options ModelOptions) bool {
	for _, key := range [...]string{"diarize", "speaker_labels", "diarization"} {
		value, ok := options[key]
		if !ok || value == nil || value == false || value == "" || value == 0 || value == float64(0) {
			continue
		}
		if text, ok := value.(string); ok && strings.EqualFold(text, "none") {
			continue
		}
		return true
	}
	return false
}

func STTSupportsKeyterms(model string) bool {
	return model != "speechmatics/linden-1" &&
		(strings.HasPrefix(model, "deepgram/") || strings.HasPrefix(model, "assemblyai/") || strings.HasPrefix(model, "speechmatics/"))
}

// MergeSTTKeyterms returns a provider-formatted shallow overlay and never
// mutates caller-owned options. Duplicate terms preserve first-seen order.
func MergeSTTKeyterms(model string, options ModelOptions, sessionTerms []string) (ModelOptions, bool) {
	if !STTSupportsKeyterms(model) {
		return nil, false
	}
	if strings.HasPrefix(model, "speechmatics/") {
		existing := objectSlice(options["additional_vocab"])
		vocabulary := make([]map[string]any, len(existing), len(existing)+len(sessionTerms))
		copy(vocabulary, existing)
		seen := make(map[string]struct{}, len(existing)+len(sessionTerms))
		for _, entry := range existing {
			if term, _ := entry["content"].(string); term != "" {
				seen[term] = struct{}{}
			}
		}
		for _, term := range sessionTerms {
			if _, ok := seen[term]; ok {
				continue
			}
			seen[term] = struct{}{}
			vocabulary = append(vocabulary, map[string]any{"content": term})
		}
		return ModelOptions{"additional_vocab": vocabulary}, true
	}
	key := "keyterm"
	if strings.HasPrefix(model, "assemblyai/") {
		key = "keyterms_prompt"
	}
	terms := stringSlice(options[key])
	seen := make(map[string]struct{}, len(terms)+len(sessionTerms))
	merged := make([]string, 0, len(terms)+len(sessionTerms))
	for _, term := range append(terms, sessionTerms...) {
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		merged = append(merged, term)
	}
	return ModelOptions{key: merged}, true
}

func objectSlice(value any) []map[string]any {
	switch value := value.(type) {
	case []map[string]any:
		return append([]map[string]any(nil), value...)
	case []any:
		result := make([]map[string]any, 0, len(value))
		for _, item := range value {
			if object, ok := item.(map[string]any); ok {
				result = append(result, object)
			}
		}
		return result
	default:
		return nil
	}
}

func stringSlice(value any) []string {
	switch value := value.(type) {
	case string:
		return []string{value}
	case []string:
		return append([]string(nil), value...)
	case []any:
		result := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok {
				result = append(result, text)
			}
		}
		return result
	default:
		return nil
	}
}
