// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"encoding/json"
	"sync"
	"time"
)

// UsageSummary is retained for source compatibility.
// Deprecated: use ModelUsageCollector for provider/model attribution.
type UsageSummary struct {
	LLMPromptTokens       int64
	LLMPromptCachedTokens int64
	LLMCompletionTokens   int64
	TTSCharactersCount    int64
	STTAudioDuration      time.Duration
}

func (s UsageSummary) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		LLMPromptTokens       int64   `json:"llmPromptTokens"`
		LLMPromptCachedTokens int64   `json:"llmPromptCachedTokens"`
		LLMCompletionTokens   int64   `json:"llmCompletionTokens"`
		TTSCharactersCount    int64   `json:"ttsCharactersCount"`
		STTAudioDurationMS    float64 `json:"sttAudioDurationMs"`
	}{s.LLMPromptTokens, s.LLMPromptCachedTokens, s.LLMCompletionTokens, s.TTSCharactersCount, durationMilliseconds(s.STTAudioDuration)})
}

// UsageCollector aggregates the legacy global counters.
// Deprecated: use ModelUsageCollector.
type UsageCollector struct {
	mu      sync.Mutex
	summary UsageSummary
}

func NewUsageCollector() *UsageCollector { return &UsageCollector{} }

func (c *UsageCollector) Collect(metric Metric) {
	if c == nil || metric == nil {
		return
	}
	c.mu.Lock()
	switch m := metric.(type) {
	case LLM:
		c.summary.LLMPromptTokens += m.PromptTokens
		c.summary.LLMPromptCachedTokens += m.PromptCachedTokens
		c.summary.LLMCompletionTokens += m.CompletionTokens
	case *LLM:
		if m != nil {
			c.summary.LLMPromptTokens += m.PromptTokens
			c.summary.LLMPromptCachedTokens += m.PromptCachedTokens
			c.summary.LLMCompletionTokens += m.CompletionTokens
		}
	case Realtime:
		c.summary.LLMPromptTokens += m.InputTokens
		c.summary.LLMPromptCachedTokens += m.InputDetails.Cached
		c.summary.LLMCompletionTokens += m.OutputTokens
	case *Realtime:
		if m != nil {
			c.summary.LLMPromptTokens += m.InputTokens
			c.summary.LLMPromptCachedTokens += m.InputDetails.Cached
			c.summary.LLMCompletionTokens += m.OutputTokens
		}
	case TTS:
		c.summary.TTSCharactersCount += m.CharactersCount
	case *TTS:
		if m != nil {
			c.summary.TTSCharactersCount += m.CharactersCount
		}
	case STT:
		c.summary.STTAudioDuration += m.AudioDuration
	case *STT:
		if m != nil {
			c.summary.STTAudioDuration += m.AudioDuration
		}
	}
	c.mu.Unlock()
}

func (c *UsageCollector) Summary() UsageSummary {
	if c == nil {
		return UsageSummary{}
	}
	c.mu.Lock()
	result := c.summary
	c.mu.Unlock()
	return result
}

// GetSummary is the agents-js compatibility name for Summary.
func (c *UsageCollector) GetSummary() UsageSummary { return c.Summary() }
