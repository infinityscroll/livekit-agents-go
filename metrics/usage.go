// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

type UsageKind string

const (
	UsageLLM          UsageKind = "llm_usage"
	UsageTTS          UsageKind = "tts_usage"
	UsageSTT          UsageKind = "stt_usage"
	UsageInterruption UsageKind = "interruption_usage"
	UsageEOT          UsageKind = "eot_usage"
)

type ModelUsage interface {
	UsageKind() UsageKind
	UsageProvider() string
	UsageModel() string
}

type LLMUsage struct {
	Provider                 string
	Model                    string
	InputTokens              int64
	InputCachedTokens        int64
	InputCacheCreationTokens int64
	InputAudioTokens         int64
	InputCachedAudioTokens   int64
	InputTextTokens          int64
	InputCachedTextTokens    int64
	InputImageTokens         int64
	InputCachedImageTokens   int64
	OutputTokens             int64
	OutputAudioTokens        int64
	OutputTextTokens         int64
	SessionDuration          time.Duration
}

func (u LLMUsage) UsageKind() UsageKind  { return UsageLLM }
func (u LLMUsage) UsageProvider() string { return u.Provider }
func (u LLMUsage) UsageModel() string    { return u.Model }

func (u LLMUsage) MarshalJSON() ([]byte, error) { return json.Marshal(modelUsageValues(u, false)) }

type TTSUsage struct {
	Provider        string
	Model           string
	InputTokens     int64
	OutputTokens    int64
	CharactersCount int64
	AudioDuration   time.Duration
}

func (u TTSUsage) UsageKind() UsageKind  { return UsageTTS }
func (u TTSUsage) UsageProvider() string { return u.Provider }
func (u TTSUsage) UsageModel() string    { return u.Model }

func (u TTSUsage) MarshalJSON() ([]byte, error) { return json.Marshal(modelUsageValues(u, false)) }

type STTUsage struct {
	Provider      string
	Model         string
	InputTokens   int64
	OutputTokens  int64
	AudioDuration time.Duration
}

func (u STTUsage) UsageKind() UsageKind  { return UsageSTT }
func (u STTUsage) UsageProvider() string { return u.Provider }
func (u STTUsage) UsageModel() string    { return u.Model }

func (u STTUsage) MarshalJSON() ([]byte, error) { return json.Marshal(modelUsageValues(u, false)) }

type RequestUsage struct {
	Kind          UsageKind
	Provider      string
	Model         string
	TotalRequests int64
}

func (u RequestUsage) UsageKind() UsageKind  { return u.Kind }
func (u RequestUsage) UsageProvider() string { return u.Provider }
func (u RequestUsage) UsageModel() string    { return u.Model }

func (u RequestUsage) MarshalJSON() ([]byte, error) { return json.Marshal(modelUsageValues(u, false)) }

// FilterZeroValues returns the agents-js wire representation of usage with
// zero-valued numeric fields removed. The type, provider, and model fields are
// retained even when their strings are empty, matching agents-js.
func FilterZeroValues(usage ModelUsage) map[string]any {
	return modelUsageValues(usage, true)
}

func modelUsageValues(usage ModelUsage, omitZero bool) map[string]any {
	values := make(map[string]any, 20)
	putInt := func(key string, value int64) {
		if !omitZero || value != 0 {
			values[key] = value
		}
	}
	putDuration := func(key string, value time.Duration) {
		if !omitZero || value != 0 {
			values[key] = durationMilliseconds(value)
		}
	}
	switch u := usage.(type) {
	case LLMUsage:
		values["type"], values["provider"], values["model"] = UsageLLM, u.Provider, u.Model
		putInt("inputTokens", u.InputTokens)
		putInt("inputCachedTokens", u.InputCachedTokens)
		putInt("inputCacheCreationTokens", u.InputCacheCreationTokens)
		putInt("inputAudioTokens", u.InputAudioTokens)
		putInt("inputCachedAudioTokens", u.InputCachedAudioTokens)
		putInt("inputTextTokens", u.InputTextTokens)
		putInt("inputCachedTextTokens", u.InputCachedTextTokens)
		putInt("inputImageTokens", u.InputImageTokens)
		putInt("inputCachedImageTokens", u.InputCachedImageTokens)
		putInt("outputTokens", u.OutputTokens)
		putInt("outputAudioTokens", u.OutputAudioTokens)
		putInt("outputTextTokens", u.OutputTextTokens)
		putDuration("sessionDurationMs", u.SessionDuration)
	case TTSUsage:
		values["type"], values["provider"], values["model"] = UsageTTS, u.Provider, u.Model
		putInt("inputTokens", u.InputTokens)
		putInt("outputTokens", u.OutputTokens)
		putInt("charactersCount", u.CharactersCount)
		putDuration("audioDurationMs", u.AudioDuration)
	case STTUsage:
		values["type"], values["provider"], values["model"] = UsageSTT, u.Provider, u.Model
		putInt("inputTokens", u.InputTokens)
		putInt("outputTokens", u.OutputTokens)
		putDuration("audioDurationMs", u.AudioDuration)
	case RequestUsage:
		values["type"], values["provider"], values["model"] = u.Kind, u.Provider, u.Model
		putInt("totalRequests", u.TotalRequests)
	case *LLMUsage:
		if u != nil {
			return modelUsageValues(*u, omitZero)
		}
	case *TTSUsage:
		if u != nil {
			return modelUsageValues(*u, omitZero)
		}
	case *STTUsage:
		if u != nil {
			return modelUsageValues(*u, omitZero)
		}
	case *RequestUsage:
		if u != nil {
			return modelUsageValues(*u, omitZero)
		}
	}
	return values
}

// ModelUsageCollector is safe for concurrent collection and snapshots.
type ModelUsageCollector struct {
	mu           sync.Mutex
	llm          map[string]*LLMUsage
	tts          map[string]*TTSUsage
	stt          map[string]*STTUsage
	interruption map[string]*RequestUsage
	eot          map[string]*RequestUsage
}

func NewModelUsageCollector() *ModelUsageCollector {
	return &ModelUsageCollector{
		llm:          make(map[string]*LLMUsage),
		tts:          make(map[string]*TTSUsage),
		stt:          make(map[string]*STTUsage),
		interruption: make(map[string]*RequestUsage),
		eot:          make(map[string]*RequestUsage),
	}
}

func modelKey(provider, model string) string { return provider + "\x00" + model }

func (c *ModelUsageCollector) Collect(metric Metric) {
	if metric == nil {
		return
	}
	// All metric methods use value receivers, so pointers also satisfy Metric.
	// Normalize them before calling a method to make typed nils harmless and to
	// keep the aggregation switch independent of caller representation.
	switch m := metric.(type) {
	case *LLM:
		if m == nil {
			return
		}
		metric = *m
	case *STT:
		if m == nil {
			return
		}
		metric = *m
	case *TTS:
		if m == nil {
			return
		}
		metric = *m
	case *VAD:
		if m == nil {
			return
		}
		metric = *m
	case *EOU:
		if m == nil {
			return
		}
		metric = *m
	case *Realtime:
		if m == nil {
			return
		}
		metric = *m
	case *EOTInference:
		if m == nil {
			return
		}
		metric = *m
	case *Interruption:
		if m == nil {
			return
		}
		metric = *m
	case *Avatar:
		if m == nil {
			return
		}
		metric = *m
	}
	meta := metric.MetricMetadata()
	key := modelKey(meta.ModelProvider, meta.ModelName)
	c.mu.Lock()
	defer c.mu.Unlock()
	switch m := metric.(type) {
	case LLM:
		u := c.llm[key]
		if u == nil {
			u = &LLMUsage{Provider: meta.ModelProvider, Model: meta.ModelName}
			c.llm[key] = u
		}
		u.InputTokens += m.PromptTokens
		u.InputCachedTokens += m.PromptCachedTokens
		u.InputCacheCreationTokens += m.CacheCreationTokens
		u.OutputTokens += m.CompletionTokens
	case Realtime:
		u := c.llm[key]
		if u == nil {
			u = &LLMUsage{Provider: meta.ModelProvider, Model: meta.ModelName}
			c.llm[key] = u
		}
		u.InputTokens += m.InputTokens
		u.InputCachedTokens += m.InputDetails.Cached
		u.InputTextTokens += m.InputDetails.Text
		u.InputCachedTextTokens += m.InputDetails.CachedDetails.Text
		u.InputImageTokens += m.InputDetails.Image
		u.InputCachedImageTokens += m.InputDetails.CachedDetails.Image
		u.InputAudioTokens += m.InputDetails.Audio
		u.InputCachedAudioTokens += m.InputDetails.CachedDetails.Audio
		u.OutputTextTokens += m.OutputDetails.Text
		u.OutputAudioTokens += m.OutputDetails.Audio
		u.OutputTokens += m.OutputTokens
		u.SessionDuration += m.SessionDuration
	case TTS:
		u := c.tts[key]
		if u == nil {
			u = &TTSUsage{Provider: meta.ModelProvider, Model: meta.ModelName}
			c.tts[key] = u
		}
		u.InputTokens += m.InputTokens
		u.OutputTokens += m.OutputTokens
		u.CharactersCount += m.CharactersCount
		u.AudioDuration += m.AudioDuration
	case STT:
		u := c.stt[key]
		if u == nil {
			u = &STTUsage{Provider: meta.ModelProvider, Model: meta.ModelName}
			c.stt[key] = u
		}
		u.InputTokens += m.InputTokens
		u.OutputTokens += m.OutputTokens
		u.AudioDuration += m.AudioDuration
	case Interruption:
		u := c.interruption[key]
		if u == nil {
			u = &RequestUsage{Kind: UsageInterruption, Provider: meta.ModelProvider, Model: meta.ModelName}
			c.interruption[key] = u
		}
		u.TotalRequests += m.NumRequests
	case EOTInference:
		u := c.eot[key]
		if u == nil {
			u = &RequestUsage{Kind: UsageEOT, Provider: meta.ModelProvider, Model: meta.ModelName}
			c.eot[key] = u
		}
		u.TotalRequests += m.NumRequests
	}
}

func (c *ModelUsageCollector) Snapshot() []ModelUsage {
	c.mu.Lock()
	result := make([]ModelUsage, 0, len(c.llm)+len(c.tts)+len(c.stt)+len(c.interruption)+len(c.eot))
	for _, usage := range c.llm {
		copy := *usage
		result = append(result, copy)
	}
	for _, usage := range c.tts {
		copy := *usage
		result = append(result, copy)
	}
	for _, usage := range c.stt {
		copy := *usage
		result = append(result, copy)
	}
	for _, usage := range c.interruption {
		copy := *usage
		result = append(result, copy)
	}
	for _, usage := range c.eot {
		copy := *usage
		result = append(result, copy)
	}
	c.mu.Unlock()
	sort.Slice(result, func(i, j int) bool {
		if result[i].UsageKind() != result[j].UsageKind() {
			return result[i].UsageKind() < result[j].UsageKind()
		}
		if result[i].UsageProvider() != result[j].UsageProvider() {
			return result[i].UsageProvider() < result[j].UsageProvider()
		}
		return result[i].UsageModel() < result[j].UsageModel()
	})
	return result
}
