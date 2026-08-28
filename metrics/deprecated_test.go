// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestUsageCollectorConcurrentAndJSON(t *testing.T) {
	t.Parallel()
	collector := NewUsageCollector()
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.Collect(LLM{PromptTokens: 2, PromptCachedTokens: 1, CompletionTokens: 3})
			collector.Collect(&TTS{CharactersCount: 4})
			collector.Collect(STT{AudioDuration: 10 * time.Millisecond})
		}()
	}
	wg.Wait()
	summary := collector.GetSummary()
	if summary.LLMPromptTokens != 32 || summary.TTSCharactersCount != 64 || summary.STTAudioDuration != 160*time.Millisecond {
		t.Fatalf("summary = %#v", summary)
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"llmPromptTokens":32,"llmPromptCachedTokens":16,"llmCompletionTokens":48,"ttsCharactersCount":64,"sttAudioDurationMs":160}` {
		t.Fatalf("JSON = %s", encoded)
	}
}
