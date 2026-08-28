// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestModelUsageCollectorConcurrent(t *testing.T) {
	t.Parallel()
	c := NewModelUsageCollector()
	const workers = 16
	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range iterations {
				c.Collect(LLM{Timestamp: time.Now(), PromptTokens: 2, CompletionTokens: 3, Metadata: Metadata{ModelProvider: "test", ModelName: "model"}})
			}
		}()
	}
	wg.Wait()
	got := c.Snapshot()
	if len(got) != 1 {
		t.Fatalf("len(Snapshot()) = %d", len(got))
	}
	usage := got[0].(LLMUsage)
	if usage.InputTokens != workers*iterations*2 || usage.OutputTokens != workers*iterations*3 {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestModelUsageCollectorAcceptsPointersAndTypedNil(t *testing.T) {
	t.Parallel()
	c := NewModelUsageCollector()
	metric := &LLM{PromptTokens: 2, CompletionTokens: 3, Metadata: Metadata{ModelProvider: "test", ModelName: "model"}}
	c.Collect(metric)
	var nilMetric *LLM
	c.Collect(nilMetric)
	got := c.Snapshot()
	if len(got) != 1 {
		t.Fatalf("len(Snapshot()) = %d", len(got))
	}
	usage := got[0].(LLMUsage)
	if usage.InputTokens != 2 || usage.OutputTokens != 3 {
		t.Fatalf("usage = %#v", usage)
	}
}
