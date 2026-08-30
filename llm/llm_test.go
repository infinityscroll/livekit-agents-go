// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/metrics"
)

func TestBaseStreamCollectAndMetrics(t *testing.T) {
	t.Parallel()
	base := NewBase("test", "provider", "model")
	metricCh := make(chan metrics.LLM, 1)
	base.OnMetrics(func(metric metrics.LLM) { metricCh <- metric })
	stream := NewBaseStream(context.Background(), base, ChatOptions{}, 4)
	if err := stream.Emit(context.Background(), ChatChunk{ID: "request", Delta: &ChoiceDelta{Role: RoleAssistant, Content: " hello"}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Emit(context.Background(), ChatChunk{ID: "request", Delta: &ChoiceDelta{Role: RoleAssistant, Content: " world "}, Usage: &CompletionUsage{PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5}}); err != nil {
		t.Fatal(err)
	}
	stream.Finish(nil)
	response, err := stream.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if response.Text != "hello world" || response.Usage == nil || response.Usage.TotalTokens != 5 {
		t.Fatalf("response = %#v", response)
	}
	select {
	case metric := <-metricCh:
		if metric.RequestID != "request" || metric.CompletionTokens != 3 || metric.TimeToFirstToken < 0 {
			t.Fatalf("metric = %#v", metric)
		}
	case <-time.After(time.Second):
		t.Fatal("metrics not emitted")
	}
}
