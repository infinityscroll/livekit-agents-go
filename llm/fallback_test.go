// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
)

type fallbackTestPlan struct {
	chunks    []ChatChunk
	failAt    int
	err       error
	wait      bool
	chatError error
}

type fallbackTestLLM struct {
	*Base
	mu        sync.Mutex
	plans     []fallbackTestPlan
	next      int
	chatCount atomic.Int64
	closes    atomic.Int64
}

func newFallbackTestLLM(label string, plans ...fallbackTestPlan) *fallbackTestLLM {
	return &fallbackTestLLM{Base: NewBase(label, "test", label), plans: plans}
}

func (m *fallbackTestLLM) Chat(ctx context.Context, options ChatOptions) (LLMStream, error) {
	m.chatCount.Add(1)
	m.mu.Lock()
	index := m.next
	if index >= len(m.plans) {
		index = len(m.plans) - 1
	} else {
		m.next++
	}
	plan := m.plans[index]
	m.mu.Unlock()
	if plan.chatError != nil {
		return nil, plan.chatError
	}
	stream := NewBaseStream(ctx, m.Base, options, 8)
	go func() {
		if plan.wait {
			<-stream.Context().Done()
			stream.Finish(context.Cause(stream.Context()))
			return
		}
		if plan.failAt == 0 {
			stream.Finish(plan.err)
			return
		}
		for i, chunk := range plan.chunks {
			if plan.failAt == i+1 {
				stream.Finish(plan.err)
				return
			}
			if err := stream.Emit(stream.Context(), chunk); err != nil {
				stream.Finish(err)
				return
			}
		}
		stream.Finish(nil)
	}()
	return stream, nil
}

func (m *fallbackTestLLM) Close(context.Context) error {
	m.closes.Add(1)
	return nil
}

func fallbackChunk(text string) ChatChunk {
	return ChatChunk{ID: "id", Delta: &ChoiceDelta{Role: RoleAssistant, Content: text}}
}

func collectFallbackChunks(t *testing.T, stream LLMStream) ([]ChatChunk, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var chunks []ChatChunk
	for {
		chunk, err := stream.Recv(ctx)
		if errors.Is(err, io.EOF) {
			return chunks, nil
		}
		if err != nil {
			return chunks, err
		}
		chunks = append(chunks, chunk)
	}
}

func TestFallbackAdapterValidationAndPrimary(t *testing.T) {
	t.Parallel()
	if _, err := NewFallbackAdapter(FallbackOptions{}); err == nil {
		t.Fatal("NewFallbackAdapter accepted no providers")
	}
	primary := newFallbackTestLLM("primary", fallbackTestPlan{failAt: -1, chunks: []ChatChunk{fallbackChunk("a"), fallbackChunk("b")}})
	secondary := newFallbackTestLLM("secondary", fallbackTestPlan{failAt: -1, chunks: []ChatChunk{fallbackChunk("c")}})
	adapter, err := NewFallbackAdapter(FallbackOptions{LLMs: []LLM{primary, secondary}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	llmStream, err := adapter.Chat(context.Background(), ChatOptions{ChatContext: EmptyChatContext()})
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := collectFallbackChunks(t, llmStream)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 || primary.chatCount.Load() != 1 || secondary.chatCount.Load() != 0 {
		t.Fatalf("chunks=%d primary calls=%d secondary calls=%d", len(chunks), primary.chatCount.Load(), secondary.chatCount.Load())
	}
}

func TestFallbackAdapterFallsBackAndEmitsAvailability(t *testing.T) {
	t.Parallel()
	failure := agents.NewAPIConnectionError("primary failed", true, nil)
	primary := newFallbackTestLLM("primary", fallbackTestPlan{failAt: 0, err: failure})
	secondary := newFallbackTestLLM("secondary", fallbackTestPlan{failAt: -1, chunks: []ChatChunk{fallbackChunk("ok")}})
	adapter, err := NewFallbackAdapter(FallbackOptions{LLMs: []LLM{primary, secondary}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	events := make(chan AvailabilityChangedEvent, 4)
	unsubscribe := adapter.OnAvailabilityChanged(func(event AvailabilityChangedEvent) { events <- event })
	defer unsubscribe()

	llmStream, err := adapter.Chat(context.Background(), ChatOptions{})
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := collectFallbackChunks(t, llmStream)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0].Delta.Content != "ok" {
		t.Fatalf("unexpected chunks: %#v", chunks)
	}
	select {
	case event := <-events:
		if event.LLM != primary || event.Available {
			t.Fatalf("unexpected availability event: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("availability event not emitted")
	}
}

func TestFallbackAdapterMidStreamPolicy(t *testing.T) {
	t.Parallel()
	failure := agents.NewAPIError("midstream", nil, true, nil)
	for _, retry := range []bool{false, true} {
		retry := retry
		t.Run(map[bool]string{false: "stop", true: "retry"}[retry], func(t *testing.T) {
			t.Parallel()
			primary := newFallbackTestLLM("primary", fallbackTestPlan{failAt: 2, err: failure, chunks: []ChatChunk{fallbackChunk("partial"), fallbackChunk("lost")}})
			secondary := newFallbackTestLLM("secondary", fallbackTestPlan{failAt: -1, chunks: []ChatChunk{fallbackChunk("fallback")}})
			adapter, err := NewFallbackAdapter(FallbackOptions{LLMs: []LLM{primary, secondary}, RetryOnChunkSent: retry})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = adapter.Close(context.Background()) })
			llmStream, err := adapter.Chat(context.Background(), ChatOptions{})
			if err != nil {
				t.Fatal(err)
			}
			chunks, gotErr := collectFallbackChunks(t, llmStream)
			if retry {
				if gotErr != nil || len(chunks) != 2 || secondary.chatCount.Load() != 1 {
					t.Fatalf("retry chunks=%d err=%v secondary calls=%d", len(chunks), gotErr, secondary.chatCount.Load())
				}
			} else {
				if !errors.Is(gotErr, failure) || len(chunks) != 1 || secondary.chatCount.Load() != 0 {
					t.Fatalf("stop chunks=%d err=%v secondary calls=%d", len(chunks), gotErr, secondary.chatCount.Load())
				}
			}
		})
	}
}

func TestFallbackAdapterAttemptTimeoutAndAllFailed(t *testing.T) {
	t.Parallel()
	stuck := newFallbackTestLLM("stuck", fallbackTestPlan{wait: true, failAt: -1})
	failed := newFallbackTestLLM("failed", fallbackTestPlan{chatError: agents.NewAPIConnectionError("no", true, nil)})
	adapter, err := NewFallbackAdapter(FallbackOptions{LLMs: []LLM{stuck, failed}, AttemptTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	llmStream, err := adapter.Chat(context.Background(), ChatOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, gotErr := collectFallbackChunks(t, llmStream)
	var connectionErr *agents.APIConnectionError
	if !errors.As(gotErr, &connectionErr) {
		t.Fatalf("error = %T %v, want APIConnectionError", gotErr, gotErr)
	}
}

func TestFallbackAdapterRecoveryAndForwarding(t *testing.T) {
	t.Parallel()
	failure := agents.NewAPIConnectionError("down", true, nil)
	primary := newFallbackTestLLM("primary",
		fallbackTestPlan{failAt: 0, err: failure},
		fallbackTestPlan{failAt: -1, chunks: []ChatChunk{fallbackChunk("recovered")}},
	)
	secondary := newFallbackTestLLM("secondary", fallbackTestPlan{failAt: -1, chunks: []ChatChunk{fallbackChunk("secondary")}})
	adapter, err := NewFallbackAdapter(FallbackOptions{LLMs: []LLM{primary, secondary}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	events := make(chan AvailabilityChangedEvent, 4)
	adapter.OnAvailabilityChanged(func(event AvailabilityChangedEvent) { events <- event })
	llmStream, err := adapter.Chat(context.Background(), ChatOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collectFallbackChunks(t, llmStream); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	recovered := false
	for !recovered {
		select {
		case event := <-events:
			recovered = event.LLM == primary && event.Available
		case <-deadline:
			t.Fatal("provider did not recover")
		}
	}

	metricsCh := make(chan metrics.LLM, 1)
	adapter.OnMetrics(func(metric metrics.LLM) { metricsCh <- metric })
	wantMetric := metrics.LLM{Label: "child"}
	primary.EmitMetrics(wantMetric)
	select {
	case got := <-metricsCh:
		if got.Label != wantMetric.Label {
			t.Fatalf("forwarded metric = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("child metric not forwarded")
	}
}

func TestFallbackAdapterCloseClosesProviders(t *testing.T) {
	t.Parallel()
	provider := newFallbackTestLLM("provider", fallbackTestPlan{failAt: -1})
	adapter, err := NewFallbackAdapter(FallbackOptions{LLMs: []LLM{provider}})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if provider.closes.Load() != 1 {
		t.Fatalf("provider closes = %d", provider.closes.Load())
	}
	if _, err := adapter.Chat(context.Background(), ChatOptions{}); !errors.Is(err, stream.ErrClosed) {
		t.Fatalf("Chat after Close error = %v", err)
	}
}
