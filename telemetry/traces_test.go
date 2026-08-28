// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/livekit/agents-go/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type capturedSpan struct {
	trace.Span
	mu         sync.Mutex
	recording  bool
	attributes []attribute.KeyValue
	events     []string
	status     codes.Code
	statusText string
	errors     []error
}

func newCapturedSpan(recording bool) *capturedSpan {
	_, base := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	return &capturedSpan{Span: base, recording: recording}
}

func (s *capturedSpan) IsRecording() bool { return s.recording }
func (s *capturedSpan) SetAttributes(values ...attribute.KeyValue) {
	s.mu.Lock()
	s.attributes = append(s.attributes, values...)
	s.mu.Unlock()
}
func (s *capturedSpan) AddEvent(name string, _ ...trace.EventOption) {
	s.mu.Lock()
	s.events = append(s.events, name)
	s.mu.Unlock()
}
func (s *capturedSpan) SetStatus(code codes.Code, description string) {
	s.mu.Lock()
	s.status, s.statusText = code, description
	s.mu.Unlock()
}
func (s *capturedSpan) RecordError(err error, _ ...trace.EventOption) {
	s.mu.Lock()
	s.errors = append(s.errors, err)
	s.mu.Unlock()
}

func capturedAttributes(span *capturedSpan) map[string]attribute.Value {
	span.mu.Lock()
	defer span.mu.Unlock()
	result := make(map[string]attribute.Value, len(span.attributes))
	for _, value := range span.attributes {
		result[string(value.Key)] = value.Value
	}
	return result
}

func TestRecordExceptionRedacted(t *testing.T) {
	span := newCapturedSpan(true)
	RecordException(span, errors.New("secret customer text"), RecordExceptionOptions{Redacted: true})
	attributes := capturedAttributes(span)
	if attributes[AttrExceptionMessage].AsString() != RedactedExceptionMessage || strings.Contains(span.statusText, "secret") || len(span.errors) != 0 {
		t.Fatalf("redacted exception leaked: attrs=%v status=%q errors=%v", attributes, span.statusText, span.errors)
	}
	if span.status != codes.Error || len(span.events) != 1 || span.events[0] != "exception" {
		t.Fatalf("status/events = %v/%v", span.status, span.events)
	}
}

func TestRecordExceptionUnredacted(t *testing.T) {
	span := newCapturedSpan(true)
	err := errors.New("provider unavailable")
	RecordException(span, err, RecordExceptionOptions{})
	attributes := capturedAttributes(span)
	if attributes[AttrExceptionMessage].AsString() != err.Error() || span.statusText != err.Error() || len(span.errors) != 1 {
		t.Fatalf("attrs=%v status=%q errors=%v", attributes, span.statusText, span.errors)
	}
}

func TestRecordRealtimeMetrics(t *testing.T) {
	span := newCapturedSpan(true)
	timestamp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	metric := metrics.Realtime{
		Label: "openai.realtime", Timestamp: timestamp, TimeToFirstToken: 250 * time.Millisecond,
		InputTokens: 10, OutputTokens: 20,
		InputDetails:  metrics.InputTokenDetails{Text: 3, Audio: 6, Cached: 1},
		OutputDetails: metrics.OutputTokenDetails{Text: 8, Audio: 12},
	}
	if err := RecordRealtimeMetrics(context.Background(), span, metric); err != nil {
		t.Fatal(err)
	}
	attributes := capturedAttributes(span)
	if attributes[AttrGenAIRequestModel].AsString() != "openai.realtime" || attributes[AttrGenAIUsageInputTokens].AsInt64() != 10 || attributes[AttrGenAIUsageOutputAudio].AsInt64() != 12 {
		t.Fatalf("attributes = %v", attributes)
	}
	if got := attributes[AttrLangfuseCompletionStartTime].AsString(); got != "2026-01-02T03:04:05.25Z" {
		t.Fatalf("completion start = %q", got)
	}
}

func TestTraceAttributesStableConversion(t *testing.T) {
	values, err := traceAttributes(map[string]any{
		"z": map[string]any{"nested": true},
		"a": []string{"one", "two"},
		"d": 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || values[0].Key != "a" || values[1].Key != "d" || values[2].Key != "z" {
		t.Fatalf("attributes = %v", values)
	}
	if values[1].Value.AsFloat64() != 0.25 || values[2].Value.AsString() != `{"nested":true}` {
		t.Fatalf("converted attributes = %v", values)
	}
}

func TestStartSpanAndWithSpanValidation(t *testing.T) {
	if _, _, err := StartSpan(context.Background(), StartSpanOptions{}); err == nil {
		t.Fatal("empty span name accepted")
	}
	provider := noop.NewTracerProvider()
	if err := SetTracerProvider(provider); err != nil {
		t.Fatal(err)
	}
	want := errors.New("callback")
	err := WithSpan(context.Background(), StartSpanOptions{Name: "operation"}, func(context.Context, trace.Span) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("WithSpan = %v", err)
	}
}

func TestDynamicTracerConcurrentAccess(t *testing.T) {
	provider := noop.NewTracerProvider()
	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if index%8 == 0 {
					_ = SetTracerProvider(provider)
				} else {
					_ = Tracer()
				}
			}
		}(index)
	}
	wait.Wait()
}
