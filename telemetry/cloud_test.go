// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestSetupCloudTracerExportsOTLPAndMetadata(t *testing.T) {
	var requests atomic.Int64
	var mu sync.Mutex
	var exported collectortrace.ExportTraceServiceRequest
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.URL.Path != "/observability/traces/otlp/v0" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") {
			t.Error("authorization header missing")
		}
		var reader io.Reader = request.Body
		if request.Header.Get("Content-Encoding") == "gzip" {
			compressed, err := gzip.NewReader(request.Body)
			if err != nil {
				t.Errorf("gzip reader: %v", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			defer compressed.Close()
			reader = compressed
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Errorf("read trace body: %v", err)
		}
		mu.Lock()
		err = proto.Unmarshal(body, &exported)
		mu.Unlock()
		if err != nil {
			t.Errorf("decode OTLP trace: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	traces, logs := true, false
	gate := NewUploadGate(nil)
	runtime, err := SetupCloudTracer(context.Background(), SetupCloudTracerOptions{
		RoomID: "RM_test", JobID: "AJ_test", CloudHostname: server.URL,
		AgentName: "concierge", EnableTraces: &traces, EnableLogs: &logs,
		Metadata: map[string]any{"tenant": "acme"},
		APIKey:   agents.NewSecretString("api-key"), APISecret: agents.NewSecretString("api-secret"),
		HTTPClient: server.Client(), UploadGate: gate,
		TraceBatchWait: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, span := Tracer().Start(context.Background(), "agent.turn")
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Fatalf("trace requests = %d", requests.Load())
	}
	mu.Lock()
	if len(exported.ResourceSpans) != 1 || len(exported.ResourceSpans[0].ScopeSpans) != 1 || len(exported.ResourceSpans[0].ScopeSpans[0].Spans) != 1 {
		mu.Unlock()
		t.Fatalf("exported trace shape = %#v", &exported)
	}
	resourceAttrs := otlpAttributes(exported.ResourceSpans[0].Resource.Attributes)
	spanAttrs := otlpAttributes(exported.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes)
	spanName := exported.ResourceSpans[0].ScopeSpans[0].Spans[0].Name
	mu.Unlock()
	if spanName != "agent.turn" || resourceAttrs["room_id"] != "RM_test" || resourceAttrs["job_id"] != "AJ_test" || resourceAttrs["service.name"] != "livekit-agents" {
		t.Fatalf("resource/name = %q %#v", spanName, resourceAttrs)
	}
	if spanAttrs["tenant"] != "acme" || spanAttrs[AttrAgentName] != "concierge" {
		t.Fatalf("span attrs = %#v", spanAttrs)
	}
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatalf("idempotent shutdown: %v", err)
	}
}

func TestCloudTraceUploadGateDisablesFutureRequests(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusUnauthorized)
		_, _ = writer.Write([]byte("data recording is disabled by owner"))
	}))
	defer server.Close()
	traces, logs := true, false
	gate := NewUploadGate(nil)
	runtime, err := SetupCloudTracer(context.Background(), SetupCloudTracerOptions{
		RoomID: "RM_gate", JobID: "AJ_gate", CloudHostname: server.URL,
		EnableTraces: &traces, EnableLogs: &logs,
		APIKey: agents.NewSecretString("key"), APISecret: agents.NewSecretString("secret"),
		HTTPClient: server.Client(), UploadGate: gate, TraceBatchWait: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, first := Tracer().Start(context.Background(), "first")
	first.End()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.ForceFlush(ctx); err == nil {
		t.Fatal("unauthorized export unexpectedly succeeded")
	}
	if !gate.Disabled() || requests.Load() != 1 {
		t.Fatalf("gate/requests = %v/%d", gate.Disabled(), requests.Load())
	}
	_, second := Tracer().Start(context.Background(), "second")
	second.End()
	if err := runtime.ForceFlush(ctx); err != nil {
		t.Fatalf("disabled export should be a no-op: %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("disabled gate made %d requests", requests.Load())
	}
	_ = runtime.Shutdown(ctx)
}

type cloudTestProcessor struct {
	starts   atomic.Int64
	ends     atomic.Int64
	flushes  atomic.Int64
	shutdown atomic.Int64
}

func (p *cloudTestProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) { p.starts.Add(1) }
func (p *cloudTestProcessor) OnEnd(sdktrace.ReadOnlySpan)                     { p.ends.Add(1) }
func (p *cloudTestProcessor) ForceFlush(context.Context) error {
	p.flushes.Add(1)
	return nil
}
func (p *cloudTestProcessor) Shutdown(context.Context) error {
	p.shutdown.Add(1)
	return nil
}

func TestFanoutSpanProcessorLifecycle(t *testing.T) {
	fanout := &FanoutSpanProcessor{}
	first, second := &cloudTestProcessor{}, &cloudTestProcessor{}
	if err := fanout.Add(first); err != nil {
		t.Fatal(err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(fanout))
	_, span := provider.Tracer("test").Start(context.Background(), "one")
	span.End()
	if first.starts.Load() != 1 || first.ends.Load() != 1 {
		t.Fatalf("first counts = %d/%d", first.starts.Load(), first.ends.Load())
	}
	if err := fanout.Add(second); err != nil {
		t.Fatal(err)
	}
	_, span = provider.Tracer("test").Start(context.Background(), "two")
	span.End()
	if first.starts.Load() != 2 || second.starts.Load() != 1 || second.ends.Load() != 1 {
		t.Fatalf("fanout counts = %d/%d/%d", first.starts.Load(), second.starts.Load(), second.ends.Load())
	}
	if err := fanout.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first.flushes.Load() != 1 || second.flushes.Load() != 1 {
		t.Fatal("force flush was not fanned out")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first.shutdown.Load() != 1 || second.shutdown.Load() != 1 {
		t.Fatal("shutdown was not fanned out")
	}
	if err := fanout.Add(&cloudTestProcessor{}); err == nil {
		t.Fatal("closed fanout accepted a processor")
	}
}

func TestSetupCloudTracerValidation(t *testing.T) {
	if _, err := SetupCloudTracer(context.Background(), SetupCloudTracerOptions{}); err == nil {
		t.Fatal("missing identifiers accepted")
	}
	if _, err := SetupCloudTracer(context.Background(), SetupCloudTracerOptions{RoomID: "r", JobID: "j", CloudHostname: "https://host/path"}); err == nil {
		t.Fatal("hostname path accepted")
	}
}

func otlpAttributes(values []*commonv1.KeyValue) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		if value != nil && value.Value != nil {
			result[value.Key] = value.Value.GetStringValue()
		}
	}
	return result
}
