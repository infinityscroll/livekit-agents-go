// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type capturedOTLPLog struct {
	Body       string
	Severity   string
	TraceID    string
	SpanID     string
	Attributes map[string]any
}

func TestCloudLogHandlerBatchesGroupsErrorsAndTraceContext(t *testing.T) {
	var mu sync.Mutex
	var records []capturedOTLPLog
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		var payload struct {
			ResourceLogs []struct {
				ScopeLogs []struct {
					LogRecords []struct {
						Body struct {
							StringValue string `json:"stringValue"`
						} `json:"body"`
						SeverityText string `json:"severityText"`
						TraceID      string `json:"traceId"`
						SpanID       string `json:"spanId"`
						Attributes   []struct {
							Key   string         `json:"key"`
							Value map[string]any `json:"value"`
						} `json:"attributes"`
					} `json:"logRecords"`
				} `json:"scopeLogs"`
			} `json:"resourceLogs"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode payload: %v", err)
			return
		}
		mu.Lock()
		for _, resource := range payload.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				for _, record := range scope.LogRecords {
					attrs := make(map[string]any, len(record.Attributes))
					for _, attr := range record.Attributes {
						attrs[attr.Key] = decodeOTLPJSONValue(attr.Value)
					}
					records = append(records, capturedOTLPLog{
						Body: record.Body.StringValue, Severity: record.SeverityText,
						TraceID: record.TraceID, SpanID: record.SpanID, Attributes: attrs,
					})
				}
			}
		}
		mu.Unlock()
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	exporter, err := NewSimpleOTLPHTTPLogExporter(SimpleOTLPHTTPLogExporterConfig{
		Endpoint: server.URL, ScopeName: "test", APIKey: agents.NewSecretString("key"),
		APISecret: agents.NewSecretString("secret"), HTTPClient: server.Client(), UploadGate: NewUploadGate(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewCloudLogHandler(CloudLogHandlerOptions{
		Exporter: exporter, BatchSize: 8, FlushInterval: time.Hour,
		StaticAttrs: map[string]any{"service.instance.id": "worker-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	ctx, span := provider.Tracer("test").Start(context.Background(), "parent")
	logger := slog.New(handler).WithGroup("request").With("room", "RM1")
	logger.LogAttrs(ctx, slog.LevelError, "provider failed", slog.Any("error", errors.New("offline")), slog.Int("attempt", 2))
	spanContext := trace.SpanContextFromContext(ctx)
	span.End()
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := handler.Flush(flushCtx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(records) != 1 {
		mu.Unlock()
		t.Fatalf("records = %#v", records)
	}
	record := records[0]
	mu.Unlock()
	if record.Body != "provider failed" || record.Severity != "error" || record.TraceID != spanContext.TraceID().String() || record.SpanID != spanContext.SpanID().String() {
		t.Fatalf("record envelope = %#v", record)
	}
	if record.Attributes["request.room"] != "RM1" || record.Attributes["request.attempt"] != "2" || record.Attributes["service.instance.id"] != "worker-1" {
		t.Fatalf("record attrs = %#v", record.Attributes)
	}
	errorValue, ok := record.Attributes["request.error"].(map[string]any)
	if !ok || errorValue["message"] != "offline" {
		t.Fatalf("error attr = %#v", record.Attributes["request.error"])
	}
	if err := handler.Shutdown(flushCtx); err != nil {
		t.Fatal(err)
	}
}

func TestCloudLogHandlerDropsLowSeverityWhenSaturated(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	exporter, err := NewSimpleOTLPHTTPLogExporter(SimpleOTLPHTTPLogExporterConfig{
		Endpoint: server.URL, ScopeName: "test", APIKey: agents.NewSecretString("key"),
		APISecret: agents.NewSecretString("secret"), HTTPClient: server.Client(), UploadGate: NewUploadGate(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewCloudLogHandler(CloudLogHandlerOptions{Exporter: exporter, Capacity: 1, BatchSize: 1, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(handler)
	logger.Info("first")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("export did not start")
	}
	logger.Info("queued")
	logger.Info("dropped")
	if handler.Dropped() != 1 {
		t.Fatalf("dropped = %d", handler.Dropped())
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := handler.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func decodeOTLPJSONValue(value map[string]any) any {
	for _, key := range []string{"stringValue", "intValue", "doubleValue", "boolValue", "kvlistValue", "arrayValue"} {
		if result, ok := value[key]; ok {
			if key != "kvlistValue" {
				return result
			}
			wire, _ := result.(map[string]any)
			entries, _ := wire["values"].([]any)
			decoded := make(map[string]any, len(entries))
			for _, entry := range entries {
				pair, _ := entry.(map[string]any)
				name, _ := pair["key"].(string)
				nested, _ := pair["value"].(map[string]any)
				decoded[name] = decodeOTLPJSONValue(nested)
			}
			return decoded
		}
	}
	return nil
}
