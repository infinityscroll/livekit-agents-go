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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
)

func testExporter(t *testing.T, endpoint string, gate *UploadGate, client *http.Client) *SimpleOTLPHTTPLogExporter {
	t.Helper()
	exporter, err := NewSimpleOTLPHTTPLogExporter(SimpleOTLPHTTPLogExporterConfig{
		Endpoint: endpoint, ScopeName: "chat_history", HTTPClient: client, UploadGate: gate,
		APIKey: agents.NewSecretString("api-key"), APISecret: agents.NewSecretString("api-secret-at-least-32-bytes-long"),
		ResourceAttributes: map[string]any{"room_id": "room"},
		ScopeAttributes:    map[string]any{"job_id": "job"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return exporter
}

func TestSimpleOTLPHTTPLogExporterPayload(t *testing.T) {
	var body []byte
	var authorization, contentType string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		authorization = request.Header.Get("Authorization")
		contentType = request.Header.Get("Content-Type")
		var err error
		body, err = io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	exporter := testExporter(t, server.URL, NewUploadGate(slog.New(slog.NewTextHandler(io.Discard, nil))), server.Client())
	timestamp := time.Unix(1_700_000_000, 123_000_000)
	err := exporter.Export(context.Background(), []SimpleLogRecord{{
		Body: "chat item", Timestamp: timestamp, SeverityNumber: 17, SeverityText: "error",
		Attributes: map[string]any{
			"count": 2.0, "transcriptConfidence": 1.0, "nested": map[string]any{"ok": true},
			"items": []any{"text", int64(4)},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(authorization, "Bearer ") || strings.Contains(authorization, "api-secret") || contentType != "application/json" {
		t.Fatalf("headers authorization=%q content-type=%q", authorization, contentType)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	encoded := string(body)
	for _, expected := range []string{
		`"service.name"`, `"livekit-agents"`, `"timeUnixNano":"1700000000123000000"`,
		`"severityNumber":17`, `"intValue":"2"`, `"doubleValue":1`, `"boolValue":true`,
	} {
		if !strings.Contains(encoded, expected) {
			t.Fatalf("payload missing %s: %s", expected, encoded)
		}
	}
}

func TestSimpleExporterEmptyIsLazy(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	exporter, err := NewSimpleOTLPHTTPLogExporter(SimpleOTLPHTTPLogExporterConfig{Endpoint: server.URL, ScopeName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.Export(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("empty export performed network I/O")
	}
}

func TestUploadGateDisablesAndResets(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			response.WriteHeader(http.StatusUnauthorized)
			_, _ = response.Write([]byte("Data recording is disabled by owner"))
			return
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	gate := NewUploadGate(slog.New(slog.NewTextHandler(io.Discard, nil)))
	gate.Reset()
	exporter := testExporter(t, server.URL, gate, server.Client())
	record := []SimpleLogRecord{{Body: "test", Timestamp: time.Now()}}
	if err := exporter.Export(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if !gate.Disabled() {
		t.Fatal("gate did not disable")
	}
	if err := exporter.Export(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("disabled export made %d requests", calls.Load())
	}
	oldGeneration := gate.Generation()
	gate.Reset()
	if gate.Disable(oldGeneration) {
		t.Fatal("stale generation disabled reset gate")
	}
	if err := exporter.Export(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("reset export calls = %d", calls.Load())
	}
}

func TestExporterReturnsTypedBoundedError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusBadGateway)
		_, _ = response.Write([]byte(strings.Repeat("x", 100)))
	}))
	defer server.Close()
	exporter := testExporter(t, server.URL, NewUploadGate(nil), server.Client())
	exporter.config.MaxErrorBodyBytes = 16
	err := exporter.Export(context.Background(), []SimpleLogRecord{{Body: "test"}})
	var exportError *LogExportError
	if !errors.As(err, &exportError) || exportError.StatusCode != http.StatusBadGateway || len(exportError.Body) > 19 || !strings.HasSuffix(exportError.Body, "…") {
		t.Fatalf("error = %#v", err)
	}
}

func TestExporterRequestLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("request should not be sent") }))
	defer server.Close()
	exporter := testExporter(t, server.URL, NewUploadGate(nil), server.Client())
	exporter.config.MaxRequestBytes = 32
	err := exporter.Export(context.Background(), []SimpleLogRecord{{Body: strings.Repeat("x", 100)}})
	if err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("error = %v", err)
	}
}

func TestUploadGateConcurrentAccess(t *testing.T) {
	gate := NewUploadGate(slog.New(slog.NewTextHandler(io.Discard, nil)))
	generation := gate.Reset()
	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			for iteration := 0; iteration < 100; iteration++ {
				_ = gate.Disabled()
				_ = gate.Generation()
				if index == 0 {
					gate.Disable(generation)
				}
			}
		}(index)
	}
	wait.Wait()
	if !gate.Disabled() {
		t.Fatal("gate should be disabled")
	}
}

func TestExporterConfigValidation(t *testing.T) {
	for _, config := range []SimpleOTLPHTTPLogExporterConfig{
		{},
		{ScopeName: "scope", CloudHostname: "https://example.com/path"},
		{ScopeName: "scope", Endpoint: "https://user:password@example.com"},
	} {
		if _, err := NewSimpleOTLPHTTPLogExporter(config); err == nil {
			t.Fatalf("accepted config %#v", config)
		}
	}
}
