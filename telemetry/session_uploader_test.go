// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/voice"
	livekit "github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

func TestSessionReportUploaderStreamsExactMultipartAndRetries(t *testing.T) {
	t.Parallel()
	audioBytes := []byte("OggS\x00production-test")
	audioPath := t.TempDir() + "/recording.ogg"
	if err := os.WriteFile(audioPath, audioBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	start := time.UnixMilli(1_734_000_100_000)
	recordingStart := start.Add(time.Second)
	itemTime := start.Add(2 * time.Second)
	report := voice.CreateSessionReport(voice.SessionReportOptions{
		JobID: "job_1", RoomID: "RM_1", Room: "support", StartedAt: start,
		Timestamp: start.Add(5 * time.Second), AudioRecordingPath: audioPath,
		AudioRecordingStartedAt: &recordingStart,
		Options: voice.ReportSessionOptions{
			Interruption: voice.DefaultInterruptionOptions(),
			Endpointing:  voice.DefaultEndpointingOptions,
			MaxToolSteps: 3, PreemptiveGeneration: voice.DefaultPreemptiveGenerationOptions,
			Recording: voice.DefaultRecordingOptions(),
		},
		ChatHistory: llm.NewChatContext(
			&llm.FunctionCall{
				ID: "item_1", CallID: "call_1", Name: "lookup", Arguments: "{}",
				CreatedAt: itemTime, Extra: map[string]any{"providerKey": "kept"},
			},
			&llm.FunctionCallOutput{
				ID: "item_2", CallID: "call_1", Name: "lookup", Output: "failed",
				IsError: true, CreatedAt: itemTime,
			},
		),
		ModelUsage: []metrics.ModelUsage{
			metrics.LLMUsage{Provider: "openai", Model: "gpt", InputTokens: 4},
		},
	})

	var logs atomic.Int32
	var recordings atomic.Int32
	var sleepMu sync.Mutex
	var sleepDelays []time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") {
			t.Errorf("missing bearer authorization")
		}
		switch request.URL.Path {
		case "/logs":
			logs.Add(1)
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read logs: %v", err)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			for _, expected := range []string{
				"session report", "chat item", "arguments", "providerKey", "severityNumber",
			} {
				if !bytes.Contains(payload, []byte(expected)) {
					t.Errorf("log payload missing %q: %s", expected, payload)
				}
			}
			writer.WriteHeader(http.StatusOK)
		case "/recordings":
			attempt := recordings.Add(1)
			if request.ContentLength <= 0 {
				t.Errorf("recording content length = %d", request.ContentLength)
			}
			if attempt == 1 {
				writer.Header().Set("Retry-After", "0")
				writer.WriteHeader(http.StatusServiceUnavailable)
				_, _ = writer.Write([]byte("retry"))
				return
			}
			verifyRecordingMultipart(t, request, report, audioBytes)
			writer.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	gate := NewUploadGate(nil)
	gate.Reset()
	uploader, err := NewSessionReportUploader(SessionReportUploaderConfig{
		AgentName:         "support-agent",
		RecordingEndpoint: server.URL + "/recordings",
		LogEndpoint:       server.URL + "/logs",
		APIKey:            agents.NewSecretString("api-key"),
		APISecret:         agents.NewSecretString("api-secret-at-least-32-bytes-long"),
		HTTPClient:        server.Client(),
		UploadGate:        gate,
		Metadata: map[string]any{
			agents.AttributeSimulationEnabled: true,
			agents.AttributeRedactionEnabled:  true,
		},
		MaxRetries: 1,
		Sleep: func(_ context.Context, delay time.Duration) error {
			sleepMu.Lock()
			sleepDelays = append(sleepDelays, delay)
			sleepMu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.Upload(context.Background(), report); err != nil {
		t.Fatal(err)
	}
	if logs.Load() != 1 || recordings.Load() != 2 {
		t.Fatalf("requests: logs=%d recordings=%d", logs.Load(), recordings.Load())
	}
	sleepMu.Lock()
	defer sleepMu.Unlock()
	if len(sleepDelays) != 1 || sleepDelays[0] != 0 {
		t.Fatalf("retry delays = %v", sleepDelays)
	}
}

func verifyRecordingMultipart(
	t *testing.T,
	request *http.Request,
	report *voice.SessionReport,
	audioWant []byte,
) {
	t.Helper()
	mediaType := request.Header.Get("Content-Type")
	const prefix = "multipart/form-data; boundary="
	if !strings.HasPrefix(mediaType, prefix) {
		t.Errorf("content type = %q", mediaType)
		return
	}
	reader := multipart.NewReader(request.Body, strings.TrimPrefix(mediaType, prefix))
	parts := make(map[string][]byte)
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("next multipart part: %v", err)
			return
		}
		payload, err := io.ReadAll(part)
		if err != nil {
			t.Errorf("read multipart part: %v", err)
			return
		}
		parts[part.FormName()] = payload
	}
	var header livekit.MetricsRecordingHeader
	if err := proto.Unmarshal(parts["header"], &header); err != nil {
		t.Errorf("decode recording header: %v", err)
		return
	}
	if header.RoomId != report.RoomID || header.JobId != report.JobID ||
		!header.Simulated || !header.RedactionEnabled ||
		header.Duration != uint64(report.Duration/time.Millisecond) {
		t.Errorf("recording header = %#v", &header)
	}
	if !bytes.Equal(parts["audio"], audioWant) {
		t.Errorf("audio = %q", parts["audio"])
	}
	chat := string(parts["chat_history"])
	for _, expected := range []string{
		"\"call_id\":\"call_1\"", "\"arguments\":\"{}\"", "\"is_error\":true", "\"providerKey\":\"kept\"",
	} {
		if !strings.Contains(chat, expected) {
			t.Errorf("chat history missing %q: %s", expected, chat)
		}
	}
}

func TestSessionReportUploaderUploadGateStopsRecording(t *testing.T) {
	t.Parallel()
	var recordings atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/logs" {
			writer.WriteHeader(http.StatusUnauthorized)
			_, _ = writer.Write([]byte("data recording is disabled by owner"))
			return
		}
		recordings.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	gate := NewUploadGate(nil)
	gate.Reset()
	uploader, err := NewSessionReportUploader(SessionReportUploaderConfig{
		RecordingEndpoint: server.URL + "/recordings", LogEndpoint: server.URL + "/logs",
		APIKey:     agents.NewSecretString("key"),
		APISecret:  agents.NewSecretString("secret-at-least-32-bytes-long"),
		HTTPClient: server.Client(), UploadGate: gate, MaxRetries: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	report := voice.CreateSessionReport(voice.SessionReportOptions{
		JobID: "job", RoomID: "room", Timestamp: time.Now(),
		Options:     voice.ReportSessionOptions{Recording: voice.DefaultRecordingOptions()},
		ChatHistory: llm.EmptyChatContext(),
	})
	if err := uploader.Upload(context.Background(), report); err != nil {
		t.Fatal(err)
	}
	if !gate.Disabled() || recordings.Load() != 0 {
		t.Fatalf("gate disabled=%v recordings=%d", gate.Disabled(), recordings.Load())
	}
}

func TestParseGoogleRetryInfoRejectsJSON(t *testing.T) {
	t.Parallel()
	if delay := parseGoogleRetryInfo([]byte("{\"retry\":\"later\"}")); delay >= 0 {
		t.Fatalf("delay = %s", delay)
	}
	if !json.Valid([]byte("{\"ok\":true}")) {
		t.Fatal("test invariant")
	}
}
