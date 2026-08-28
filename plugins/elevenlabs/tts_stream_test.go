// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/tts"
)

func TestMultiContextTTSWireAlignmentAndGracefulClose(t *testing.T) {
	serverErr := make(chan error, 1)
	closedSocket := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/text-to-speech/voice-1/multi-stream-input" {
			serverErr <- errors.New("unexpected path: " + r.URL.Path)
			return
		}
		query := r.URL.Query()
		if query.Get("model_id") != string(ElevenTurboV25) || query.Get("output_format") != string(PCM22050) || query.Get("auto_mode") != "true" || query.Get("sync_alignment") != "true" {
			serverErr <- errors.New("unexpected query: " + r.URL.RawQuery)
			return
		}
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		var contextID string
		seenContent := false
		seenFinalFlush := false
		seenCloseContext := false
		for !seenCloseContext {
			var packet map[string]any
			if err := conn.ReadJSON(&packet); err != nil {
				serverErr <- err
				return
			}
			if id, ok := packet["context_id"].(string); ok {
				contextID = id
			}
			text, _ := packet["text"].(string)
			if text == " " {
				if _, ok := packet["voice_settings"]; !ok {
					serverErr <- errors.New("initialization omitted voice_settings")
					return
				}
			} else if text == "Hello world. " {
				seenContent = true
				if packet["flush"] != true {
					serverErr <- errors.New("auto mode content did not flush")
					return
				}
			} else if text == "" && packet["flush"] == true {
				seenFinalFlush = true
			}
			if packet["close_context"] == true {
				seenCloseContext = true
			}
		}
		if contextID == "" || !seenContent || !seenFinalFlush {
			serverErr <- errors.New("missing context content/finalization")
			return
		}
		text := "Hello world. "
		chars := make([]string, 0, len([]rune(text)))
		starts := make([]int, 0, len([]rune(text)))
		durations := make([]int, 0, len([]rune(text)))
		for i, char := range []rune(text) {
			chars = append(chars, string(char))
			starts = append(starts, 100+i*10)
			durations = append(durations, 10)
		}
		pcm := []byte{1, 0, 2, 0, 3, 0, 4, 0}
		if err := conn.WriteJSON(map[string]any{
			"context_id": contextID, "audio": base64.StdEncoding.EncodeToString(pcm), "is_final": true,
			"normalized_alignment": map[string]any{"chars": chars, "char_start_times_ms": starts, "char_durations_ms": durations},
		}); err != nil {
			serverErr <- err
			return
		}
		var closePacket map[string]any
		if err := conn.ReadJSON(&closePacket); err != nil {
			serverErr <- err
			return
		}
		if closePacket["close_socket"] != true {
			serverErr <- errors.New("missing provider close_socket")
			return
		}
		close(closedSocket)
		serverErr <- nil
	}))
	defer server.Close()

	client, err := NewTTS(TTSOptions{APIKey: "secret", BaseURL: server.URL + "/v1", VoiceID: "voice-1"})
	if err != nil {
		t.Fatal(err)
	}
	metricCh := make(chan metrics.TTS, 1)
	client.OnMetrics(func(metric metrics.TTS) { metricCh <- metric })
	value, err := client.Stream(t.Context(), tts.StreamOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	stream := value.(*SynthesizeStream)
	if err := stream.PushText(t.Context(), "Hello world."); err != nil {
		t.Fatal(err)
	}
	if err := stream.EndInput(); err != nil {
		t.Fatal(err)
	}
	result, err := stream.Recv(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Final || len(result.Frame.Data) != 4 || result.Frame.Data[3] != 4 || len(result.TimedTranscripts) != 2 {
		t.Fatalf("unexpected synthesized result: %+v", result)
	}
	if result.TimedTranscripts[0].Text != "Hello " || result.TimedTranscripts[1].Text != "world. " {
		t.Fatalf("unexpected transcript alignment: %+v", result.TimedTranscripts)
	}
	if result.TimedTranscripts[0].StartTime == nil || *result.TimedTranscripts[0].StartTime != 0 {
		t.Fatalf("alignment leading silence was not normalized: %+v", result.TimedTranscripts)
	}
	if _, err := stream.Recv(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	select {
	case metric := <-metricCh:
		if !metric.Streamed || metric.CharactersCount != int64(len([]rune("Hello world."))) || metric.AudioDuration != result.Frame.Duration() {
			t.Fatalf("unexpected stream metric: %+v", metric)
		}
	case <-time.After(time.Second):
		t.Fatal("missing stream metric")
	}
	if err := client.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closedSocket:
	case <-time.After(time.Second):
		t.Fatal("provider socket was not gracefully closed")
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTTSOptionRolloverKeepsOldContextAlive(t *testing.T) {
	connected := make(chan string, 2)
	serverErrors := make(chan error, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		voiceID := parts[len(parts)-2]
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		connected <- voiceID
		var contextID string
		for {
			var packet map[string]any
			if err := conn.ReadJSON(&packet); err != nil {
				serverErrors <- err
				return
			}
			if id, ok := packet["context_id"].(string); ok {
				contextID = id
			}
			if packet["close_context"] == true {
				break
			}
		}
		pcm := base64.StdEncoding.EncodeToString([]byte{1, 0})
		if err := conn.WriteJSON(map[string]any{"contextId": contextID, "audio": pcm, "isFinal": true}); err != nil {
			serverErrors <- err
			return
		}
		for {
			var packet map[string]any
			if err := conn.ReadJSON(&packet); err != nil {
				return
			}
			if packet["close_socket"] == true {
				serverErrors <- nil
				return
			}
		}
	}))
	defer server.Close()
	client, err := NewTTS(TTSOptions{APIKey: "x", BaseURL: server.URL, VoiceID: "voice-old"})
	if err != nil {
		t.Fatal(err)
	}
	firstValue, err := client.Stream(t.Context(), tts.StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	first := firstValue.(*SynthesizeStream)
	if got := <-connected; got != "voice-old" {
		t.Fatalf("first voice=%s", got)
	}
	newVoice := "voice-new"
	if err := client.UpdateOptions(t.Context(), TTSUpdateOptions{VoiceID: &newVoice}); err != nil {
		t.Fatal(err)
	}
	secondValue, err := client.Stream(t.Context(), tts.StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second := secondValue.(*SynthesizeStream)
	if got := <-connected; got != "voice-new" {
		t.Fatalf("second voice=%s", got)
	}
	for _, stream := range []*SynthesizeStream{first, second} {
		if err := stream.PushText(t.Context(), "Done."); err != nil {
			t.Fatal(err)
		}
		_ = stream.EndInput()
		result, err := stream.Recv(t.Context())
		if err != nil || !result.Final {
			t.Fatalf("rollover stream result=%+v err=%v", result, err)
		}
	}
	if err := client.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case err := <-serverErrors:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("connection did not retire")
		}
	}
}

func TestTTSContextSemaphoreAndWriterBackpressureAreCancellable(t *testing.T) {
	connectionCtx, cancelConnection := context.WithCancelCause(context.Background())
	defer cancelConnection(io.EOF)
	connection := &ttsConnection{
		ctx: connectionCtx, done: make(chan struct{}), contexts: make(map[string]*ttsContextState),
		slots: make(chan struct{}, maxProviderContextCount), commands: make(chan ttsCommand, 1),
	}
	states := make([]*ttsContextState, maxProviderContextCount)
	for i := range states {
		states[i] = &ttsContextState{id: agents.ShortUUID("ctx_"), ctx: t.Context(), failed: make(chan struct{})}
		if err := connection.register(t.Context(), states[i]); err != nil {
			t.Fatal(err)
		}
	}
	sixth := &ttsContextState{id: "sixth", ctx: t.Context(), failed: make(chan struct{})}
	blockedCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := connection.register(blockedCtx, sixth); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sixth context did not obey cancellation: %v", err)
	}
	connection.releaseContext(states[0], false)
	if err := connection.register(t.Context(), sixth); err != nil {
		t.Fatalf("sixth context was not admitted after release: %v", err)
	}

	connection.commands <- ttsCommand{}
	writeCtx, cancelWrite := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancelWrite()
	err := connection.sendCommand(writeCtx, ttsCommand{ack: make(chan error, 1)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded writer did not obey cancellation: %v", err)
	}
}

func TestV3WebSocketRejectedBeforeDial(t *testing.T) {
	var requested bool
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requested = true }))
	defer server.Close()
	client, err := NewTTS(TTSOptions{APIKey: "x", BaseURL: server.URL, Model: ElevenV3})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Stream(t.Context(), tts.StreamOptions{})
	var unsupported *UnsupportedTransportError
	if !errors.As(err, &unsupported) || requested {
		t.Fatalf("v3 websocket must fail before dial: %T %v requested=%v", err, err, requested)
	}
	_ = client.Close(t.Context())
}

func TestTTSConnectionLevelProviderErrorFailsContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var packet map[string]any
		if err := conn.ReadJSON(&packet); err != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"error": "provider unavailable"})
	}))
	defer server.Close()
	client, err := NewTTS(TTSOptions{APIKey: "x", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	value, err := client.Stream(t.Context(), tts.StreamOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: -1}})
	if err != nil {
		t.Fatal(err)
	}
	stream := value.(*SynthesizeStream)
	if err := stream.PushText(t.Context(), "hello."); err != nil {
		t.Fatal(err)
	}
	recvCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = stream.Recv(recvCtx)
	var provider *ProviderError
	if !errors.As(err, &provider) {
		t.Fatalf("expected connection-level ProviderError, got %T: %v", err, err)
	}
	_ = client.Close(t.Context())
}
