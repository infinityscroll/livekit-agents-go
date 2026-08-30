// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/tts"
)

func TestInferenceTTSPooledStreamingAlignmentAndFinalFrame(t *testing.T) {
	t.Parallel()
	var connections atomic.Int32
	serverErrors := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connections.Add(1)
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		var create TTSSessionCreate
		if err := conn.ReadJSON(&create); err != nil {
			serverErrors <- err
			return
		}
		if request.URL.Path != "/v1/tts" || create.Type != "session.create" ||
			create.Model != "cartesia/sonic-3" || create.SampleRate != "16000" {
			serverErrors <- &testError{"tts session.create", create}
			return
		}
		if err := conn.WriteJSON(map[string]any{"type": "session.created", "session_id": "tts-session"}); err != nil {
			serverErrors <- err
			return
		}
		for turn := 0; turn < 2; turn++ {
			var transcript struct {
				Type       string       `json:"type"`
				Transcript string       `json:"transcript"`
				Extra      ModelOptions `json:"extra"`
			}
			if err := conn.ReadJSON(&transcript); err != nil {
				serverErrors <- err
				return
			}
			if transcript.Type != "input_transcript" || transcript.Transcript != "hello " || transcript.Extra == nil {
				serverErrors <- &testError{"input_transcript", transcript}
				return
			}
			var flush map[string]any
			if err := conn.ReadJSON(&flush); err != nil {
				serverErrors <- err
				return
			}
			if flush["type"] != "session.flush" {
				serverErrors <- &testError{"session.flush", flush}
				return
			}
			messages := []map[string]any{
				{"type": "output_alignment", "words": []map[string]any{{"word": "hello", "start": 0.0, "end": 0.25}}},
				{"type": "output_audio", "session_id": "tts-session", "audio": base64.StdEncoding.EncodeToString([]byte{1, 0, 2, 0, 3, 0, 4, 0})},
				{"type": "done", "session_id": "tts-session"},
			}
			for _, message := range messages {
				if err := conn.WriteJSON(message); err != nil {
					serverErrors <- err
					return
				}
			}
		}
		serverErrors <- nil
	}))
	defer server.Close()

	client, err := NewTTS(TTSOptions{
		Model: "cartesia/sonic-3", Voice: "voice-a", BaseURL: server.URL + "/v1",
		Credentials: testCredentials(), ModelOptions: ModelOptions{"add_timestamps": true},
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !client.Capabilities().AlignedTranscript {
		t.Fatal("aligned transcript capability was not enabled")
	}
	for turn := 0; turn < 2; turn++ {
		streamValue, err := client.Stream(context.Background(), tts.StreamOptions{})
		if err != nil {
			t.Fatal(err)
		}
		stream := streamValue.(*SynthesizeStream)
		if err := stream.PushText(context.Background(), "hello"); err != nil {
			t.Fatal(err)
		}
		if err := stream.EndInput(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		audio, err := stream.Recv(ctx)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if !audio.Final || audio.RequestID != "tts-session" || len(audio.Frame.Data) != 4 {
			cancel()
			t.Fatalf("audio = %+v", audio)
		}
		if len(audio.TimedTranscripts) != 1 || audio.TimedTranscripts[0].Text != "hello " ||
			audio.TimedTranscripts[0].EndTime == nil || *audio.TimedTranscripts[0].EndTime != 250*time.Millisecond {
			cancel()
			t.Fatalf("alignment = %+v", audio.TimedTranscripts)
		}
		if _, err := stream.Recv(ctx); !errors.Is(err, io.EOF) {
			cancel()
			t.Fatalf("terminal error = %v", err)
		}
		if err := stream.Wait(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("websocket connections = %d, want pooled reuse with 1", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
}

func TestInferenceTTSCancelUnblocksAllPumps(t *testing.T) {
	t.Parallel()
	connected := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var create TTSSessionCreate
		if conn.ReadJSON(&create) != nil {
			return
		}
		close(connected)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	client, err := NewTTS(TTSOptions{Model: "cartesia/sonic-3", BaseURL: server.URL, Credentials: testCredentials()})
	if err != nil {
		t.Fatal(err)
	}
	streamValue, err := client.Stream(context.Background(), tts.StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamValue.(*SynthesizeStream)
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("websocket did not connect")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stream.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
