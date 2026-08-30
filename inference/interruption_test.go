// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
)

type interruptionCreateMessage struct {
	Type     string `json:"type"`
	Settings struct {
		SampleRate  int      `json:"sample_rate"`
		NumChannels int      `json:"num_channels"`
		MinFrames   int      `json:"min_frames"`
		Encoding    string   `json:"encoding"`
		Threshold   *float64 `json:"threshold"`
	} `json:"settings"`
}

func TestAdaptiveInterruptionWireEventsAndMetrics(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ready := make(chan struct{})
	serverErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/bargein" {
			serverErrors <- errors.New("unexpected bargein path: " + request.URL.Path)
			return
		}
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		var create interruptionCreateMessage
		if err := conn.ReadJSON(&create); err != nil {
			serverErrors <- err
			return
		}
		if create.Type != "session.create" || create.Settings.SampleRate != 16_000 || create.Settings.NumChannels != 1 || create.Settings.MinFrames != 2 || create.Settings.Encoding != "s16le" || create.Settings.Threshold != nil {
			serverErrors <- errors.New("invalid adaptive interruption session.create")
			return
		}
		if err := conn.WriteJSON(map[string]any{"type": "session.created", "default_threshold": .65}); err != nil {
			serverErrors <- err
			return
		}
		close(ready)
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			serverErrors <- err
			return
		}
		if messageType != websocket.BinaryMessage || len(payload) != 8+2_400*2 {
			serverErrors <- errors.New("invalid adaptive interruption audio packet")
			return
		}
		requestID := binary.LittleEndian.Uint64(payload[:8])
		if err := conn.WriteJSON(map[string]any{
			"type": "bargein_detected", "created_at": requestID,
			"probabilities": []float64{.9, .8, .2}, "prediction_duration": .012,
		}); err != nil {
			serverErrors <- err
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				serverErrors <- nil
				return
			}
		}
	}))
	defer server.Close()

	detector, err := NewAdaptiveInterruptionDetector(AdaptiveInterruptionDetectorOptions{
		BaseURL: server.URL + "/v1", Credentials: testCredentials(),
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	metricsValues := make(chan metrics.Interruption, 2)
	overlapValues := make(chan OverlappingSpeechEvent, 2)
	unsubMetrics := detector.OnMetrics(func(metric metrics.Interruption) { metricsValues <- metric })
	unsubOverlap := detector.OnOverlappingSpeech(func(event OverlappingSpeechEvent) { overlapValues <- event })
	defer unsubMetrics()
	defer unsubOverlap()
	stream, err := detector.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("adaptive interruption session did not become ready")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := stream.AgentSpeechStarted(ctx); err != nil {
		t.Fatal(err)
	}
	prefix, _ := agents.NewAudioFrame(make([]int16, 800), 16_000, 1)
	if err := stream.PushAudio(ctx, prefix); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := stream.OverlapSpeechStarted(ctx, 50*time.Millisecond, started); err != nil {
		t.Fatal(err)
	}
	interval, _ := agents.NewAudioFrame(make([]int16, 1_600), 16_000, 1)
	if err := stream.PushAudio(ctx, interval); err != nil {
		t.Fatal(err)
	}
	event, err := stream.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !event.IsInterruption || event.AgentEnded || event.NumRequests != 1 || math.Abs(event.Probability-.8) > 1e-6 || len(event.SpeechInput) != 2_400 || event.PredictionDuration != 12*time.Millisecond || event.OverlapStartedAt == nil || !event.OverlapStartedAt.Equal(started) {
		t.Fatalf("interruption event = %+v", event)
	}
	select {
	case callback := <-overlapValues:
		if !callback.IsInterruption {
			t.Fatalf("overlap callback = %+v", callback)
		}
	case <-ctx.Done():
		t.Fatal("missing overlap callback")
	}
	select {
	case metric := <-metricsValues:
		if metric.NumInterruptions != 1 || metric.NumBackchannels != 0 || metric.NumRequests != 1 || metric.PredictionDuration != 12*time.Millisecond {
			t.Fatalf("interruption metric = %+v", metric)
		}
	case <-ctx.Done():
		t.Fatal("missing interruption metric")
	}

	// Agent-ended overlap is inconclusive and must not count as a confirmed
	// backchannel.
	if err := stream.OverlapSpeechStarted(ctx, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := stream.OverlapSpeechEnded(ctx, time.Now(), true); err != nil {
		t.Fatal(err)
	}
	event, err = stream.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.IsInterruption || !event.AgentEnded || event.NumRequests != 0 {
		t.Fatalf("agent-ended event = %+v", event)
	}
	select {
	case metric := <-metricsValues:
		if metric.NumInterruptions != 0 || metric.NumBackchannels != 0 {
			t.Fatalf("agent-ended metric = %+v", metric)
		}
	case <-ctx.Done():
		t.Fatal("missing agent-ended metric")
	}

	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serverErrors:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("adaptive interruption server did not stop")
	}
}

func TestAdaptiveInterruptionOverlapEndWaitsForInflightVerdict(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	requestIDs := make(chan uint64, 1)
	release := make(chan struct{})
	serverErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		var create interruptionCreateMessage
		if err := conn.ReadJSON(&create); err != nil {
			serverErrors <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{"type": "session.created", "default_threshold": .65}); err != nil {
			serverErrors <- err
			return
		}
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			serverErrors <- err
			return
		}
		if messageType != websocket.BinaryMessage || len(payload) < 8 {
			serverErrors <- errors.New("invalid adaptive interruption audio packet")
			return
		}
		requestID := binary.LittleEndian.Uint64(payload[:8])
		requestIDs <- requestID
		<-release
		if err := conn.WriteJSON(map[string]any{
			"type": "inference_done", "created_at": requestID,
			"probabilities": []float64{.75, .6, .1}, "prediction_duration": .008,
		}); err != nil {
			serverErrors <- err
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	detector, err := NewAdaptiveInterruptionDetector(AdaptiveInterruptionDetectorOptions{
		BaseURL: server.URL, Credentials: testCredentials(),
		DetectionInterval: 10 * time.Millisecond, InferenceTimeout: time.Second,
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := detector.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := value.AgentSpeechStarted(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := value.OverlapSpeechStarted(context.Background(), 0, started); err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 160), AdaptiveInterruptionSampleRate, 1)
	if err := value.PushAudio(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestIDs:
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("adaptive interruption request was not sent")
	}
	ended := time.Now()
	if err := value.OverlapSpeechEnded(context.Background(), ended, false); err != nil {
		t.Fatal(err)
	}
	short, cancelShort := context.WithTimeout(context.Background(), 75*time.Millisecond)
	_, err = value.Recv(short)
	cancelShort()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Recv before inference_done = %v, want deadline exceeded", err)
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	event, err := value.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.IsInterruption || event.NumRequests != 1 || len(event.SpeechInput) != 160 ||
		math.Abs(event.Probability-.6) > 1e-6 || !event.DetectedAt.Equal(ended) ||
		event.OverlapStartedAt == nil || !event.OverlapStartedAt.Equal(started) {
		t.Fatalf("deferred overlap event = %+v", event)
	}
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAdaptiveInterruptionOverlapEndInflightTimeoutIsVisible(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	requestSeen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var create interruptionCreateMessage
		if conn.ReadJSON(&create) != nil {
			return
		}
		if conn.WriteJSON(map[string]any{"type": "session.created", "default_threshold": .65}) != nil {
			return
		}
		if messageType, payload, err := conn.ReadMessage(); err == nil && messageType == websocket.BinaryMessage && len(payload) >= 8 {
			requestSeen <- struct{}{}
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	detector, err := NewAdaptiveInterruptionDetector(AdaptiveInterruptionDetectorOptions{
		BaseURL: server.URL, Credentials: testCredentials(),
		DetectionInterval: 10 * time.Millisecond, InferenceTimeout: 50 * time.Millisecond,
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := detector.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := value.AgentSpeechStarted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := value.OverlapSpeechStarted(context.Background(), 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 160), AdaptiveInterruptionSampleRate, 1)
	if err := value.PushAudio(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("adaptive interruption request was not sent")
	}
	if err := value.OverlapSpeechEnded(context.Background(), time.Now(), false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = value.Recv(ctx)
	var status *agents.APIStatusError
	if !errors.As(err, &status) || status.StatusCode != http.StatusRequestTimeout || status.Retryable() {
		t.Fatalf("Recv timeout error = %v, want non-retryable status 408", err)
	}
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAdaptiveInterruptionUpdateReconnectsWithNewSettings(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	creates := make(chan interruptionCreateMessage, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var create interruptionCreateMessage
		if conn.ReadJSON(&create) != nil {
			return
		}
		creates <- create
		if conn.WriteJSON(map[string]any{"type": "session.created", "default_threshold": .6}) != nil {
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	detector, err := NewAdaptiveInterruptionDetector(AdaptiveInterruptionDetectorOptions{
		BaseURL: server.URL, Credentials: testCredentials(),
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := detector.Stream(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case first := <-creates:
		if first.Settings.MinFrames != 2 || first.Settings.Threshold != nil {
			t.Fatalf("first session settings = %+v", first.Settings)
		}
	case <-ctx.Done():
		t.Fatal("missing first interruption session")
	}
	threshold, minimum := .72, 75*time.Millisecond
	if err := detector.UpdateOptions(AdaptiveInterruptionUpdateOptions{Threshold: &threshold, MinimumInterruptionDuration: &minimum}); err != nil {
		t.Fatal(err)
	}
	select {
	case second := <-creates:
		if second.Settings.MinFrames != 3 || second.Settings.Threshold == nil || *second.Settings.Threshold != .72 {
			t.Fatalf("updated session settings = %+v", second.Settings)
		}
	case <-ctx.Done():
		t.Fatal("options update did not reconnect")
	}
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAdaptiveInterruptionRejectsSessionWithoutThreshold(t *testing.T) {
	options := resolvedInterruptionOptions{}
	state := newInterruptionActorState(resolvedInterruptionOptions{maxAudio: time.Second})
	detector := &AdaptiveInterruptionDetector{}
	stream := &InterruptionStream{detector: detector}
	err := stream.handleServerMessage(&state, options, []byte(`{"type":"session.created"}`))
	var status *agents.APIStatusError
	if !errors.As(err, &status) || status.StatusCode != 500 {
		t.Fatalf("missing-threshold error = %T %v", err, err)
	}
}

func TestAdaptiveInterruptionDefaults(t *testing.T) {
	detector, err := NewAdaptiveInterruptionDetector(AdaptiveInterruptionDetectorOptions{Credentials: testCredentials()})
	if err != nil {
		t.Fatal(err)
	}
	options := detector.options()
	if options.minDuration != 50*time.Millisecond || options.minFrames != 2 || options.maxAudio != 3*time.Second || options.prefix != time.Second || options.interval != 100*time.Millisecond || options.timeout != 700*time.Millisecond || detector.SampleRate() != 16_000 {
		t.Fatalf("adaptive interruption defaults = %+v", options)
	}
	if err := detector.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestOverlappingSpeechEventJSONUsesCrossSDKUnits(t *testing.T) {
	started := time.UnixMilli(1_700_000_000_000)
	event := OverlappingSpeechEvent{
		Type: "overlapping_speech", DetectedAt: started.Add(250 * time.Millisecond),
		IsInterruption: false, AgentEnded: true, TotalDuration: 1250 * time.Millisecond,
		PredictionDuration: 12 * time.Millisecond, DetectionDelay: 225 * time.Millisecond,
		OverlapStartedAt: &started, SpeechInput: []int16{1, 2}, Probabilities: []float32{.8},
		Probability: .8, NumRequests: 2,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	if got["detectedAt"] != float64(started.Add(250*time.Millisecond).UnixMilli()) || got["totalDurationInS"] != 1.25 || got["predictionDurationInS"] != .012 || got["detectionDelayInS"] != .225 || got["overlapStartedAt"] != float64(started.UnixMilli()) || got["agentEnded"] != true {
		t.Fatalf("overlap event JSON = %s", payload)
	}
	if _, ok := got["speechInput"]; ok {
		t.Fatalf("overlap event leaked raw PCM: %s", payload)
	}
}

func FuzzEstimateInterruptionProbability(f *testing.F) {
	f.Add([]byte{230, 204, 51}, uint8(2))
	f.Add([]byte{}, uint8(1))
	f.Fuzz(func(t *testing.T, raw []byte, frames uint8) {
		if len(raw) > 256 {
			raw = raw[:256]
		}
		n := int(frames%16) + 1
		values := make([]float32, len(raw))
		for i := range raw {
			values[i] = float32(raw[i]) / 255
		}
		original := slices.Clone(values)
		got := EstimateInterruptionProbability(values, time.Duration(n)*adaptiveInterruptionFrameDuration)
		if !slices.Equal(values, original) {
			t.Fatal("estimator mutated its input")
		}
		want := float64(0)
		if len(values) >= n {
			reference := slices.Clone(values)
			slices.SortFunc(reference, func(a, b float32) int {
				switch {
				case a > b:
					return -1
				case a < b:
					return 1
				default:
					return 0
				}
			})
			want = float64(reference[n-1])
		}
		if got != want {
			t.Fatalf("probability = %v, want %v (n=%d values=%v)", got, want, n, values)
		}
	})
}

func BenchmarkEstimateInterruptionProbability(b *testing.B) {
	probabilities := make([]float32, 120)
	for i := range probabilities {
		probabilities[i] = float32((i*37)%101) / 100
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = EstimateInterruptionProbability(probabilities, DefaultMinimumInterruptionDuration)
	}
}
