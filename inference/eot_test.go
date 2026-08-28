// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/ipc"
)

type fakeTurnTransport struct {
	mu       sync.Mutex
	attached *TurnDetectorStream
	started  chan struct{}
	release  chan error
	requests chan string
	frames   chan agents.AudioFrame
	flushes  chan FlushSentinel
	start    sync.Once
	closed   atomic.Int32
}

func TestExecutorEOTPredictorAndContextDiscovery(t *testing.T) {
	var called atomic.Int32
	executor := ipc.InferenceExecutorFunc(func(_ context.Context, method string, data any) (any, error) {
		if method != EOTInferenceMethod {
			t.Fatalf("method = %q", method)
		}
		input, ok := data.(EOTInferenceInput)
		if !ok {
			t.Fatalf("input type = %T", data)
		}
		decoded, err := base64.StdEncoding.DecodeString(input.PCM)
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded) != 6 {
			t.Fatalf("PCM byte length = %d", len(decoded))
		}
		called.Add(1)
		return map[string]any{"probability": 0.73, "inferenceDurationMs": 1.25}, nil
	})
	ctx := agents.WithInferenceExecutor(context.Background(), executor)
	provider := &sharedLocalPredictor{ctx: context.Background()}
	predictor, err := provider.get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	probability, err := predictor.PredictEndOfTurn(ctx, []int16{-1, 0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if probability != 0.73 || called.Load() != 1 {
		t.Fatalf("probability = %v, calls = %d", probability, called.Load())
	}
	if err := provider.close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func newFakeTurnTransport() *fakeTurnTransport {
	return &fakeTurnTransport{
		started: make(chan struct{}), requests: make(chan string, 8),
		frames: make(chan agents.AudioFrame, 8), flushes: make(chan FlushSentinel, 8),
	}
}

func (f *fakeTurnTransport) Attach(stream *TurnDetectorStream) {
	f.mu.Lock()
	f.attached = stream
	f.mu.Unlock()
}

func (f *fakeTurnTransport) Run(ctx context.Context) error {
	f.start.Do(func() { close(f.started) })
	if f.release != nil {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case err := <-f.release:
			return err
		}
	}
	f.mu.Lock()
	attached := f.attached
	f.mu.Unlock()
	if attached == nil {
		return errors.New("fake turn transport is not attached")
	}
	return attached.drain(ctx, f)
}

func (f *fakeTurnTransport) RunInference(ctx context.Context, id string) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case f.requests <- id:
		return nil
	}
}

func (f *fakeTurnTransport) PushFrame(ctx context.Context, frame agents.AudioFrame) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case f.frames <- frame:
		return nil
	}
}

func (f *fakeTurnTransport) Flush(ctx context.Context, sentinel FlushSentinel) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case f.flushes <- sentinel:
		return nil
	}
}

func (f *fakeTurnTransport) Close() error {
	f.closed.Add(1)
	return nil
}

type fakeLocalEOTPredictor struct {
	probability float64
	calls       atomic.Int32
	closes      atomic.Int32
}

func (p *fakeLocalEOTPredictor) PredictEndOfTurn(ctx context.Context, _ []int16) (float64, error) {
	select {
	case <-ctx.Done():
		return 0, context.Cause(ctx)
	default:
	}
	p.calls.Add(1)
	return p.probability, nil
}
func (p *fakeLocalEOTPredictor) Close(context.Context) error { p.closes.Add(1); return nil }

func TestTurnDetectorStreamPredictionLifecycle(t *testing.T) {
	detector, err := NewTurnDetector(TurnDetectorOptions{Version: TurnDetectorV1Mini})
	if err != nil {
		t.Fatal(err)
	}
	transport := newFakeTurnTransport()
	stream, err := detector.Stream(context.Background(), TurnDetectorStreamOptions{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("transport did not start")
	}

	first, err := stream.BeginPrediction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	firstID := <-transport.requests
	second, err := stream.BeginPrediction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondID := <-transport.requests
	if firstID == secondID {
		t.Fatal("prediction IDs were reused")
	}
	firstEvent, err := first.Wait(context.Background())
	if err != nil || firstEvent.EndOfTurnProbability != 0 {
		t.Fatalf("superseded prediction = %+v, %v", firstEvent, err)
	}

	stream.ResolvePrediction(firstID, .99, TurnPredictionDetails{})
	inferenceDuration, detectionDelay, backchannel := 12*time.Millisecond, 18*time.Millisecond, .81
	stream.ResolvePrediction(secondID, .73, TurnPredictionDetails{
		InferenceDuration: &inferenceDuration, DetectionDelay: &detectionDelay,
		BackchannelProbability: &backchannel,
	})
	secondEvent, err := second.Wait(context.Background())
	if err != nil || secondEvent.EndOfTurnProbability != .73 || secondEvent.BackchannelProbability == nil || *secondEvent.BackchannelProbability != .81 || secondEvent.InferenceDuration == nil || *secondEvent.InferenceDuration != inferenceDuration {
		t.Fatalf("prediction = %+v, %v", secondEvent, err)
	}

	frame, _ := agents.NewAudioFrame(make([]int16, 320), 16_000, 1)
	if err := stream.PushAudio(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-transport.frames:
		if got.SampleRate != 16_000 || got.Channels != 1 || len(got.Data) != 320 {
			t.Fatalf("transport frame = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("audio was not drained")
	}
	if err := stream.Flush(context.Background(), "turn committed"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-transport.flushes:
		if got.Reason != "turn committed" {
			t.Fatalf("flush reason = %q", got.Reason)
		}
	case <-time.After(time.Second):
		t.Fatal("flush was not drained")
	}
	if err := stream.EndInput(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := stream.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if transport.closed.Load() != 1 {
		t.Fatalf("transport Close calls = %d", transport.closed.Load())
	}
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTurnDetectorCloudFailureFallsBackAndStaysLocal(t *testing.T) {
	predictor := &fakeLocalEOTPredictor{probability: .67}
	detector, err := NewTurnDetector(TurnDetectorOptions{
		Version: TurnDetectorV1, Credentials: testCredentials(), LocalPredictor: predictor,
	})
	if err != nil {
		t.Fatal(err)
	}
	cloud := newFakeTurnTransport()
	cloud.release = make(chan error, 1)
	stream, err := detector.Stream(context.Background(), TurnDetectorStreamOptions{Transport: cloud})
	if err != nil {
		t.Fatal(err)
	}
	<-cloud.started
	prediction, err := stream.BeginPrediction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	<-cloud.requests
	cloud.release <- agents.NewAPIConnectionError("cloud unavailable", false, io.ErrUnexpectedEOF)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	event, err := prediction.Wait(ctx)
	if err != nil || event.EndOfTurnProbability != 1 {
		t.Fatalf("fallback in-flight prediction = %+v, %v", event, err)
	}
	if !stream.IsFallback() {
		t.Fatal("stream did not fall back")
	}
	if detector.Model() != string(TurnDetectorModelV1Mini) || stream.Model() != string(TurnDetectorModelV1Mini) {
		t.Fatalf("models after fallback = detector %q stream %q", detector.Model(), stream.Model())
	}
	localEvent, err := stream.Predict(ctx)
	if err != nil || localEvent.EndOfTurnProbability != .67 {
		t.Fatalf("local prediction = %+v, %v", localEvent, err)
	}
	if predictor.calls.Load() != 1 {
		t.Fatalf("local predictor calls = %d", predictor.calls.Load())
	}
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if predictor.closes.Load() != 1 {
		t.Fatalf("local predictor close calls = %d", predictor.closes.Load())
	}
}

func TestTurnDetectorClosesUnusedInjectedPredictor(t *testing.T) {
	predictor := &fakeLocalEOTPredictor{}
	detector, err := NewTurnDetector(TurnDetectorOptions{Version: TurnDetectorV1Mini, LocalPredictor: predictor})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := detector.Close(ctx); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	group.Wait()
	if predictor.closes.Load() != 1 {
		t.Fatalf("unused predictor close calls = %d", predictor.closes.Load())
	}
}

func TestTurnDetectionEventJSONUsesCrossSDKMilliseconds(t *testing.T) {
	detection, inference, backchannel := 12_500*time.Microsecond, 7*time.Millisecond, .8
	event := newTurnDetectionEvent(.6)
	event.DetectionDelay, event.InferenceDuration, event.BackchannelProbability = &detection, &inference, &backchannel
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	if got["type"] != "eot_prediction" || got["endOfTurnProbability"] != .6 || got["detectionDelay"] != 12.5 || got["inferenceDuration"] != 7.0 || got["backchannelProbability"] != .8 {
		t.Fatalf("turn event JSON = %s", payload)
	}
}

func TestTurnDetectorDefaultsAndRedactedConfig(t *testing.T) {
	detector, err := NewTurnDetector(TurnDetectorOptions{
		Version: TurnDetectorV1, BaseURL: "https://gateway.example.test/v1",
		Credentials: testCredentials(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if detector.SampleRate() != 16_000 || detector.Model() != string(TurnDetectorModelV1) {
		t.Fatalf("turn detector defaults: rate=%d model=%q", detector.SampleRate(), detector.Model())
	}
	stream, err := detector.Stream(context.Background(), TurnDetectorStreamOptions{Transport: newFakeTurnTransport()})
	if err != nil {
		t.Fatal(err)
	}
	if stream.PredictionTimeout() != time.Second {
		t.Fatalf("prediction timeout = %v", stream.PredictionTimeout())
	}
	payload, err := json.Marshal(detector)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "api-key") || strings.Contains(string(payload), "a-long-test-api-secret") {
		t.Fatalf("config leaked credentials: %s", payload)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
