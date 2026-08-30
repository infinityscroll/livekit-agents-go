// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/inference"
	"github.com/infinityscroll/livekit-agents-go/ipc"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/stt"
	"github.com/infinityscroll/livekit-agents-go/vad"
)

type fixedEOTPredictor struct {
	probability float64
	calls       atomic.Int64
}

func (p *fixedEOTPredictor) PredictEndOfTurn(context.Context, []int16) (float64, error) {
	p.calls.Add(1)
	return p.probability, nil
}
func (*fixedEOTPredictor) Close(context.Context) error { return nil }

func TestTurnHandlingAcceptsConcreteStreamingDetector(t *testing.T) {
	predictor := &fixedEOTPredictor{probability: 0.9}
	detector, err := inference.NewTurnDetector(inference.TurnDetectorOptions{
		Version:        inference.TurnDetectorV1Mini,
		LocalPredictor: predictor,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = detector.Close(context.Background()) })
	options := TurnHandlingOptions{
		TurnDetection:        UseTurnDetector(detector),
		Endpointing:          StreamingEndpointingOptions,
		Interruption:         DefaultInterruptionOptions(),
		PreemptiveGeneration: DefaultPreemptiveGenerationOptions,
	}
	if err := options.Validate(); err != nil {
		t.Fatal(err)
	}
	selected, ok := options.TurnDetection.Value()
	if !ok {
		t.Fatal("streaming detector override was not concrete")
	}
	mode, got, err := turnDetectionSelection(selected)
	if err != nil {
		t.Fatal(err)
	}
	if mode != TurnDetectionVAD || got != detector {
		t.Fatalf("selection = (%q, %p), want VAD + %p", mode, got, detector)
	}
}

func TestAgentSessionDefaultTurnDetectorAndVADTriState(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if session.models.vad == nil || !session.models.vadDefault {
		t.Fatal("omitted VAD did not provision a lazy default inference VAD")
	}
	selected, ok := session.opts.turn.TurnDetection.Value()
	if !ok {
		t.Fatal("omitted turn detection did not provision a default detector")
	}
	_, detector, err := turnDetectionSelection(selected)
	if err != nil || detector == nil {
		t.Fatalf("default turn detector = %v, %v", detector, err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	disabled, err := NewAgentSession(AgentSessionOptions[struct{}]{
		VADSelection: disableVAD(),
		TurnHandling: &TurnHandlingOptions{
			TurnDetection:        disableTurnDetection(),
			Endpointing:          DefaultEndpointingOptions,
			Interruption:         DefaultInterruptionOptions(),
			PreemptiveGeneration: DefaultPreemptiveGenerationOptions,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if disabled.models.vad != nil || !disabled.opts.turn.TurnDetection.IsDisabled() {
		t.Fatal("explicit VAD/turn-detection disable was not preserved")
	}
	if err := disabled.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultTurnDetectorPreservesParentInferenceExecutor(t *testing.T) {
	called := make(chan string, 1)
	executor := ipc.InferenceExecutorFunc(func(_ context.Context, method string, _ any) (any, error) {
		called <- method
		return map[string]any{"probability": 0.82}, nil
	})
	parent := agents.WithInferenceExecutor(context.Background(), executor)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{ParentContext: parent, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	selected, _ := session.opts.turn.TurnDetection.Value()
	_, detector, err := turnDetectionSelection(selected)
	if err != nil || detector == nil {
		t.Fatalf("default detector = %v, %v", detector, err)
	}
	detectionStream, err := detector.Stream(session.ctx)
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame([]int16{1, 2, 3}, 16_000, 1)
	if err := detectionStream.PushAudio(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	prediction, err := detectionStream.BeginPrediction(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prediction.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := detectionStream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case method := <-called:
		if method != inference.EOTInferenceMethod {
			t.Fatalf("inference method = %q", method)
		}
	case <-time.After(time.Second):
		t.Fatal("default turn detector did not discover the parent inference executor")
	}
}

func TestStartContextPropagatesExecutorToDefaultTurnDetectorAndVAD(t *testing.T) {
	calls := make(chan string, 16)
	executor := ipc.InferenceExecutorFunc(func(_ context.Context, method string, input any) (any, error) {
		switch method {
		case inference.EOTInferenceMethod:
			calls <- method
			return inference.EOTInferenceOutput{Probability: 0.9}, nil
		case inference.VADInferenceMethod:
			request, ok := input.(inference.VADInferenceInput)
			if !ok {
				return nil, fmt.Errorf("VAD input type = %T", input)
			}
			calls <- method + ":" + string(request.Operation)
			switch request.Operation {
			case inference.VADOperationInit:
				return inference.VADInferenceOutput{WindowSamples: 512}, nil
			case inference.VADOperationPredict:
				return inference.VADInferenceOutput{Probability: 0.1}, nil
			default:
				return nil, nil
			}
		default:
			return nil, &ipc.UnknownMethodError{Method: method}
		}
	})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	input := NewBaseAudioInput(t.Context(), 4, nil)
	source := stream.NewChannel[agents.AudioFrame](4)
	if _, err := input.Add(source); err != nil {
		t.Fatal(err)
	}
	session.Input().SetAudio(input)
	startCtx := agents.WithInferenceExecutor(t.Context(), executor)
	if err := session.Start(startCtx, MustAgent(AgentOptions[struct{}]{ID: "job_executor_agent"})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = session.Close(context.Background())
		_ = input.Close()
	})

	session.mu.RLock()
	activity := session.activity
	session.mu.RUnlock()
	activity.mu.RLock()
	recognition := activity.recognition
	activity.mu.RUnlock()
	if recognition == nil {
		t.Fatal("default recognition pipeline was not installed")
	}
	recognition.mu.Lock()
	turnStream := recognition.turnStream
	recognition.mu.Unlock()
	if turnStream == nil {
		t.Fatal("default turn-detector stream was not installed")
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 512), 16_000, 1)
	if err := source.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}

	wantVAD := map[string]bool{
		inference.VADInferenceMethod + ":" + string(inference.VADOperationInit):    false,
		inference.VADInferenceMethod + ":" + string(inference.VADOperationPredict): false,
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for remaining := len(wantVAD); remaining > 0; {
		select {
		case call := <-calls:
			seen, tracked := wantVAD[call]
			if tracked && !seen {
				wantVAD[call] = true
				remaining--
			}
		case <-deadline.C:
			t.Fatalf("Start context VAD executor calls = %v", wantVAD)
		}
	}
	// The VAD call also proves the recognition pump is live, avoiding a race
	// with the EOT transport goroutine's one-time lazy backend discovery.
	prediction, err := turnStream.BeginPrediction(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prediction.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case call := <-calls:
			if call == inference.EOTInferenceMethod {
				return
			}
		case <-deadline.C:
			t.Fatal("Start context did not reach the default EOT executor")
		}
	}
}

func TestAudioRecognitionStreamingEOTPredictionControlsEndpointing(t *testing.T) {
	predictor := &fixedEOTPredictor{probability: 0.01}
	detector, err := inference.NewTurnDetector(inference.TurnDetectorOptions{
		Version:        inference.TurnDetectorV1Mini,
		LocalPredictor: predictor,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = detector.Close(context.Background()) })
	vadProvider := newSessionTestVAD()
	sttOutput := stream.NewChannel[STTNodeItem](8)
	audio := stream.NewChannel[agents.AudioFrame](4)
	predictions := make(chan EOTPredictionEvent, 1)
	commits := make(chan RecognizedTurn, 1)
	minDelay, maxDelay := 5*time.Millisecond, 80*time.Millisecond
	started := time.Now()
	recognition, err := NewAudioRecognition(t.Context(), AudioRecognitionOptions{
		VAD: vadProvider, TurnDetector: detector, TurnDetection: TurnDetectionVAD,
		Endpointing: &FixedEndpointing{minDelay: minDelay, maxDelay: maxDelay},
		STTNode: func(context.Context, stream.Reader[agents.AudioFrame]) (stream.Reader[STTNodeItem], error) {
			return sttOutput, nil
		},
		Callbacks: AudioRecognitionCallbacks{
			OnCommit: func(turn RecognizedTurn) { commits <- turn },
			OnEOTPrediction: func(event inference.TurnDetectionEvent, threshold float64, delay time.Duration) {
				predictions <- EOTPredictionEvent{Probability: event.EndOfTurnProbability, Threshold: threshold, Delay: delay}
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := recognition.Start(audio); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recognition.Close(context.Background()) })
	frame, _ := agents.NewAudioFrame(make([]int16, 320), 16_000, 1)
	if err := audio.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	vadProvider.emit(t, vad.Event{Type: vad.StartOfSpeech, Timestamp: started})
	if err := sttOutput.Send(t.Context(), STTEventItem(stt.SpeechEvent{
		Type:         stt.FinalTranscript,
		Alternatives: []stt.SpeechData{{Text: "a difficult unfinished thought", Language: agents.LanguageCode("en")}},
	})); err != nil {
		t.Fatal(err)
	}
	ended := time.Now()
	vadProvider.emit(t, vad.Event{Type: vad.EndOfSpeech, Timestamp: ended})

	select {
	case event := <-predictions:
		if event.Threshold <= 0 || event.Probability != predictor.probability {
			t.Fatalf("prediction = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("streaming EOT prediction was not emitted")
	}
	turn := receiveTurn(t, commits)
	if turn.Transcript != "a difficult unfinished thought" {
		t.Fatalf("turn transcript = %q", turn.Transcript)
	}
	if elapsed := time.Since(ended); elapsed+10*time.Millisecond < maxDelay {
		t.Fatalf("low-probability EOT committed after %s, before max delay %s", elapsed, maxDelay)
	}
	if predictor.calls.Load() == 0 {
		t.Fatal("local EOT predictor was never called")
	}
}

func TestStreamingTurnDetectorRequiresVAD(t *testing.T) {
	detector, err := inference.NewTurnDetector(inference.TurnDetectorOptions{
		Version:        inference.TurnDetectorV1Mini,
		LocalPredictor: &fixedEOTPredictor{probability: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = detector.Close(context.Background()) })
	_, err = NewAudioRecognition(t.Context(), AudioRecognitionOptions{TurnDetector: detector})
	if err == nil {
		t.Fatal("streaming detector without VAD was accepted")
	}
}

func TestAdaptiveCompatibilityMatrix(t *testing.T) {
	provider := newSessionTestStreamingSTT()
	provider.UpdateCapabilities(func(capabilities *stt.Capabilities) {
		*capabilities = stt.Capabilities{Streaming: true, AlignedTranscript: stt.AlignedTranscriptWord}
	})
	models := activityModels{stt: provider, vad: newSessionTestVAD()}
	if !adaptiveCompatible(models, TurnDetectionVAD) {
		t.Fatal("aligned streaming STT + VAD should enable adaptive interruption")
	}
	provider.UpdateCapabilities(func(capabilities *stt.Capabilities) {
		*capabilities = stt.Capabilities{Streaming: true}
	})
	if adaptiveCompatible(models, TurnDetectionVAD) {
		t.Fatal("unaligned STT should not enable adaptive interruption")
	}
	if adaptiveCompatible(activityModels{vad: models.vad}, TurnDetectionVAD) {
		t.Fatal("ordinary LLM path without STT should not enable adaptive interruption")
	}
	if adaptiveCompatible(models, TurnDetectionManual) {
		t.Fatal("manual turn handling should not enable adaptive interruption")
	}
}

func TestAudioRecognitionAdaptiveInterruptionGatesTranscriptAndCommit(t *testing.T) {
	for _, test := range []struct {
		name         string
		interruption bool
		fallback     bool
	}{
		{name: "interruption", interruption: true},
		{name: "backchannel"},
		{name: "unrecoverable fallback", fallback: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			ready, packet, release, responded := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseServer()
			serverErr := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				conn, err := upgrader.Upgrade(writer, request, nil)
				if err != nil {
					serverErr <- err
					return
				}
				defer conn.Close()
				var create map[string]any
				if err := conn.ReadJSON(&create); err != nil {
					serverErr <- err
					return
				}
				if err := conn.WriteJSON(map[string]any{"type": "session.created", "default_threshold": .65}); err != nil {
					serverErr <- err
					return
				}
				close(ready)
				messageType, payload, err := conn.ReadMessage()
				if err != nil {
					serverErr <- err
					return
				}
				if messageType != websocket.BinaryMessage || len(payload) < 10 {
					serverErr <- fmt.Errorf("adaptive audio packet type=%d bytes=%d", messageType, len(payload))
					return
				}
				requestID := binary.LittleEndian.Uint64(payload[:8])
				close(packet)
				<-release
				message := map[string]any{
					"type": "inference_done", "created_at": requestID,
					"probabilities": []float64{.9, .8}, "prediction_duration": .001,
					"is_bargein": test.interruption,
				}
				if test.interruption {
					message["type"] = "bargein_detected"
				}
				if err := conn.WriteJSON(message); err != nil {
					serverErr <- err
					return
				}
				close(responded)
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			}))
			defer server.Close()

			detector, err := inference.NewAdaptiveInterruptionDetector(inference.AdaptiveInterruptionDetectorOptions{
				BaseURL: server.URL + "/v1",
				Credentials: inference.Credentials{
					APIKey: agents.NewSecretString("api-key"), APISecret: agents.NewSecretString("a-long-test-api-secret"),
				},
				ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: time.Second},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer detector.Close(context.Background())
			vadProvider := newSessionTestVAD()
			sttOutput := stream.NewChannel[STTNodeItem](8)
			audio := stream.NewChannel[agents.AudioFrame](8)
			started := make(chan struct{}, 1)
			finals := make(chan stt.SpeechEvent, 1)
			commits := make(chan RecognizedTurn, 1)
			interruptions := make(chan struct{}, 1)
			backchannels := make(chan struct{}, 1)
			overlaps := make(chan inference.OverlappingSpeechEvent, 2)
			recognition, err := NewAudioRecognition(t.Context(), AudioRecognitionOptions{
				VAD: vadProvider, InterruptionDetector: detector, TurnDetection: TurnDetectionVAD,
				Endpointing: &FixedEndpointing{minDelay: 5 * time.Millisecond, maxDelay: 20 * time.Millisecond},
				STTNode: func(context.Context, stream.Reader[agents.AudioFrame]) (stream.Reader[STTNodeItem], error) {
					return sttOutput, nil
				},
				Callbacks: AudioRecognitionCallbacks{
					OnStartOfSpeech:   func(time.Time) { started <- struct{}{} },
					OnFinalTranscript: func(event stt.SpeechEvent) { finals <- event },
					OnCommit:          func(turn RecognizedTurn) { commits <- turn },
					OnInterruption:    func(time.Duration, int) { interruptions <- struct{}{} },
					OnBackchannel:     func(RecognizedTurn) { backchannels <- struct{}{} },
					OnOverlappingSpeech: func(event inference.OverlappingSpeechEvent) {
						overlaps <- event
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := recognition.Start(audio); err != nil {
				t.Fatal(err)
			}
			defer recognition.Close(context.Background())
			waitClosed(t, ready, "adaptive websocket setup")
			if err := recognition.AgentSpeechStarted(t.Context(), time.Now()); err != nil {
				t.Fatal(err)
			}
			prefix, _ := agents.NewAudioFrame(make([]int16, 800), 16_000, 1)
			if err := audio.Send(t.Context(), prefix); err != nil {
				t.Fatal(err)
			}
			speechStarted := time.Now()
			vadProvider.emit(t, vad.Event{Type: vad.StartOfSpeech, Timestamp: speechStarted})
			waitClosed(t, started, "adaptive overlap start")
			if err := sttOutput.Send(t.Context(), STTEventItem(stt.SpeechEvent{
				Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{{Text: "held overlap", Language: "en"}},
			})); err != nil {
				t.Fatal(err)
			}
			interval, _ := agents.NewAudioFrame(make([]int16, 1_600), 16_000, 1)
			if err := audio.Send(t.Context(), interval); err != nil {
				t.Fatal(err)
			}
			waitClosed(t, packet, "adaptive inference packet")
			select {
			case <-finals:
				t.Fatal("aligned transcript escaped before the adaptive verdict")
			case <-time.After(15 * time.Millisecond):
			}
			if test.fallback {
				// Force the endpoint commit while the verdict is pending, then
				// emulate the activity's unrecoverable-error fallback.
				if err := recognition.sendControl(t.Context(), recognitionSignal{kind: recognitionManualCommit}); err != nil {
					t.Fatal(err)
				}
				if err := recognition.DisableAdaptiveInterruption(t.Context()); err != nil {
					t.Fatal(err)
				}
				releaseServer()
				select {
				case event := <-finals:
					alternative, _ := firstAlternative(event)
					if alternative.Text != "held overlap" {
						t.Fatalf("fallback transcript = %q", alternative.Text)
					}
				case <-time.After(time.Second):
					t.Fatal("adaptive fallback discarded the held transcript")
				}
				if turn := receiveTurn(t, commits); turn.Transcript != "held overlap" {
					t.Fatalf("adaptive fallback turn = %+v", turn)
				}
				return
			}
			releaseServer()
			waitClosed(t, responded, "adaptive verdict")
			if test.interruption {
				select {
				case event := <-overlaps:
					if !event.IsInterruption {
						t.Fatalf("adaptive verdict = %+v", event)
					}
				case <-time.After(time.Second):
					t.Fatal("adaptive interruption verdict did not reach recognition")
				}
				vadProvider.emit(t, vad.Event{Type: vad.EndOfSpeech, Timestamp: time.Now()})
			} else {
				// inference_done is cached until overlap end, unlike the terminal
				// bargein_detected message.
				time.Sleep(5 * time.Millisecond)
				vadProvider.emit(t, vad.Event{Type: vad.EndOfSpeech, Timestamp: time.Now()})
				select {
				case event := <-overlaps:
					if event.IsInterruption {
						t.Fatalf("adaptive verdict = %+v", event)
					}
				case <-time.After(time.Second):
					t.Fatal("adaptive backchannel verdict did not reach recognition")
				}
			}

			if test.interruption {
				waitClosed(t, interruptions, "adaptive interruption callback")
				select {
				case event := <-finals:
					alternative, _ := firstAlternative(event)
					if alternative.Text != "held overlap" {
						t.Fatalf("released transcript = %q", alternative.Text)
					}
				case <-time.After(time.Second):
					t.Fatal("interruption verdict did not release held transcript")
				}
				if turn := receiveTurn(t, commits); turn.Transcript != "held overlap" {
					t.Fatalf("committed adaptive turn = %+v", turn)
				}
			} else {
				waitClosed(t, backchannels, "adaptive backchannel callback")
				select {
				case event := <-finals:
					t.Fatalf("backchannel transcript was published: %+v", event)
				case turn := <-commits:
					t.Fatalf("backchannel was committed: %+v", turn)
				case <-time.After(30 * time.Millisecond):
				}
			}
			select {
			case err := <-serverErr:
				t.Fatal(err)
			default:
			}
		})
	}
}
