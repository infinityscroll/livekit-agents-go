// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stt"
)

func amdDuration(value time.Duration) *time.Duration { return &value }
func amdBool(value bool) *bool                       { return &value }

type amdTestLLM struct {
	*llm.Base
	chat       func(context.Context, llm.ChatOptions) (llm.LLMStream, error)
	closeCount atomic.Int32
}

func newAMDTestLLM(model string, chat func(context.Context, llm.ChatOptions) (llm.LLMStream, error)) *amdTestLLM {
	return &amdTestLLM{Base: llm.NewBase("amd-test", "test", model), chat: chat}
}

func amdJSONTestLLM(category AMDCategory, reason string) *amdTestLLM {
	payload, _ := json.Marshal(map[string]any{"category": category, "reason": reason})
	return newAMDTestLLM("openai/gpt-4.1-mini", func(_ context.Context, options llm.ChatOptions) (llm.LLMStream, error) {
		return newAMDSliceStream(options, llm.ChatChunk{ID: "json", Delta: &llm.ChoiceDelta{Role: llm.RoleAssistant, Content: string(payload)}}), nil
	})
}

func (m *amdTestLLM) Chat(ctx context.Context, options llm.ChatOptions) (llm.LLMStream, error) {
	if m.chat == nil {
		return nil, errors.New("test LLM chat is not configured")
	}
	return m.chat(ctx, options)
}
func (m *amdTestLLM) Close(ctx context.Context) error {
	m.closeCount.Add(1)
	return m.Base.Close(ctx)
}

type amdSliceStream struct {
	mu     sync.Mutex
	chunks []llm.ChatChunk
	index  int
	closed bool
	chat   *llm.ChatContext
	tools  *llm.Context
}

func newAMDSliceStream(options llm.ChatOptions, chunks ...llm.ChatChunk) *amdSliceStream {
	chat := options.ChatContext
	if chat == nil {
		chat = llm.EmptyChatContext()
	}
	return &amdSliceStream{chunks: chunks, chat: chat.Copy(llm.CopyOptions{}), tools: options.ToolContext}
}
func (s *amdSliceStream) Recv(context.Context) (llm.ChatChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.index >= len(s.chunks) {
		return llm.ChatChunk{}, io.EOF
	}
	chunk := s.chunks[s.index]
	s.index++
	return chunk, nil
}
func (s *amdSliceStream) Collect(ctx context.Context) (llm.CollectedResponse, error) {
	var result llm.CollectedResponse
	var text strings.Builder
	for {
		chunk, err := s.Recv(ctx)
		if errors.Is(err, io.EOF) {
			result.Text = text.String()
			return result, nil
		}
		if err != nil {
			return result, err
		}
		if chunk.Delta != nil {
			text.WriteString(chunk.Delta.Content)
			result.ToolCalls = append(result.ToolCalls, chunk.Delta.ToolCalls...)
		}
	}
}
func (s *amdSliceStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
func (s *amdSliceStream) ChatContext() *llm.ChatContext { return s.chat.Copy(llm.CopyOptions{}) }
func (s *amdSliceStream) ToolContext() *llm.Context     { return s.tools }

type amdBlockingStream struct {
	closed chan struct{}
	once   sync.Once
	chat   *llm.ChatContext
	tools  *llm.Context
}

func newAMDBlockingStream(options llm.ChatOptions) *amdBlockingStream {
	return &amdBlockingStream{closed: make(chan struct{}), chat: options.ChatContext, tools: options.ToolContext}
}
func (s *amdBlockingStream) Recv(ctx context.Context) (llm.ChatChunk, error) {
	select {
	case <-ctx.Done():
		return llm.ChatChunk{}, context.Cause(ctx)
	case <-s.closed:
		return llm.ChatChunk{}, io.EOF
	}
}
func (s *amdBlockingStream) Collect(ctx context.Context) (llm.CollectedResponse, error) {
	_, err := s.Recv(ctx)
	return llm.CollectedResponse{}, err
}
func (s *amdBlockingStream) Close() error                  { s.once.Do(func() { close(s.closed) }); return nil }
func (s *amdBlockingStream) ChatContext() *llm.ChatContext { return s.chat }
func (s *amdBlockingStream) ToolContext() *llm.Context     { return s.tools }

type amdTestSession struct {
	model       llm.LLM
	bus         *EventBus
	pause       atomic.Int32
	resume      atomic.Int32
	interrupt   atomic.Int32
	published   atomic.Int32
	predictions chan AMDPredictionEvent
	boundMu     sync.Mutex
	bound       *AMD
}

func newAMDTestSession(model llm.LLM, events bool) (*amdTestSession, AMDSessionAdapter) {
	session := &amdTestSession{model: model, predictions: make(chan AMDPredictionEvent, 8)}
	if events {
		session.bus = NewEventBus(context.Background(), EventBusOptions{QueueCapacity: 32})
	}
	adapter := AMDSessionAdapter{
		PauseReplyAuthorization:  func() error { session.pause.Add(1); return nil },
		ResumeReplyAuthorization: func() error { session.resume.Add(1); return nil },
		CurrentLLM:               func() llm.LLM { return session.model },
		MaxEndpointingDelay:      func() time.Duration { return 15 * time.Millisecond },
		Interrupt: func(context.Context, bool) error {
			session.interrupt.Add(1)
			return nil
		},
		PublishPrediction: func(_ context.Context, event AMDPredictionEvent) error {
			session.published.Add(1)
			session.predictions <- event
			return nil
		},
		Bind: func(value *AMD) { session.boundMu.Lock(); session.bound = value; session.boundMu.Unlock() },
	}
	if session.bus != nil {
		adapter.Subscribe = session.bus.Subscribe
	}
	return session, adapter
}

func (s *amdTestSession) close() {
	if s.bus != nil {
		_ = s.bus.Close()
	}
}

func amdFastOptions(model llm.LLM) AMDOptions {
	return AMDOptions{
		LLM: model, SuppressCompatibilityWarning: true,
		NoSpeechTimeout:         amdDuration(200 * time.Millisecond),
		DetectionTimeout:        amdDuration(time.Second),
		HumanSilenceThreshold:   amdDuration(5 * time.Millisecond),
		MachineSilenceThreshold: amdDuration(5 * time.Millisecond),
		MaxEndpointingDelay:     amdDuration(5 * time.Millisecond),
	}
}

func closeAMDTest(t *testing.T, detector *AMD) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := detector.Close(ctx); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
}

func TestAMDCategoriesDefaultsPromptAndClassificationTool(t *testing.T) {
	if AMDCategoryHuman != "human" || AMDCategoryMachineIVR != "machine-ivr" ||
		AMDCategoryMachineVM != "machine-vm" || AMDCategoryMachineUnavailable != "machine-unavailable" ||
		AMDCategoryUncertain != "uncertain" {
		t.Fatal("AMD category wire values changed")
	}
	if !strings.Contains(AMDPrompt, "Please state your name and why you're calling") ||
		!strings.Contains(AMDPrompt, "voice mailbox that hasn't been set up") {
		t.Fatal("AMD benchmark prompt lost its few-shot examples")
	}

	seen := make(chan llm.ChatOptions, 1)
	model := newAMDTestLLM("openai/gpt-4.1-mini", func(_ context.Context, options llm.ChatOptions) (llm.LLMStream, error) {
		seen <- options
		call := llm.NewFunctionCall("call-1", "save_prediction", `{"label":"machine-vm"}`)
		return newAMDSliceStream(options, llm.ChatChunk{ID: "tool", Delta: &llm.ChoiceDelta{Role: llm.RoleAssistant, ToolCalls: []*llm.FunctionCall{call}}}), nil
	})
	session, adapter := newAMDTestSession(model, false)
	options := amdFastOptions(model)
	detector, err := NewAMDWithSession(adapter, options)
	if err != nil {
		t.Fatal(err)
	}
	closeAMDTest(t, detector)
	predictions := make(chan AMDPredictionEvent, 2)
	metricsSeen := make(chan AMDMetrics, 2)
	detector.OnPrediction(func(event AMDPredictionEvent) { predictions <- event })
	detector.OnMetrics(func(value AMDMetrics) { metricsSeen <- value })

	execution, err := detector.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !detector.Listening() {
		t.Fatal("Start returned before fallback listening began")
	}
	if err := detector.OnUserSpeechStarted(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := detector.OnTranscript(t.Context(), "Please leave a message after the tone"); err != nil {
		t.Fatal(err)
	}
	if err := detector.OnUserSpeechEnded(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	result, err := execution.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.Type() != EventAMDPrediction || result.Category != AMDCategoryMachineVM || !result.IsMachine || result.Reason != "llm" {
		t.Fatalf("prediction = %#v", result)
	}
	if session.pause.Load() != 1 || session.resume.Load() != 1 || session.interrupt.Load() != 1 || session.published.Load() != 1 {
		t.Fatalf("lifecycle counts pause=%d resume=%d interrupt=%d publish=%d", session.pause.Load(), session.resume.Load(), session.interrupt.Load(), session.published.Load())
	}

	chatOptions := <-seen
	if chatOptions.ToolChoice.Kind != llm.ToolChoiceRequired {
		t.Fatalf("tool choice = %q", chatOptions.ToolChoice.Kind)
	}
	tools := chatOptions.ToolContext.SortedFunctionTools()
	if len(tools) != 2 || tools[0].Name() != "postpone_termination" || tools[1].Name() != "save_prediction" {
		t.Fatalf("classification tools = %#v", tools)
	}
	items := chatOptions.ChatContext.Items()
	if len(items) != 2 {
		t.Fatalf("classification chat item count = %d", len(items))
	}
	select {
	case event := <-predictions:
		if event.Category != AMDCategoryMachineVM {
			t.Fatalf("callback prediction = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("prediction callback not emitted")
	}
	select {
	case metric := <-metricsSeen:
		if metric.Category != AMDCategoryMachineVM || metric.Duration <= 0 {
			t.Fatalf("AMD metric = %#v", metric)
		}
	case <-time.After(time.Second):
		t.Fatal("AMD metric not emitted")
	}
}

func TestAMDTwoGateAndEndOfTurnOwnership(t *testing.T) {
	model := amdJSONTestLLM(AMDCategoryMachineVM, "voicemail greeting")
	session, adapter := newAMDTestSession(model, false)
	options := amdFastOptions(model)
	options.MachineSilenceThreshold = amdDuration(80 * time.Millisecond)
	options.MaxEndpointingDelay = amdDuration(time.Second)
	detector, err := NewAMDWithSession(adapter, options)
	if err != nil {
		t.Fatal(err)
	}
	closeAMDTest(t, detector)
	execution, err := detector.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_ = detector.OnUserSpeechStarted(t.Context())
	_ = detector.OnTranscript(t.Context(), "Please leave a message after the tone")
	_ = detector.OnUserSpeechEnded(t.Context(), 0)
	time.Sleep(10 * time.Millisecond)

	skip, err := detector.OnEndOfTurn(t.Context(), RecognizedTurn{Transcript: "Please leave a message after the tone"})
	if err != nil {
		t.Fatal(err)
	}
	if skip {
		t.Fatal("committed but silence-gated verdict skipped the reply")
	}
	result, err := execution.Wait(t.Context())
	if err != nil || result.Category != AMDCategoryMachineVM {
		t.Fatalf("Wait() = %#v, %v", result, err)
	}
	skip, err = detector.OnEndOfTurn(t.Context(), RecognizedTurn{})
	if err != nil || !skip {
		t.Fatalf("settled machine OnEndOfTurn = %v, %v", skip, err)
	}
	if session.interrupt.Load() != 1 {
		t.Fatalf("interrupt count = %d", session.interrupt.Load())
	}
}

func TestAMDHumanAndInterruptOptOutDoNotOwnTurn(t *testing.T) {
	for _, test := range []struct {
		name      string
		category  AMDCategory
		interrupt bool
	}{
		{name: "human", category: AMDCategoryHuman, interrupt: true},
		{name: "machine opt out", category: AMDCategoryMachineUnavailable, interrupt: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := amdJSONTestLLM(test.category, "classified")
			session, adapter := newAMDTestSession(model, false)
			options := amdFastOptions(model)
			options.InterruptOnMachine = amdBool(test.interrupt)
			detector, err := NewAMDWithSession(adapter, options)
			if err != nil {
				t.Fatal(err)
			}
			closeAMDTest(t, detector)
			execution, _ := detector.Start(t.Context())
			_ = detector.OnTranscript(t.Context(), "hello there")
			result, err := execution.Wait(t.Context())
			if err != nil || result.Category != test.category {
				t.Fatalf("Wait() = %#v, %v", result, err)
			}
			skip, err := detector.OnEndOfTurn(t.Context(), RecognizedTurn{})
			if err != nil || skip {
				t.Fatalf("OnEndOfTurn() = %v, %v", skip, err)
			}
			if session.interrupt.Load() != 0 {
				t.Fatalf("interrupt count = %d", session.interrupt.Load())
			}
		})
	}
}

func TestAMDShortGreetingAndLateTranscript(t *testing.T) {
	t.Run("short greeting", func(t *testing.T) {
		model := amdJSONTestLLM(AMDCategoryHuman, "unused")
		_, adapter := newAMDTestSession(model, false)
		options := amdFastOptions(model)
		options.HumanSilenceThreshold = amdDuration(15 * time.Millisecond)
		detector, err := NewAMDWithSession(adapter, options)
		if err != nil {
			t.Fatal(err)
		}
		closeAMDTest(t, detector)
		execution, _ := detector.Start(t.Context())
		_ = detector.OnUserSpeechStarted(t.Context())
		time.Sleep(2 * time.Millisecond)
		_ = detector.OnUserSpeechEnded(t.Context(), 0)
		result, err := execution.Wait(t.Context())
		if err != nil || result.Category != AMDCategoryHuman || result.Reason != "short_greeting" || result.SpeechDurationMS == 0 {
			t.Fatalf("short greeting = %#v, %v", result, err)
		}
	})

	t.Run("late transcript cancels heuristic", func(t *testing.T) {
		model := amdJSONTestLLM(AMDCategoryHuman, "llm-verified")
		_, adapter := newAMDTestSession(model, false)
		options := amdFastOptions(model)
		options.HumanSilenceThreshold = amdDuration(40 * time.Millisecond)
		options.MachineSilenceThreshold = amdDuration(60 * time.Millisecond)
		detector, err := NewAMDWithSession(adapter, options)
		if err != nil {
			t.Fatal(err)
		}
		closeAMDTest(t, detector)
		execution, _ := detector.Start(t.Context())
		_ = detector.OnUserSpeechStarted(t.Context())
		_ = detector.OnUserSpeechEnded(t.Context(), 0)
		time.Sleep(5 * time.Millisecond)
		_ = detector.OnTranscript(t.Context(), "hello there")
		result, err := execution.Wait(t.Context())
		if err != nil || result.Reason != "llm-verified" || result.Transcript != "hello there" {
			t.Fatalf("late transcript = %#v, %v", result, err)
		}
	})
}

func TestAMDTimeoutGates(t *testing.T) {
	t.Run("no speech", func(t *testing.T) {
		model := amdJSONTestLLM(AMDCategoryHuman, "unused")
		session, adapter := newAMDTestSession(model, false)
		options := amdFastOptions(model)
		options.NoSpeechTimeout = amdDuration(5 * time.Millisecond)
		detector, err := NewAMDWithSession(adapter, options)
		if err != nil {
			t.Fatal(err)
		}
		closeAMDTest(t, detector)
		result, err := detector.Execute(t.Context())
		if err != nil || result.Category != AMDCategoryUncertain || result.Reason != "no_speech_timeout" || result.IsMachine {
			t.Fatalf("no speech = %#v, %v", result, err)
		}
		if session.interrupt.Load() != 0 {
			t.Fatal("uncertain result interrupted session")
		}
	})

	t.Run("wait until finished", func(t *testing.T) {
		model := amdJSONTestLLM(AMDCategoryMachineVM, "llm")
		_, adapter := newAMDTestSession(model, false)
		options := amdFastOptions(model)
		options.DetectionTimeout = amdDuration(15 * time.Millisecond)
		options.MaxEndpointingDelay = amdDuration(time.Second)
		options.MachineSilenceThreshold = amdDuration(0)
		detector, err := NewAMDWithSession(adapter, options)
		if err != nil {
			t.Fatal(err)
		}
		closeAMDTest(t, detector)
		execution, _ := detector.Start(t.Context())
		_ = detector.OnUserSpeechStarted(t.Context())
		_ = detector.OnTranscript(t.Context(), "voicemail")
		time.Sleep(30 * time.Millisecond)
		select {
		case <-execution.Done():
			t.Fatal("default waitUntilFinished released before EOT")
		default:
		}
		_, _ = detector.OnEndOfTurn(t.Context(), RecognizedTurn{})
		result, err := execution.Wait(t.Context())
		if err != nil || result.Category != AMDCategoryMachineVM {
			t.Fatalf("gated result = %#v, %v", result, err)
		}
	})

	t.Run("hard detection cap", func(t *testing.T) {
		model := amdJSONTestLLM(AMDCategoryMachineVM, "llm")
		_, adapter := newAMDTestSession(model, false)
		options := amdFastOptions(model)
		options.WaitUntilFinished = amdBool(false)
		options.DetectionTimeout = amdDuration(10 * time.Millisecond)
		options.MaxEndpointingDelay = amdDuration(time.Second)
		detector, err := NewAMDWithSession(adapter, options)
		if err != nil {
			t.Fatal(err)
		}
		closeAMDTest(t, detector)
		execution, _ := detector.Start(t.Context())
		_ = detector.OnUserSpeechStarted(t.Context())
		_ = detector.OnTranscript(t.Context(), "voicemail")
		result, err := execution.Wait(t.Context())
		if err != nil || result.Category != AMDCategoryMachineVM {
			t.Fatalf("hard-cap result = %#v, %v", result, err)
		}
	})
}

func TestAMDPostponeTerminationIsBoundedAndReclassifies(t *testing.T) {
	var calls atomic.Int32
	model := newAMDTestLLM("openai/gpt-4.1-mini", func(_ context.Context, options llm.ChatOptions) (llm.LLMStream, error) {
		count := calls.Add(1)
		var call *llm.FunctionCall
		if count == 1 {
			call = llm.NewFunctionCall("postpone", "postpone_termination", `{"seconds":0.005}`)
		} else {
			call = llm.NewFunctionCall("save", "save_prediction", `{"label":"machine-ivr"}`)
		}
		return newAMDSliceStream(options, llm.ChatChunk{ID: "tools", Delta: &llm.ChoiceDelta{Role: llm.RoleAssistant, ToolCalls: []*llm.FunctionCall{call}}}), nil
	})
	_, adapter := newAMDTestSession(model, false)
	options := amdFastOptions(model)
	detector, err := NewAMDWithSession(adapter, options)
	if err != nil {
		t.Fatal(err)
	}
	closeAMDTest(t, detector)
	execution, _ := detector.Start(t.Context())
	_ = detector.OnUserSpeechStarted(t.Context())
	_ = detector.OnUserSpeechEnded(t.Context(), 0)
	_ = detector.OnTranscript(t.Context(), "Press 1 for sales")
	result, err := execution.Wait(t.Context())
	if err != nil || result.Category != AMDCategoryMachineIVR || calls.Load() < 2 {
		t.Fatalf("postpone result=%#v calls=%d err=%v", result, calls.Load(), err)
	}
}

func TestAMDClassificationFailureAndCloseResumeAuthorization(t *testing.T) {
	t.Run("classification error", func(t *testing.T) {
		model := newAMDTestLLM("openai/gpt-4.1-mini", func(context.Context, llm.ChatOptions) (llm.LLMStream, error) {
			return nil, errors.New("boom")
		})
		session, adapter := newAMDTestSession(model, false)
		detector, err := NewAMDWithSession(adapter, amdFastOptions(model))
		if err != nil {
			t.Fatal(err)
		}
		closeAMDTest(t, detector)
		execution, _ := detector.Start(t.Context())
		_ = detector.OnTranscript(t.Context(), "hello")
		_, err = execution.Wait(t.Context())
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("classification error = %v", err)
		}
		if session.resume.Load() != 1 {
			t.Fatalf("resume count = %d", session.resume.Load())
		}
	})

	t.Run("close active", func(t *testing.T) {
		streamCreated := make(chan *amdBlockingStream, 1)
		model := newAMDTestLLM("openai/gpt-4.1-mini", func(_ context.Context, options llm.ChatOptions) (llm.LLMStream, error) {
			stream := newAMDBlockingStream(options)
			streamCreated <- stream
			return stream, nil
		})
		session, adapter := newAMDTestSession(model, false)
		detector, err := NewAMDWithSession(adapter, amdFastOptions(model))
		if err != nil {
			t.Fatal(err)
		}
		execution, _ := detector.Start(t.Context())
		_ = detector.OnTranscript(t.Context(), "hello")
		select {
		case <-streamCreated:
		case <-time.After(time.Second):
			t.Fatal("classifier stream was not created")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := detector.Close(ctx); err != nil {
			t.Fatal(err)
		}
		_, err = execution.Wait(t.Context())
		if !errors.Is(err, ErrAMDClosed) {
			t.Fatalf("active execution error = %v", err)
		}
		if session.resume.Load() != 1 {
			t.Fatalf("resume count = %d", session.resume.Load())
		}
		if model.closeCount.Load() != 0 {
			t.Fatal("caller-owned LLM was closed")
		}
		if err := detector.Close(ctx); err != nil {
			t.Fatalf("idempotent Close = %v", err)
		}
	})
}

func TestAMDListeningGateStartsTimersAfterAudio(t *testing.T) {
	model := amdJSONTestLLM(AMDCategoryHuman, "unused")
	_, adapter := newAMDTestSession(model, false)
	release := make(chan struct{})
	options := amdFastOptions(model)
	options.NoSpeechTimeout = amdDuration(8 * time.Millisecond)
	options.DetectionTimeout = amdDuration(100 * time.Millisecond)
	options.TrackPublicationTimeout = amdDuration(time.Second)
	options.ListeningGate = AMDListeningGateFunc(func(ctx context.Context, identity string) error {
		if identity != "callee" {
			t.Errorf("gate identity = %q", identity)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
	options.ParticipantIdentity = "callee"
	detector, err := NewAMDWithSession(adapter, options)
	if err != nil {
		t.Fatal(err)
	}
	closeAMDTest(t, detector)
	started := time.Now()
	execution, _ := detector.Start(t.Context())
	time.Sleep(20 * time.Millisecond)
	select {
	case <-execution.Done():
		t.Fatal("detection timer started before participant audio")
	default:
	}
	close(release)
	result, err := execution.Wait(t.Context())
	if err != nil || result.Reason != "no_speech_timeout" || time.Since(started) < 20*time.Millisecond {
		t.Fatalf("gated result=%#v elapsed=%v err=%v", result, time.Since(started), err)
	}
}

func TestAMDListeningGateFailuresSettleParticipantMissing(t *testing.T) {
	for _, test := range []struct {
		name    string
		gate    AMDListeningGate
		timeout time.Duration
	}{
		{name: "adapter error", gate: AMDListeningGateFunc(func(context.Context, string) error { return errors.New("participant disconnected") }), timeout: time.Second},
		{name: "timeout", gate: AMDListeningGateFunc(func(ctx context.Context, _ string) error { <-ctx.Done(); return context.Cause(ctx) }), timeout: 5 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := amdJSONTestLLM(AMDCategoryHuman, "unused")
			_, adapter := newAMDTestSession(model, false)
			options := amdFastOptions(model)
			options.ListeningGate = test.gate
			options.TrackPublicationTimeout = amdDuration(test.timeout)
			detector, err := NewAMDWithSession(adapter, options)
			if err != nil {
				t.Fatal(err)
			}
			closeAMDTest(t, detector)
			result, err := detector.Execute(t.Context())
			if err != nil || result.Category != AMDCategoryUncertain || result.Reason != "participant_missing" {
				t.Fatalf("participant missing = %#v, %v", result, err)
			}
		})
	}
}

func TestAMDSessionEventsDriveRecognitionAndSessionClose(t *testing.T) {
	t.Run("recognition", func(t *testing.T) {
		model := amdJSONTestLLM(AMDCategoryHuman, "session transcript")
		session, adapter := newAMDTestSession(model, true)
		defer session.close()
		detector, err := NewAMDWithSession(adapter, amdFastOptions(model))
		if err != nil {
			t.Fatal(err)
		}
		closeAMDTest(t, detector)
		execution, _ := detector.Start(t.Context())
		now := time.Now()
		_ = session.bus.Publish(t.Context(), NewUserStateChangedEvent(UserStateListening, UserStateSpeaking, now))
		_ = session.bus.Publish(t.Context(), UserInputTranscribedEvent{EventBase: newEventBase(EventUserInputTranscribed, now), Transcript: "hello", Final: true})
		_ = session.bus.Publish(t.Context(), NewUserStateChangedEvent(UserStateSpeaking, UserStateListening, now.Add(time.Millisecond)))
		result, err := execution.Wait(t.Context())
		if err != nil || result.Category != AMDCategoryHuman || result.Transcript != "hello" {
			t.Fatalf("session-driven result = %#v, %v", result, err)
		}
	})

	t.Run("close event", func(t *testing.T) {
		model := amdJSONTestLLM(AMDCategoryHuman, "unused")
		session, adapter := newAMDTestSession(model, true)
		defer session.close()
		detector, err := NewAMDWithSession(adapter, amdFastOptions(model))
		if err != nil {
			t.Fatal(err)
		}
		closeAMDTest(t, detector)
		execution, _ := detector.Start(t.Context())
		_ = session.bus.Publish(t.Context(), NewCloseEvent(CloseReasonUserInitiated, nil, time.Now()))
		result, err := execution.Wait(t.Context())
		if err != nil || result.Reason != "session_closed" {
			t.Fatalf("session close result = %#v, %v", result, err)
		}
	})
}

type amdTestSpeechStream struct {
	events chan stt.SpeechEvent
	closed chan struct{}
	once   sync.Once
	pushes atomic.Int32
}

func newAMDTestSpeechStream() *amdTestSpeechStream {
	return &amdTestSpeechStream{events: make(chan stt.SpeechEvent, 8), closed: make(chan struct{})}
}
func (s *amdTestSpeechStream) Recv(ctx context.Context) (stt.SpeechEvent, error) {
	select {
	case event := <-s.events:
		return event, nil
	case <-s.closed:
		return stt.SpeechEvent{}, io.EOF
	case <-ctx.Done():
		return stt.SpeechEvent{}, context.Cause(ctx)
	}
}
func (s *amdTestSpeechStream) Push(context.Context, agents.AudioFrame) error {
	s.pushes.Add(1)
	return nil
}
func (*amdTestSpeechStream) Flush(context.Context) error { return nil }
func (*amdTestSpeechStream) EndInput() error             { return nil }
func (s *amdTestSpeechStream) Close() error              { s.once.Do(func() { close(s.closed) }); return nil }

type amdTestSTT struct {
	*stt.Base
	stream *amdTestSpeechStream
	closed atomic.Int32
}

func newAMDTestSTT() *amdTestSTT {
	return &amdTestSTT{Base: stt.NewBase("amd-stt", "test", "cartesia/ink-whisper", stt.Capabilities{Streaming: true}), stream: newAMDTestSpeechStream()}
}
func (*amdTestSTT) Recognize(context.Context, []agents.AudioFrame, stt.RecognizeOptions) (stt.SpeechEvent, error) {
	return stt.SpeechEvent{}, errors.New("unused")
}
func (s *amdTestSTT) Stream(context.Context, stt.StreamOptions) (stt.SpeechStream, error) {
	return s.stream, nil
}
func (s *amdTestSTT) Close(context.Context) error { s.closed.Add(1); return s.stream.Close() }

type amdTestAudio struct {
	frames chan agents.AudioFrame
	closed chan struct{}
	once   sync.Once
}

func newAMDTestAudio() *amdTestAudio {
	return &amdTestAudio{frames: make(chan agents.AudioFrame, 8), closed: make(chan struct{})}
}
func (a *amdTestAudio) Recv(ctx context.Context) (agents.AudioFrame, error) {
	select {
	case frame := <-a.frames:
		return frame, nil
	case <-a.closed:
		return agents.AudioFrame{}, io.EOF
	case <-ctx.Done():
		return agents.AudioFrame{}, context.Cause(ctx)
	}
}
func (a *amdTestAudio) Close() error { a.once.Do(func() { close(a.closed) }); return nil }

func TestAMDDedicatedSTTUsesOnlyDedicatedTranscriptAndAudioTee(t *testing.T) {
	model := amdJSONTestLLM(AMDCategoryMachineVM, "voicemail")
	speech := newAMDTestSTT()
	audio := newAMDTestAudio()
	_, adapter := newAMDTestSession(model, false)
	options := amdFastOptions(model)
	options.STT = speech
	options.AudioSource = AMDAudioSourceFunc(func(context.Context) (AMDAudioSubscription, error) { return audio, nil })
	detector, err := NewAMDWithSession(adapter, options)
	if err != nil {
		t.Fatal(err)
	}
	closeAMDTest(t, detector)
	execution, _ := detector.Start(t.Context())
	_ = detector.OnUserSpeechStarted(t.Context())
	_ = detector.OnTranscript(t.Context(), "ignored session transcript")
	audio.frames <- agents.AudioFrame{Data: []int16{1}, SampleRate: 16_000, Channels: 1, SamplesPerChannel: 1}
	deadline := time.Now().Add(time.Second)
	for speech.stream.pushes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	speech.stream.events <- stt.SpeechEvent{Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{{Text: "Please leave a message"}}}
	_ = detector.OnUserSpeechEnded(t.Context(), 0)
	result, err := execution.Wait(t.Context())
	if err != nil || result.Category != AMDCategoryMachineVM || result.Transcript != "Please leave a message" {
		t.Fatalf("dedicated STT result = %#v, %v", result, err)
	}
	if speech.stream.pushes.Load() == 0 {
		t.Fatal("dedicated STT did not receive tee audio")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if speech.closed.Load() != 0 {
		t.Fatal("caller-owned dedicated STT was closed")
	}
}

func TestAMDOptionErrorsAndParsing(t *testing.T) {
	model := amdJSONTestLLM(AMDCategoryHuman, "ok")
	_, adapter := newAMDTestSession(model, false)
	negative := -time.Millisecond
	_, err := NewAMDWithSession(adapter, AMDOptions{LLM: model, NoSpeechTimeout: &negative})
	if !errors.Is(err, ErrAMDInvalidOptions) {
		t.Fatalf("negative timeout error = %v", err)
	}
	_, err = NewAMDWithSession(AMDSessionAdapter{}, AMDOptions{SuppressCompatibilityWarning: true})
	if !errors.Is(err, ErrAMDNoLLM) {
		t.Fatalf("missing LLM error = %v", err)
	}
	if category, reason := parseAMDDetection(`prefix {"category":"machine-unavailable","reason":" full "} suffix`); category != AMDCategoryMachineUnavailable || reason != "full" {
		t.Fatalf("parse detection = %q, %q", category, reason)
	}
	if category, reason := parseAMDDetection("not-json"); category != AMDCategoryUncertain || reason != "not-json" {
		t.Fatalf("parse fallback = %q, %q", category, reason)
	}
}

func TestAMDSessionRegistryAndReusableStress(t *testing.T) {
	model := amdJSONTestLLM(AMDCategoryHuman, "unused")
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model})
	if err != nil {
		t.Fatal(err)
	}
	detector, err := NewAMD(session, AMDOptions{
		LLM: model, SuppressCompatibilityWarning: true,
		NoSpeechTimeout: amdDuration(time.Millisecond), DetectionTimeout: amdDuration(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if session.AMD() != detector {
		t.Fatal("AgentSession.AMD did not return the bound detector")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if session.AMD() != nil {
		t.Fatal("AMD registry retained a closed detector")
	}
	_ = session.Close(ctx)

	// Reusability is independent of AgentSession's started-state contract.
	// Exercise it through the public adapter seam so every run can pause and
	// resume authorization deterministically.
	_, adapter := newAMDTestSession(model, false)
	detector, err = NewAMDWithSession(adapter, AMDOptions{
		LLM: model, SuppressCompatibilityWarning: true,
		NoSpeechTimeout: amdDuration(time.Millisecond), DetectionTimeout: amdDuration(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 40; index++ {
		result, runErr := detector.Execute(t.Context())
		if runErr != nil || result.Reason != "no_speech_timeout" {
			t.Fatalf("run %d = %#v, %v", index, result, runErr)
		}
	}
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

var _ llm.LLM = (*amdTestLLM)(nil)
var _ stt.STT = (*amdTestSTT)(nil)
var _ AMDAudioSubscription = (*amdTestAudio)(nil)
var _ metrics.Metric = metrics.LLM{}
