// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stt"
)

type keytermTestSTT struct {
	*stt.Base
	mu     sync.Mutex
	pushes [][]string
}

func newKeytermTestSTT(supports bool) *keytermTestSTT {
	return &keytermTestSTT{Base: stt.NewBase("test-stt", "test", "test", stt.Capabilities{Keyterms: supports})}
}

func (s *keytermTestSTT) Recognize(context.Context, []agents.AudioFrame, stt.RecognizeOptions) (stt.SpeechEvent, error) {
	return stt.SpeechEvent{}, errors.New("unused")
}
func (s *keytermTestSTT) Stream(context.Context, stt.StreamOptions) (stt.SpeechStream, error) {
	return nil, errors.New("unused")
}
func (s *keytermTestSTT) Close(context.Context) error { return nil }
func (s *keytermTestSTT) UpdateSessionKeyterms(terms []string) error {
	s.mu.Lock()
	s.pushes = append(s.pushes, slices.Clone(terms))
	s.mu.Unlock()
	return nil
}
func (s *keytermTestSTT) snapshot() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([][]string, len(s.pushes))
	for index := range s.pushes {
		result[index] = slices.Clone(s.pushes[index])
	}
	return result
}

type keytermTestLLM struct {
	mu      sync.Mutex
	results []KeytermDetectionResult
	chats   []*llm.ChatContext
	calls   atomic.Int64
	gate    <-chan struct{}
	metrics agents.EventEmitter[metrics.LLM]
	errors  agents.EventEmitter[llm.ErrorEvent]
}

func (m *keytermTestLLM) Label() string                          { return "keyterm-test-llm" }
func (m *keytermTestLLM) Provider() string                       { return "test" }
func (m *keytermTestLLM) Model() string                          { return "test" }
func (m *keytermTestLLM) Prewarm(context.Context)                {}
func (m *keytermTestLLM) Close(context.Context) error            { return nil }
func (m *keytermTestLLM) OnMetrics(fn func(metrics.LLM)) func()  { return m.metrics.Subscribe(fn) }
func (m *keytermTestLLM) OnError(fn func(llm.ErrorEvent)) func() { return m.errors.Subscribe(fn) }
func (m *keytermTestLLM) Chat(ctx context.Context, options llm.ChatOptions) (llm.LLMStream, error) {
	m.calls.Add(1)
	m.mu.Lock()
	m.chats = append(m.chats, options.ChatContext.Copy(llm.CopyOptions{}))
	var result KeytermDetectionResult
	if len(m.results) != 0 {
		result = m.results[0]
		m.results = m.results[1:]
	}
	m.mu.Unlock()
	arguments, _ := json.Marshal(map[string][]string{
		"pending": result.Pending, "confirm": result.Confirm, "remove": result.Remove,
	})
	return &keytermTestStream{
		ctx: ctx, gate: m.gate,
		response: llm.CollectedResponse{ToolCalls: []*llm.FunctionCall{llm.NewFunctionCall("call", "record_keyterms", string(arguments))}},
	}, nil
}

type keytermTestStream struct {
	ctx      context.Context
	gate     <-chan struct{}
	response llm.CollectedResponse
}

func (s *keytermTestStream) Recv(context.Context) (llm.ChatChunk, error) {
	return llm.ChatChunk{}, io.EOF
}
func (s *keytermTestStream) Close() error                  { return nil }
func (s *keytermTestStream) ChatContext() *llm.ChatContext { return llm.EmptyChatContext() }
func (s *keytermTestStream) ToolContext() *llm.Context     { return llm.EmptyToolContext() }
func (s *keytermTestStream) Collect(ctx context.Context) (llm.CollectedResponse, error) {
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return llm.CollectedResponse{}, context.Cause(ctx)
		case <-s.ctx.Done():
			return llm.CollectedResponse{}, context.Cause(s.ctx)
		}
	}
	return s.response, nil
}

type keytermTestSession struct {
	mu       sync.Mutex
	chat     *llm.ChatContext
	callback func(Event)
}

func newKeytermTestSession() *keytermTestSession {
	return &keytermTestSession{chat: llm.EmptyChatContext()}
}
func (s *keytermTestSession) ChatContext() *llm.ChatContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chat.Copy(llm.CopyOptions{})
}
func (s *keytermTestSession) OnEvent(callback func(Event), _ EventSubscriptionOptions) (func(), error) {
	s.mu.Lock()
	s.callback = callback
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		s.callback = nil
		s.mu.Unlock()
	}, nil
}
func (s *keytermTestSession) add(role llm.ChatRole, text string) {
	s.mu.Lock()
	message, _ := s.chat.AddMessage(role, text)
	callback := s.callback
	s.mu.Unlock()
	if callback != nil {
		callback(ConversationItemAddedEvent{EventBase: newEventBase(EventConversationItemAdded, time.Now()), Item: message})
	}
}

func keytermChat(t *testing.T, messages ...string) *llm.ChatContext {
	t.Helper()
	chat := llm.EmptyChatContext()
	for index, message := range messages {
		role := llm.RoleUser
		if index%2 == 1 {
			role = llm.RoleAssistant
		}
		if _, err := chat.AddMessage(role, message); err != nil {
			t.Fatal(err)
		}
	}
	return chat
}

func TestKeytermDetectorStateMachineAndSTTBinding(t *testing.T) {
	maximum := 2
	model := &keytermTestLLM{results: []KeytermDetectionResult{
		{Pending: []string{"Niamh"}, Confirm: []string{"Static"}},
		{Confirm: []string{"Niamh", "Kubernetes", "LiveKit"}},
		{Remove: []string{"Niamh"}},
	}}
	detector, err := NewKeytermDetector(KeytermsOptions{
		Keyterms:         []string{"Static", "Static"},
		KeytermDetection: KeytermDetectionOptions{Enabled: true, LLM: model, MaxKeyterms: &maximum},
	})
	if err != nil {
		t.Fatal(err)
	}
	speech := newKeytermTestSTT(true)
	if err := detector.SwapSTT(speech); err != nil {
		t.Fatal(err)
	}
	if got := speech.snapshot(); len(got) != 1 || !slices.Equal(got[0], []string{"Static"}) {
		t.Fatalf("initial keyterms = %#v", got)
	}
	if err := detector.RunOnce(context.Background(), keytermChat(t, "It's Niamh")); err != nil {
		t.Fatal(err)
	}
	if got := detector.Keyterms(); !slices.Equal(got, []string{"Static"}) {
		t.Fatalf("pending term was applied: %v", got)
	}
	if got := detector.PendingKeyterms(); !slices.Equal(got, []string{"Niamh"}) {
		t.Fatalf("pending terms = %v", got)
	}
	if len(speech.snapshot()) != 1 {
		t.Fatal("pending-only pass pushed an STT update")
	}
	if err := detector.RunOnce(context.Background(), keytermChat(t, "Niamh", "Niamh is correct")); err != nil {
		t.Fatal(err)
	}
	if got := detector.Keyterms(); !slices.Equal(got, []string{"Static", "Kubernetes", "LiveKit"}) {
		t.Fatalf("cap/eviction result = %v", got)
	}
	if err := detector.RunOnce(context.Background(), keytermChat(t, "Not Niamh")); err != nil {
		t.Fatal(err)
	}
	if got := detector.Keyterms(); !slices.Equal(got, []string{"Static", "Kubernetes", "LiveKit"}) {
		t.Fatalf("remove of evicted term changed state: %v", got)
	}
	if err := detector.SetStaticKeyterms([]string{"New"}); err != nil {
		t.Fatal(err)
	}
	if got := speech.snapshot(); !slices.Equal(got[len(got)-1], []string{"New", "Kubernetes", "LiveKit"}) {
		t.Fatalf("static update = %v", got)
	}
}

func TestAgentSessionKeytermsBindAcrossSTTUpdateAndClose(t *testing.T) {
	first, second := newKeytermTestSTT(true), newKeytermTestSTT(true)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		STT:                    first,
		VADSelection:           disableVAD(),
		DisableUserAwayTimeout: true,
		Keyterms:               KeytermsOptions{Keyterms: []string{"LiveKit", "Niamh"}},
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
	agent := MustAgent(AgentOptions[struct{}]{ID: "keyterm_agent"})
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	if session.KeytermDetector() == nil || !slices.Equal(session.KeytermDetector().Keyterms(), []string{"LiveKit", "Niamh"}) {
		t.Fatal("session keyterm detector accessor did not expose configured state")
	}
	if pushes := first.snapshot(); len(pushes) == 0 || !slices.Equal(pushes[len(pushes)-1], []string{"LiveKit", "Niamh"}) {
		t.Fatalf("initial STT keyterm pushes = %#v", pushes)
	}
	override := agents.Use[stt.STT](second)
	if err := agent.UpdateOptions(t.Context(), AgentUpdateOptions{STT: &override}); err != nil {
		t.Fatal(err)
	}
	if pushes := second.snapshot(); len(pushes) == 0 || !slices.Equal(pushes[len(pushes)-1], []string{"LiveKit", "Niamh"}) {
		t.Fatalf("updated STT keyterm pushes = %#v", pushes)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.KeytermDetector().SetStaticKeyterms([]string{"late"}); !errors.Is(err, ErrKeytermDetectorClosed) {
		t.Fatalf("keyterm update after session close = %v", err)
	}
}

func TestKeytermDetectorPendingTTL(t *testing.T) {
	model := &keytermTestLLM{results: []KeytermDetectionResult{{Pending: []string{"Tmp"}}, {}, {}, {}}}
	detector, err := NewKeytermDetector(KeytermsOptions{KeytermDetection: KeytermDetectionOptions{LLM: model}})
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < KeytermPendingTTL; pass++ {
		if err := detector.RunOnce(context.Background(), keytermChat(t, "turn")); err != nil {
			t.Fatal(err)
		}
	}
	if len(detector.PendingKeyterms()) != 1 {
		t.Fatal("pending term expired one pass too early")
	}
	if err := detector.RunOnce(context.Background(), keytermChat(t, "turn")); err != nil {
		t.Fatal(err)
	}
	if got := detector.PendingKeyterms(); len(got) != 0 {
		t.Fatalf("expired pending terms = %v", got)
	}
}

func TestKeytermDetectorTriggersSingleFlightAndPauseCancels(t *testing.T) {
	gate := make(chan struct{})
	model := &keytermTestLLM{gate: gate, results: []KeytermDetectionResult{{Confirm: []string{"NeverApplied"}}}}
	detector, err := NewKeytermDetector(KeytermsOptions{KeytermDetection: KeytermDetectionOptions{Enabled: true, LLM: model}})
	if err != nil {
		t.Fatal(err)
	}
	session := newKeytermTestSession()
	speech := newKeytermTestSTT(true)
	if err := detector.Start(context.Background(), session, speech); err != nil {
		t.Fatal(err)
	}
	session.add(llm.RoleAssistant, "hello")
	session.add(llm.RoleUser, "first")
	deadline := time.Now().Add(time.Second)
	for model.calls.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if model.calls.Load() != 1 {
		t.Fatal("detection did not start")
	}
	session.add(llm.RoleUser, "second")
	if got := model.calls.Load(); got != 1 {
		t.Fatalf("overlapping detection calls = %d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := detector.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if got := detector.Keyterms(); len(got) != 0 {
		t.Fatalf("cancelled pass mutated keyterms: %v", got)
	}
	if got := speech.snapshot(); len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("cancelled pass pushed STT: %#v", got)
	}
	session.add(llm.RoleUser, "after pause")
	if got := model.calls.Load(); got != 1 {
		t.Fatalf("unsubscribed detector ran again: %d", got)
	}
	close(gate)
}

func TestKeytermDetectionFormattingAndParsing(t *testing.T) {
	chat := llm.EmptyChatContext()
	for index := 0; index < MaxKeytermTranscriptMessages+4; index++ {
		role := llm.RoleUser
		if index%2 != 0 {
			role = llm.RoleAssistant
		}
		_, _ = chat.AddMessage(role, "line\n\nmore")
	}
	formatted, ok := FormatKeytermDetectionInput(chat, []KeytermState{{Term: "Applied", Applied: true}, {Term: "Candidate"}})
	if !ok || !containsAll(formatted, "Applied keyterms", "Applied", "Candidate keyterms", "Candidate", "record_keyterms") {
		t.Fatalf("formatted input = %q", formatted)
	}
	if stringsCount(formatted, "USER: ")+stringsCount(formatted, "ASSISTANT: ") != MaxKeytermTranscriptMessages {
		t.Fatalf("transcript was not bounded: %q", formatted)
	}
	call := llm.NewFunctionCall("call", "record_keyterms", `{"pending":["John","  ",5],"confirm":["Foo"],"remove":["Jon"]}`)
	result := ParseKeytermToolCalls([]*llm.FunctionCall{call})
	if !slices.Equal(result.Pending, []string{"John"}) || !slices.Equal(result.Confirm, []string{"Foo"}) || !slices.Equal(result.Remove, []string{"Jon"}) {
		t.Fatalf("parsed result = %#v", result)
	}
	if got := ParseKeytermToolCalls([]*llm.FunctionCall{llm.NewFunctionCall("call", "record_keyterms", "bad")}); len(got.Pending)+len(got.Confirm)+len(got.Remove) != 0 {
		t.Fatalf("invalid call parsed as %#v", got)
	}
}

func TestKeytermDetectionMetricsDetach(t *testing.T) {
	model := &keytermTestLLM{}
	detector, err := NewKeytermDetector(KeytermsOptions{KeytermDetection: KeytermDetectionOptions{Enabled: true, LLM: model}})
	if err != nil {
		t.Fatal(err)
	}
	received := atomic.Int64{}
	detector.OnMetrics(func(metrics.LLM) { received.Add(1) })
	if err := detector.Start(context.Background(), newKeytermTestSession(), newKeytermTestSTT(true)); err != nil {
		t.Fatal(err)
	}
	model.metrics.Emit(metrics.LLM{})
	if received.Load() != 1 {
		t.Fatal("metric was not forwarded")
	}
	if err := detector.Pause(context.Background()); err != nil {
		t.Fatal(err)
	}
	model.metrics.Emit(metrics.LLM{})
	if received.Load() != 1 {
		t.Fatal("metric subscription was not detached")
	}
}

func containsAll(value string, needles ...string) bool {
	for _, needle := range needles {
		if !stringsContains(value, needle) {
			return false
		}
	}
	return true
}

func stringsContains(value, needle string) bool {
	return len(needle) == 0 || stringsIndex(value, needle) >= 0
}
func stringsIndex(value, needle string) int {
	for index := 0; index+len(needle) <= len(value); index++ {
		if value[index:index+len(needle)] == needle {
			return index
		}
	}
	return -1
}
func stringsCount(value, needle string) int {
	count := 0
	for {
		index := stringsIndex(value, needle)
		if index < 0 {
			return count
		}
		count++
		value = value[index+len(needle):]
	}
}
