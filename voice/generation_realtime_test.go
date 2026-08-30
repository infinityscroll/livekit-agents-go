// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/stt"
)

type voiceTestRealtimeModel struct {
	*llm.RealtimeModelBase
	mu       sync.Mutex
	sessions []*voiceTestRealtimeSession
	closed   int
	failNext error
}

type blockingOpenRealtimeModel struct {
	*llm.RealtimeModelBase
	entered chan struct{}
	exited  chan struct{}
}

func newBlockingOpenRealtimeModel() *blockingOpenRealtimeModel {
	return &blockingOpenRealtimeModel{
		RealtimeModelBase: llm.NewRealtimeModelBase(llm.RealtimeCapabilities{}, "blocking-realtime", "test", "blocking"),
		entered:           make(chan struct{}),
		exited:            make(chan struct{}),
	}
}

func (m *blockingOpenRealtimeModel) Session(ctx context.Context) (llm.RealtimeSession, error) {
	close(m.entered)
	<-ctx.Done()
	close(m.exited)
	return nil, context.Cause(ctx)
}

func (*blockingOpenRealtimeModel) Close(context.Context) error { return nil }

func newVoiceTestRealtimeModel(capabilities llm.RealtimeCapabilities) *voiceTestRealtimeModel {
	return &voiceTestRealtimeModel{RealtimeModelBase: llm.NewRealtimeModelBase(capabilities, "test-realtime", "test", "live")}
}

func (m *voiceTestRealtimeModel) Session(ctx context.Context) (llm.RealtimeSession, error) {
	m.mu.Lock()
	if m.failNext != nil {
		err := m.failNext
		m.failNext = nil
		m.mu.Unlock()
		return nil, err
	}
	session := &voiceTestRealtimeSession{model: m, lifetime: ctx, chat: llm.EmptyChatContext(), tools: llm.EmptyToolContext()}
	m.sessions = append(m.sessions, session)
	m.mu.Unlock()
	return session, nil
}

func (m *voiceTestRealtimeModel) Close(context.Context) error {
	m.mu.Lock()
	m.closed++
	m.mu.Unlock()
	return nil
}

func (m *voiceTestRealtimeModel) latest() *voiceTestRealtimeSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sessions) == 0 {
		return nil
	}
	return m.sessions[len(m.sessions)-1]
}

func (m *voiceTestRealtimeModel) sessionCount() int {
	m.mu.Lock()
	count := len(m.sessions)
	m.mu.Unlock()
	return count
}

func (m *voiceTestRealtimeModel) failNextSession(err error) {
	m.mu.Lock()
	m.failNext = err
	m.mu.Unlock()
}

type voiceTestRealtimeSession struct {
	llm.RealtimeSessionEvents
	model    *voiceTestRealtimeModel
	lifetime context.Context

	mu                 sync.Mutex
	chat               *llm.ChatContext
	tools              *llm.Context
	instructions       string
	instructionUpdates int
	chatUpdates        int
	toolUpdates        int
	optionsUpdates     []llm.RealtimeUpdateOptions
	generated          []llm.GenerationCreatedEvent
	generateOptions    []llm.GenerateRealtimeReplyOptions
	pushed             []agents.AudioFrame
	inputStreams       []stream.Reader[agents.AudioFrame]
	commits            int
	clears             int
	interrupts         int
	truncations        []llm.TruncateRealtimeMessageOptions
	closed             int
}

func (s *voiceTestRealtimeSession) RealtimeModel() llm.RealtimeModel { return s.model }
func (s *voiceTestRealtimeSession) ChatContext() *llm.ChatContext {
	s.mu.Lock()
	chat := s.chat.Copy(llm.CopyOptions{})
	s.mu.Unlock()
	return chat
}
func (s *voiceTestRealtimeSession) Tools() *llm.Context {
	s.mu.Lock()
	tools := s.tools.Copy()
	s.mu.Unlock()
	return tools
}
func (s *voiceTestRealtimeSession) UpdateInstructions(_ context.Context, instructions string) error {
	s.mu.Lock()
	s.instructions = instructions
	s.instructionUpdates++
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) UpdateChatContext(_ context.Context, chat *llm.ChatContext) error {
	if chat == nil {
		return errors.New("nil chat")
	}
	s.mu.Lock()
	s.chat = chat.Copy(llm.CopyOptions{})
	s.chatUpdates++
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) UpdateTools(_ context.Context, tools *llm.Context) error {
	if tools == nil {
		return errors.New("nil tools")
	}
	s.mu.Lock()
	s.tools = tools.Copy()
	s.toolUpdates++
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) UpdateOptions(_ context.Context, options llm.RealtimeUpdateOptions) error {
	s.mu.Lock()
	s.optionsUpdates = append(s.optionsUpdates, options)
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) PushAudio(_ context.Context, frame agents.AudioFrame) error {
	s.mu.Lock()
	s.pushed = append(s.pushed, frame)
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) GenerateReply(_ context.Context, options llm.GenerateRealtimeReplyOptions) (llm.GenerationCreatedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generateOptions = append(s.generateOptions, options)
	if len(s.generated) == 0 {
		return llm.GenerationCreatedEvent{}, errors.New("unexpected realtime generation")
	}
	event := s.generated[0]
	s.generated = s.generated[1:]
	return event, nil
}
func (s *voiceTestRealtimeSession) CommitAudio(context.Context) error {
	s.mu.Lock()
	s.commits++
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) ClearAudio(context.Context) error {
	s.mu.Lock()
	s.clears++
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) Interrupt(context.Context) error {
	s.mu.Lock()
	s.interrupts++
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) Truncate(_ context.Context, options llm.TruncateRealtimeMessageOptions) error {
	s.mu.Lock()
	s.truncations = append(s.truncations, options)
	s.mu.Unlock()
	return nil
}
func (*voiceTestRealtimeSession) StartUserActivity() {}
func (s *voiceTestRealtimeSession) SetInputAudioStream(_ context.Context, source stream.Reader[agents.AudioFrame]) error {
	s.mu.Lock()
	s.inputStreams = append(s.inputStreams, source)
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) Close(context.Context) error {
	s.mu.Lock()
	s.closed++
	s.mu.Unlock()
	return nil
}
func (s *voiceTestRealtimeSession) enqueue(events ...llm.GenerationCreatedEvent) {
	s.mu.Lock()
	s.generated = append(s.generated, events...)
	s.mu.Unlock()
}

func TestRealtimeHandoffReuseEligibility(t *testing.T) {
	t.Run("equivalent configuration", func(t *testing.T) {
		model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
		session, err := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
		if err != nil {
			t.Fatal(err)
		}
		first := MustAgent(AgentOptions[struct{}]{ID: "first", Instructions: llm.NewInstructions("same", "")})
		second := MustAgent(AgentOptions[struct{}]{ID: "second", Instructions: llm.NewInstructions("same", "")})
		if err := session.Start(t.Context(), first); err != nil {
			t.Fatal(err)
		}
		realtime := model.latest()
		if err := session.UpdateAgent(t.Context(), second); err != nil {
			t.Fatal(err)
		}
		if model.sessionCount() != 1 {
			t.Fatalf("sessions = %d, want warm-session reuse", model.sessionCount())
		}
		realtime.mu.Lock()
		interrupts, clears, closes := realtime.interrupts, realtime.clears, realtime.closed
		realtime.mu.Unlock()
		if interrupts != 1 || clears != 1 || closes != 0 {
			t.Fatalf("reuse interrupt=%d clear=%d close=%d", interrupts, clears, closes)
		}
		if err := session.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("changed unsupported configuration", func(t *testing.T) {
		model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
		session, _ := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
		if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "first", Instructions: llm.NewInstructions("old", "")})); err != nil {
			t.Fatal(err)
		}
		firstSession := model.latest()
		if err := session.UpdateAgent(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "second", Instructions: llm.NewInstructions("new", "")})); err != nil {
			t.Fatal(err)
		}
		if model.sessionCount() != 2 {
			t.Fatalf("sessions = %d, want replacement", model.sessionCount())
		}
		firstSession.mu.Lock()
		closed := firstSession.closed
		firstSession.mu.Unlock()
		if closed != 1 {
			t.Fatalf("replaced session closes = %d", closed)
		}
		_ = session.Close(context.Background())
	})

	t.Run("changed supported configuration", func(t *testing.T) {
		model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{MidSessionInstructionsUpdate: true})
		session, _ := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
		if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "first", Instructions: llm.NewInstructions("old", "")})); err != nil {
			t.Fatal(err)
		}
		realtime := model.latest()
		if err := session.UpdateAgent(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "second", Instructions: llm.NewInstructions("new", "")})); err != nil {
			t.Fatal(err)
		}
		if model.sessionCount() != 1 {
			t.Fatalf("sessions = %d, want reuse", model.sessionCount())
		}
		realtime.mu.Lock()
		instructions, updates := realtime.instructions, realtime.instructionUpdates
		realtime.mu.Unlock()
		if instructions != "new" || updates != 2 {
			t.Fatalf("instructions=%q updates=%d", instructions, updates)
		}
		_ = session.Close(context.Background())
	})
}

func TestRealtimeSessionUsesVoiceLifetimeNotStartContext(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	session, _ := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
	startCtx, cancelStart := context.WithCancel(context.Background())
	if err := session.Start(startCtx, MustAgent(AgentOptions[struct{}]{ID: "lifetime_agent"})); err != nil {
		t.Fatal(err)
	}
	realtime := model.latest()
	cancelStart()
	select {
	case <-realtime.lifetime.Done():
		t.Fatalf("realtime lifetime ended with Start context: %v", context.Cause(realtime.lifetime))
	case <-time.After(20 * time.Millisecond):
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-realtime.lifetime.Done():
	case <-time.After(time.Second):
		t.Fatal("realtime lifetime was not cancelled by session close")
	}
}

func TestRealtimeSessionOpenHonorsStartDeadlineWithoutOwningTransport(t *testing.T) {
	model := newBlockingOpenRealtimeModel()
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		Realtime: model, DisableUserAwayTimeout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	startCtx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	err = session.Start(startCtx, MustAgent(AgentOptions[struct{}]{ID: "deadline_agent"}))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error = %v, want deadline exceeded", err)
	}
	waitClosed(t, model.exited, "cancelled realtime Session call")
}

func TestRealtimeHandoffActivationFailureKeepsOldActivity(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	session, _ := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
	first := MustAgent(AgentOptions[struct{}]{ID: "first", Instructions: llm.NewInstructions("old", "")})
	if err := session.Start(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	firstRealtime := model.latest()
	model.failNextSession(errors.New("dial failed"))
	second := MustAgent(AgentOptions[struct{}]{ID: "second", Instructions: llm.NewInstructions("new", "")})
	if err := session.UpdateAgent(t.Context(), second); err == nil || !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("handoff error = %v", err)
	}
	if session.Agent() != first {
		t.Fatal("failed handoff replaced the active agent")
	}
	firstRealtime.mu.Lock()
	closed := firstRealtime.closed
	firstRealtime.mu.Unlock()
	if closed != 0 {
		t.Fatalf("old realtime session closes = %d", closed)
	}
	handle, err := session.Say(t.Context(), "still active", SayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	_ = session.Close(context.Background())
}

func TestRealtimeModelUpdateActivationFailureRollsBackActivity(t *testing.T) {
	oldModel := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	replacement := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	agent := MustAgent(AgentOptions[struct{}]{ID: "rollback_agent"})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		Realtime: oldModel, DisableUserAwayTimeout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })

	oldRealtime := oldModel.latest()
	activationErr := errors.New("replacement dial failed")
	replacement.failNextSession(activationErr)
	replacementOverride := agents.Use[llm.RealtimeModel](replacement)
	err = agent.UpdateOptions(t.Context(), AgentUpdateOptions{Realtime: &replacementOverride})
	if err == nil || !strings.Contains(err.Error(), activationErr.Error()) {
		t.Fatalf("model update error = %v, want activation failure", err)
	}

	if !agent.RealtimeOverride().IsInherited() {
		t.Fatalf("failed update persisted agent override: %#v", agent.RealtimeOverride())
	}
	activity, err := session.currentActivity()
	if err != nil {
		t.Fatal(err)
	}
	if got := activity.modelsSnapshot().realtime; !sameInterface(got, oldModel) {
		t.Fatalf("activity model after failed update = %T %p, want old model %p", got, got, oldModel)
	}
	if got := activity.realtimeSnapshot(); !sameInterface(got, oldRealtime) {
		t.Fatalf("activity session after failed update = %T %p, want old session %p", got, got, oldRealtime)
	}
	oldRealtime.mu.Lock()
	oldClosed := oldRealtime.closed
	oldRealtime.mu.Unlock()
	if oldClosed != 0 || replacement.sessionCount() != 0 {
		t.Fatalf("failed update old closes=%d replacement sessions=%d", oldClosed, replacement.sessionCount())
	}
}

func TestRealtimeWithLocalSTTCommitsAudioWithoutDuplicateReply(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	sttOutput := stream.NewChannel[STTNodeItem](4)
	agent := MustAgent(AgentOptions[struct{}]{ID: "mixed_agent", Hooks: AgentHooks[struct{}]{
		STTNode: func(context.Context, *AgentContext[struct{}], stream.Reader[agents.AudioFrame], ModelSettings) (stream.Reader[STTNodeItem], error) {
			return sttOutput, nil
		},
	}})
	session, _ := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
	input := NewBaseAudioInput(t.Context(), 2, nil)
	session.Input().SetAudio(input)
	subscription, _ := session.Subscribe(EventSubscriptionOptions{Capacity: 32})
	defer subscription.Close()
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	event := stt.SpeechEvent{Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{{Text: "local transcript"}}}
	if err := sttOutput.Send(t.Context(), STTEventItem(event)); err != nil {
		t.Fatal(err)
	}
	for {
		value, err := subscription.Recv(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if transcribed, ok := value.(UserInputTranscribedEvent); ok && transcribed.Final {
			break
		}
	}
	model.latest().enqueue(llm.GenerationCreatedEvent{
		MessageStream: stream.FromSlice([]llm.MessageGeneration{}), FunctionStream: stream.FromSlice([]*llm.FunctionCall{}), UserInitiated: true,
	})
	if err := session.CommitUserTurn(t.Context()); err != nil {
		t.Fatal(err)
	}
	realtime := model.latest()
	var reply *SpeechHandle
	for reply == nil {
		value, err := subscription.Recv(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if created, ok := value.(SpeechCreatedEvent); ok && created.UserInitiated {
			reply = created.SpeechHandle
		}
	}
	if err := reply.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		realtime.mu.Lock()
		commits, generated := realtime.commits, len(realtime.generateOptions)
		realtime.mu.Unlock()
		if commits == 1 {
			if generated != 1 {
				t.Fatalf("GenerateReply calls = %d, want exactly one", generated)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("local endpoint did not commit realtime audio")
		}
		time.Sleep(time.Millisecond)
	}
	var localUsers int
	for _, item := range session.ChatContext().Items() {
		if message, ok := item.(*llm.ChatMessage); ok && message.Role == llm.RoleUser {
			text, _ := message.RawTextContent()
			if text == "local transcript" {
				localUsers++
			}
		}
	}
	if localUsers != 1 {
		t.Fatalf("local user transcript messages = %d, want one when provider transcription is disabled", localUsers)
	}
	if err := session.ClearUserTurn(t.Context()); err != nil {
		t.Fatal(err)
	}
	realtime.mu.Lock()
	clears := realtime.clears
	realtime.mu.Unlock()
	if clears != 1 {
		t.Fatalf("realtime clears = %d", clears)
	}
	_ = session.Close(context.Background())
}

func TestRealtimeLocalTurnStopResponseSuppressesReply(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	sttOutput := stream.NewChannel[STTNodeItem](2)
	agent := MustAgent(AgentOptions[struct{}]{ID: "stop_agent", Hooks: AgentHooks[struct{}]{
		STTNode: func(context.Context, *AgentContext[struct{}], stream.Reader[agents.AudioFrame], ModelSettings) (stream.Reader[STTNodeItem], error) {
			return sttOutput, nil
		},
		OnUserTurnCompleted: func(context.Context, *AgentContext[struct{}], *llm.ChatContext, *llm.ChatMessage) error {
			return StopResponse{}
		},
	}})
	session, _ := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
	session.Input().SetAudio(NewBaseAudioInput(t.Context(), 1, nil))
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	if err := sttOutput.Send(t.Context(), STTEventItem(stt.SpeechEvent{
		Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{{Text: "stop"}},
	})); err != nil {
		t.Fatal(err)
	}
	// The recognition actor serializes final transcript then manual commit.
	time.Sleep(5 * time.Millisecond)
	if err := session.CommitUserTurn(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		realtime := model.latest()
		realtime.mu.Lock()
		commits, replies := realtime.commits, len(realtime.generateOptions)
		realtime.mu.Unlock()
		if commits == 1 {
			if replies != 0 {
				t.Fatalf("StopResponse allowed %d realtime replies", replies)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("realtime audio was not committed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRealtimeServerEventsAudioGenerationAndClose(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{
		AudioOutput: true, TurnDetection: true, UserTranscription: true, MessageTruncation: true,
	})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	input := NewBaseAudioInput(t.Context(), 2, nil)
	session.Input().SetAudio(input)
	var frames int
	var output *ManagedAudioOutput
	output, err = NewManagedAudioOutput(AudioOutputOptions{
		SampleRate: 16_000,
		Capture:    func(context.Context, agents.AudioFrame) error { frames++; return nil },
		Flush: func(context.Context) error {
			return output.NotifyPlaybackFinished(PlaybackFinishedEvent{PlaybackPosition: 42 * time.Millisecond})
		},
		ClearBuffer: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	session.Output().SetAudio(output)
	subscription, err := session.Subscribe(EventSubscriptionOptions{Capacity: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "realtime_agent", Instructions: llm.NewInstructions("help", "")})); err != nil {
		t.Fatal(err)
	}
	realtime := model.latest()
	if realtime == nil {
		t.Fatal("realtime session was not opened")
	}
	realtime.mu.Lock()
	inputAttachCount := len(realtime.inputStreams)
	attached := inputAttachCount > 0 && realtime.inputStreams[inputAttachCount-1] != nil
	realtime.mu.Unlock()
	if !attached {
		t.Fatal("audio input was not attached to realtime session")
	}
	realtime.enqueue(llm.GenerationCreatedEvent{
		MessageStream: stream.FromSlice([]llm.MessageGeneration{}), FunctionStream: stream.FromSlice([]*llm.FunctionCall{}), UserInitiated: true,
	})
	if err := session.CommitUserTurn(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.ClearUserTurn(t.Context()); err != nil {
		t.Fatal(err)
	}

	started := time.Now().Add(-time.Second)
	realtime.EmitInputSpeechStarted(llm.InputSpeechStartedEvent{})
	if session.UserState() != UserStateSpeaking {
		t.Fatalf("user state = %q", session.UserState())
	}
	realtime.EmitInputSpeechStopped(llm.InputSpeechStoppedEvent{UserTranscriptionEnabled: true})
	realtime.EmitInputTranscriptionCompleted(llm.InputTranscriptionCompletedEvent{
		ItemID: "user-rt", Transcript: "hello", IsFinal: true, TurnStartedAt: &started,
	})
	// Providers may replay the terminal transcript after reconnect. Item IDs are
	// deduplicated before history insertion.
	realtime.EmitInputTranscriptionCompleted(llm.InputTranscriptionCompletedEvent{
		ItemID: "user-rt", Transcript: "hello", IsFinal: true, TurnStartedAt: &started,
	})

	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16_000, 1)
	event := realtimeGenerationEvent("message-rt", "response-rt", "hello ", "world", frame)
	realtime.EmitGenerationCreated(event)
	handle := waitForRealtimeSpeech(t, subscription)
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if frames != 1 {
		t.Fatalf("captured frames = %d", frames)
	}
	items := session.ChatContext().Items()
	var users, assistants int
	for _, item := range items {
		message, ok := item.(*llm.ChatMessage)
		if !ok {
			continue
		}
		switch message.ID {
		case "user-rt":
			users++
			if !message.CreatedAt.Equal(started) || !message.Metrics.StartedSpeakingAt.Equal(started) {
				t.Fatalf("user timing = %#v", message.Metrics)
			}
		case "message-rt":
			assistants++
			text, _ := message.RawTextContent()
			if text != "hello world" || len(message.Metrics.ProviderRequestIDs) != 1 || message.Metrics.ProviderRequestIDs[0] != "response-rt" {
				t.Fatalf("assistant message = %#v", message)
			}
		}
	}
	if users != 1 || assistants != 1 {
		t.Fatalf("user messages=%d assistant messages=%d", users, assistants)
	}

	realtime.EmitMetrics(metrics.Realtime{
		Timestamp: time.Now(), InputTokens: 3, OutputTokens: 2,
		Metadata: metrics.Metadata{ModelProvider: "test", ModelName: "live"},
	})
	usage := session.Usage().ModelUsage
	if len(usage) != 1 {
		t.Fatalf("usage entries = %d", len(usage))
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	realtime.mu.Lock()
	detached := len(realtime.inputStreams) >= 2 && realtime.inputStreams[len(realtime.inputStreams)-1] == nil
	commits, clears, closes := realtime.commits, realtime.clears, realtime.closed
	realtime.mu.Unlock()
	if !detached || commits != 1 || clears < 1 || closes != 1 {
		t.Fatalf("detach=%v commits=%d clears=%d closes=%d", detached, commits, clears, closes)
	}
}

func TestRealtimeManualGenerationToolOutputAndAutoReply(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{
		AutoToolReplyGeneration: true, PerResponseToolChoice: true, MidSessionChatContextUpdate: true,
	})
	call := llm.NewFunctionCall("call-rt", "lookup", `{"query":"order"}`)
	runContextSeen := make(chan struct{}, 1)
	tool := llm.MustTool(llm.FunctionToolOptions[struct {
		Query string `json:"query"`
	}, string]{
		Name: "lookup",
		Execute: func(_ context.Context, input struct {
			Query string `json:"query"`
		}, options llm.ToolOptions) (string, error) {
			if run, ok := AsRunContext[struct{}](options.Context); !ok || run.SpeechHandle() == nil {
				return "", errors.New("realtime tool did not receive its voice RunContext")
			}
			runContextSeen <- struct{}{}
			return "found " + input.Query, nil
		},
	})
	tools, _ := llm.NewToolContext(tool)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		Realtime: model, Tools: tools, DisableUserAwayTimeout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "tool_agent", Instructions: llm.NewInstructions("base", "")})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	realtime := model.latest()
	realtime.enqueue(llm.GenerationCreatedEvent{
		MessageStream:  stream.FromSlice([]llm.MessageGeneration{}),
		FunctionStream: stream.FromSlice([]*llm.FunctionCall{call}),
		UserInitiated:  true, ResponseID: "tool-response",
	})
	extra := llm.NewInstructions("one shot", "")
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{
		Instructions: &extra, ToolChoice: llm.ToolChoice{Kind: llm.ToolChoiceNone},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runContextSeen:
	default:
		t.Fatal("realtime tool execution did not expose RunContext")
	}
	var calls, outputs int
	for _, item := range session.ChatContext().Items() {
		switch value := item.(type) {
		case *llm.FunctionCall:
			if value.CallID == "call-rt" {
				calls++
			}
		case *llm.FunctionCallOutput:
			if value.CallID == "call-rt" {
				outputs++
				if value.Output != "found order" || value.IsError {
					t.Fatalf("tool output = %#v", value)
				}
			}
		}
	}
	if calls != 1 || outputs != 1 {
		t.Fatalf("calls=%d outputs=%d", calls, outputs)
	}
	realtime.mu.Lock()
	generateCalls, chatUpdates := len(realtime.generateOptions), realtime.chatUpdates
	generateOptions := append([]llm.GenerateRealtimeReplyOptions(nil), realtime.generateOptions...)
	optionUpdates := append([]llm.RealtimeUpdateOptions(nil), realtime.optionsUpdates...)
	providerChat := realtime.chat.Copy(llm.CopyOptions{})
	realtime.mu.Unlock()
	// Initial configuration plus the tool-output sync. An unchanged manual
	// reply must not force a redundant mid-session chat-context update.
	if generateCalls != 1 || chatUpdates != 2 {
		t.Fatalf("generate calls=%d chat updates=%d", generateCalls, chatUpdates)
	}
	if generateOptions[0].Instructions != "one shot" {
		t.Fatalf("response instructions = %q", generateOptions[0].Instructions)
	}
	if len(optionUpdates) != 2 {
		t.Fatalf("tool-choice updates = %d", len(optionUpdates))
	}
	firstChoice, ok := optionUpdates[0].ToolChoice.Value()
	if !ok || firstChoice.Kind != llm.ToolChoiceNone || !optionUpdates[1].ToolChoice.IsDisabled() {
		t.Fatalf("tool-choice update/reset = %#v", optionUpdates)
	}
	for _, item := range providerChat.Items() {
		if message, ok := item.(*llm.ChatMessage); ok && message.Role == llm.RoleDeveloper {
			t.Fatalf("one-shot/base instructions leaked into realtime chat: %#v", message)
		}
	}
}

func TestRealtimeInterruptionUsesSynchronizedTranscriptAndTruncates(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{AudioOutput: true, MessageTruncation: true})
	session, _ := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
	captured := make(chan struct{})
	var captureOnce sync.Once
	synchronized := "heard"
	var output *ManagedAudioOutput
	output, _ = NewManagedAudioOutput(AudioOutputOptions{
		SampleRate: 16_000,
		Capture: func(context.Context, agents.AudioFrame) error {
			captureOnce.Do(func() { close(captured) })
			return nil
		},
		ClearBuffer: func(context.Context) error {
			return output.NotifyPlaybackFinished(PlaybackFinishedEvent{
				PlaybackPosition: 20 * time.Millisecond, Interrupted: true,
				SynchronizedTranscript: &synchronized,
			})
		},
	})
	session.Output().SetAudio(output)
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "interrupt_agent"})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	audio := stream.NewChannel[agents.AudioFrame](1)
	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16_000, 1)
	if err := audio.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	realtime := model.latest()
	realtime.enqueue(llm.GenerationCreatedEvent{
		MessageStream: stream.FromSlice([]llm.MessageGeneration{{
			MessageID: "interrupt-message",
			TextStream: stream.FromSlice([]llm.RealtimeText{
				llm.PlainRealtimeText("heard "), llm.PlainRealtimeText("unheard"),
			}),
			AudioStream: audio,
			Modalities: func(context.Context) ([]llm.Modality, error) {
				return []llm.Modality{llm.ModalityText, llm.ModalityAudio}, nil
			},
		}}),
		FunctionStream: stream.FromSlice([]*llm.FunctionCall{}),
		UserInitiated:  true,
		ResponseID:     "interrupt-response",
	})
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-captured:
	case <-time.After(time.Second):
		t.Fatal("realtime audio was not forwarded")
	}
	if err := session.Interrupt(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	realtime.mu.Lock()
	truncations := append([]llm.TruncateRealtimeMessageOptions(nil), realtime.truncations...)
	realtime.mu.Unlock()
	if len(truncations) != 1 {
		t.Fatalf("truncations = %d", len(truncations))
	}
	truncate := truncations[0]
	if truncate.MessageID != "interrupt-message" || truncate.AudioEnd != 20*time.Millisecond || truncate.AudioTranscript != "heard" {
		t.Fatalf("truncate = %#v", truncate)
	}
	item, ok := session.ChatContext().GetByID("interrupt-message")
	if !ok {
		t.Fatal("partial assistant message was not committed")
	}
	message := item.(*llm.ChatMessage)
	text, _ := message.RawTextContent()
	if text != "heard" || !message.Interrupted {
		t.Fatalf("partial assistant = %#v", message)
	}
}

func TestRealtimeInterruptChecksUninterruptibleSpeechBeforeProvider(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{AudioOutput: true})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		Realtime: model, DisableUserAwayTimeout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	captured := make(chan struct{})
	var captureOnce sync.Once
	var output *ManagedAudioOutput
	output, err = NewManagedAudioOutput(AudioOutputOptions{
		SampleRate: 16_000,
		Capture: func(context.Context, agents.AudioFrame) error {
			captureOnce.Do(func() { close(captured) })
			return nil
		},
		Flush: func(context.Context) error {
			return output.NotifyPlaybackFinished(PlaybackFinishedEvent{})
		},
		ClearBuffer: func(context.Context) error {
			return output.NotifyPlaybackFinished(PlaybackFinishedEvent{Interrupted: true})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	session.Output().SetAudio(output)
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "uninterruptible_agent"})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })

	audio := stream.NewChannel[agents.AudioFrame](1)
	frame, err := agents.NewAudioFrame(make([]int16, 160), 16_000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := audio.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	realtime := model.latest()
	realtime.enqueue(llm.GenerationCreatedEvent{
		MessageStream: stream.FromSlice([]llm.MessageGeneration{{
			MessageID:   "uninterruptible-message",
			TextStream:  stream.FromSlice([]llm.RealtimeText{llm.PlainRealtimeText("keep speaking")}),
			AudioStream: audio,
			Modalities: func(context.Context) ([]llm.Modality, error) {
				return []llm.Modality{llm.ModalityText, llm.ModalityAudio}, nil
			},
		}}),
		FunctionStream: stream.FromSlice([]*llm.FunctionCall{}),
		UserInitiated:  true,
	})
	allowInterruptions := false
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{AllowInterruptions: &allowInterruptions})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-captured:
	case <-time.After(time.Second):
		t.Fatal("uninterruptible realtime speech did not begin playout")
	}

	err = session.Interrupt(t.Context(), false)
	if !errors.Is(err, ErrInterruptionsDisabled) {
		t.Fatalf("interrupt error = %v, want %v", err, ErrInterruptionsDisabled)
	}
	realtime.mu.Lock()
	providerInterrupts := realtime.interrupts
	realtime.mu.Unlock()
	if providerInterrupts != 0 {
		t.Fatalf("provider Interrupt calls = %d, want 0 for uninterruptible speech", providerInterrupts)
	}

	if err := audio.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRealtimeTimedTextPreservedForTranscription(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	session, _ := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
	var mu sync.Mutex
	var captured []agents.TimedString
	session.Output().SetTranscription(NewManagedTextOutput(TextOutputOptions{Capture: func(_ context.Context, value agents.TimedString) error {
		mu.Lock()
		captured = append(captured, value)
		mu.Unlock()
		return nil
	}}))
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "timed_agent"})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	timed := agents.NewTimedString("aligned", 10*time.Millisecond, 40*time.Millisecond)
	model.latest().enqueue(llm.GenerationCreatedEvent{
		MessageStream: stream.FromSlice([]llm.MessageGeneration{{
			MessageID:  "timed-message",
			TextStream: stream.FromSlice([]llm.RealtimeText{llm.TimedRealtimeText(timed)}),
			Modalities: func(context.Context) ([]llm.Modality, error) { return []llm.Modality{llm.ModalityText}, nil },
		}}),
		FunctionStream: stream.FromSlice([]*llm.FunctionCall{}), UserInitiated: true,
	})
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	values := append([]agents.TimedString(nil), captured...)
	mu.Unlock()
	if len(values) != 1 || values[0] != timed {
		t.Fatalf("timed transcription = %#v", values)
	}
}

func TestRealtimeUnrecoverableErrorClosesSession(t *testing.T) {
	model := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	session, _ := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: model, DisableUserAwayTimeout: true})
	subscription, _ := session.Subscribe(EventSubscriptionOptions{Capacity: 16})
	defer subscription.Close()
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "error_agent"})); err != nil {
		t.Fatal(err)
	}
	model.latest().EmitError(llm.RealtimeModelError{Err: errors.New("provider failed"), Recoverable: false})
	for {
		event, err := subscription.Recv(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if closed, ok := event.(CloseEvent); ok {
			if closed.Reason != CloseReasonError || closed.Err == nil {
				t.Fatalf("close event = %#v", closed)
			}
			break
		}
	}
}

func realtimeGenerationEvent(messageID, responseID, first, second string, frame agents.AudioFrame) llm.GenerationCreatedEvent {
	return llm.GenerationCreatedEvent{
		MessageStream: stream.FromSlice([]llm.MessageGeneration{{
			MessageID: messageID,
			TextStream: stream.FromSlice([]llm.RealtimeText{
				llm.PlainRealtimeText(first), llm.PlainRealtimeText(second),
			}),
			AudioStream: stream.FromSlice([]agents.AudioFrame{frame}),
			Modalities: func(context.Context) ([]llm.Modality, error) {
				return []llm.Modality{llm.ModalityText, llm.ModalityAudio}, nil
			},
		}}),
		FunctionStream: stream.FromSlice([]*llm.FunctionCall{}),
		ResponseID:     responseID,
	}
}

func waitForRealtimeSpeech(t *testing.T, subscription *EventSubscription) *SpeechHandle {
	t.Helper()
	for {
		event, err := subscription.Recv(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if created, ok := event.(SpeechCreatedEvent); ok && !created.UserInitiated {
			return created.SpeechHandle
		}
	}
}

var _ llm.RealtimeModel = (*voiceTestRealtimeModel)(nil)
var _ llm.RealtimeSession = (*voiceTestRealtimeSession)(nil)
