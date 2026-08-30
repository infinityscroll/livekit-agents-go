// SPDX-License-Identifier: Apache-2.0

package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/voice"
	"github.com/livekit/protocol/livekit"
)

type warmTransferTestSession struct {
	mu         sync.Mutex
	said       []string
	replies    []voice.GenerateReplyOptions
	interrupts int
	closes     int
}

func (s *warmTransferTestSession) Say(_ context.Context, text string, _ voice.SayOptions) (*voice.SpeechHandle, error) {
	s.mu.Lock()
	s.said = append(s.said, text)
	s.mu.Unlock()
	handle := voice.NewSpeechHandle(voice.SpeechHandleOptions{})
	handle.MarkDone(nil)
	return handle, nil
}
func (s *warmTransferTestSession) GenerateReply(_ context.Context, options voice.GenerateReplyOptions) (*voice.SpeechHandle, error) {
	s.mu.Lock()
	s.replies = append(s.replies, options)
	s.mu.Unlock()
	handle := voice.NewSpeechHandle(voice.SpeechHandleOptions{})
	handle.MarkDone(nil)
	return handle, nil
}
func (s *warmTransferTestSession) Interrupt(context.Context, bool) error {
	s.mu.Lock()
	s.interrupts++
	s.mu.Unlock()
	return nil
}
func (s *warmTransferTestSession) Close(context.Context, ...voice.CloseOptions) error {
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	return nil
}

type warmTransferTestConsultation struct {
	room         string
	session      *warmTransferTestSession
	disconnected chan error
	mu           sync.Mutex
	closes       int
}

func (c *warmTransferTestConsultation) RoomName() string             { return c.room }
func (c *warmTransferTestConsultation) Session() WarmTransferSession { return c.session }
func (c *warmTransferTestConsultation) Disconnected() <-chan error   { return c.disconnected }
func (c *warmTransferTestConsultation) Close(context.Context) error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	return nil
}

type warmTransferTestHold struct {
	mu    sync.Mutex
	stops int
}

func (h *warmTransferTestHold) Stop(context.Context) error {
	h.mu.Lock()
	h.stops++
	h.mu.Unlock()
	return nil
}

type warmTransferTestBackend struct {
	callerEvents chan WarmTransferParticipantEvent
	consultation *warmTransferTestConsultation
	hold         *warmTransferTestHold

	mu          sync.Mutex
	io          []WarmTransferIOState
	requests    []WarmTransferDialRequest
	deletes     []string
	removes     [][2]string
	moves       [][3]string
	postMerge   []string
	dialStarted chan struct{}
	dialOnce    sync.Once
	dialBlock   <-chan struct{}
	dialErr     error
	moveStarted chan struct{}
	moveOnce    sync.Once
	moveBlock   <-chan struct{}
	moveErr     error
}

func newWarmTransferTestBackend() *warmTransferTestBackend {
	session := &warmTransferTestSession{}
	return &warmTransferTestBackend{
		callerEvents: make(chan WarmTransferParticipantEvent, 4),
		consultation: &warmTransferTestConsultation{room: "caller-human-agent", session: session, disconnected: make(chan error, 1)},
		hold:         &warmTransferTestHold{}, dialStarted: make(chan struct{}), moveStarted: make(chan struct{}),
	}
}
func (b *warmTransferTestBackend) CallerRoomName(context.Context) (string, error) {
	return "caller", nil
}
func (b *warmTransferTestBackend) CallerLocalIdentity(context.Context) (string, error) {
	return "transfer-agent", nil
}
func (b *warmTransferTestBackend) CallerPresent(context.Context) (bool, error) { return true, nil }
func (b *warmTransferTestBackend) CallerDisconnected() <-chan WarmTransferParticipantEvent {
	return b.callerEvents
}
func (b *warmTransferTestBackend) CaptureCallerIO(context.Context) (WarmTransferIOState, error) {
	return WarmTransferIOState{AudioInput: true, AudioOutput: false, TranscriptionOutput: true}, nil
}
func (b *warmTransferTestBackend) SetCallerIO(_ context.Context, state WarmTransferIOState) error {
	b.mu.Lock()
	b.io = append(b.io, state)
	b.mu.Unlock()
	return nil
}
func (b *warmTransferTestBackend) StartHold(context.Context, WarmTransferHoldAudio) (WarmTransferHold, error) {
	return b.hold, nil
}
func (b *warmTransferTestBackend) Dial(ctx context.Context, request WarmTransferDialRequest) (WarmTransferConsultation, error) {
	b.mu.Lock()
	b.requests = append(b.requests, request.Clone())
	b.mu.Unlock()
	b.dialOnce.Do(func() { close(b.dialStarted) })
	if b.dialBlock != nil {
		select {
		case <-b.dialBlock:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	if b.dialErr != nil {
		return nil, b.dialErr
	}
	return b.consultation, nil
}
func (b *warmTransferTestBackend) MoveParticipant(ctx context.Context, from, identity, to string) error {
	b.mu.Lock()
	b.moves = append(b.moves, [3]string{from, identity, to})
	b.mu.Unlock()
	b.moveOnce.Do(func() { close(b.moveStarted) })
	if b.moveBlock != nil {
		select {
		case <-b.moveBlock:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return b.moveErr
}
func (b *warmTransferTestBackend) RemoveParticipant(_ context.Context, room, identity string) error {
	b.mu.Lock()
	b.removes = append(b.removes, [2]string{room, identity})
	b.mu.Unlock()
	return nil
}
func (b *warmTransferTestBackend) DeleteRoom(_ context.Context, room string) error {
	b.mu.Lock()
	b.deletes = append(b.deletes, room)
	b.mu.Unlock()
	return nil
}
func (b *warmTransferTestBackend) DeleteCallerRoomOnDisconnect(_ context.Context, room string) error {
	b.mu.Lock()
	b.postMerge = append(b.postMerge, room)
	b.mu.Unlock()
	return nil
}

func newWarmTransferTestTask(t *testing.T, backend *warmTransferTestBackend, mutate func(*WarmTransferTaskOptions[struct{}])) *WarmTransferTask[struct{}] {
	t.Helper()
	options := WarmTransferTaskOptions[struct{}]{
		Backend: backend, SIPCallTo: "+15551234567", SIPTrunkID: agents.Use("ST_test"),
		CleanupTimeout: time.Second, CallerHangupNoticeTimeout: time.Second, CallerHangupCleanupTimeout: time.Second,
		MergeTimeout: time.Second, DialDrainTimeout: time.Second,
	}
	if mutate != nil {
		mutate(&options)
	}
	task, err := NewWarmTransferTask(options)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func executeTransferTool(t *testing.T, task *WarmTransferTask[struct{}], name, input string) (any, error) {
	t.Helper()
	tool, ok := task.ToolContext().FunctionTool(name)
	if !ok {
		t.Fatalf("missing tool %s", name)
	}
	return tool.Execute(t.Context(), json.RawMessage(input), llm.ToolOptions{})
}

func TestWarmTransferSuccessRestoresIOAndCleansConsultation(t *testing.T) {
	backend := newWarmTransferTestBackend()
	task := newWarmTransferTestTask(t, backend, func(options *WarmTransferTaskOptions[struct{}]) { options.GreetingSpeech = TextSpeech("briefing") })
	type answer struct {
		result WarmTransferResult
		err    error
	}
	done := make(chan answer, 1)
	go func() { result, err := task.Run(t.Context()); done <- answer{result, err} }()
	if err := task.WaitReady(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := executeTransferTool(t, task, "connect_to_caller", `{}`); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || got.result.HumanAgentIdentity != HumanAgentIdentity {
		t.Fatalf("result=%#v err=%v", got.result, got.err)
	}
	backend.mu.Lock()
	if !reflect.DeepEqual(backend.io, []WarmTransferIOState{{}, {AudioInput: true, AudioOutput: false, TranscriptionOutput: true}}) {
		t.Fatalf("I/O transitions=%#v", backend.io)
	}
	if !reflect.DeepEqual(backend.moves, [][3]string{{"caller-human-agent", HumanAgentIdentity, "caller"}}) {
		t.Fatalf("moves=%v", backend.moves)
	}
	if !reflect.DeepEqual(backend.postMerge, []string{"caller"}) {
		t.Fatalf("post merge=%v", backend.postMerge)
	}
	if !reflect.DeepEqual(backend.deletes, []string{"caller-human-agent"}) {
		t.Fatalf("deletes=%v", backend.deletes)
	}
	backend.mu.Unlock()
	backend.consultation.session.mu.Lock()
	said := append([]string(nil), backend.consultation.session.said...)
	backend.consultation.session.mu.Unlock()
	if !reflect.DeepEqual(said, []string{"briefing"}) {
		t.Fatalf("greeting=%v", said)
	}
	backend.hold.mu.Lock()
	stops := backend.hold.stops
	backend.hold.mu.Unlock()
	if stops != 1 {
		t.Fatalf("hold stops=%d", stops)
	}
}

func TestWarmTransferCallerHangupNotifiesBeforeCleanup(t *testing.T) {
	backend := newWarmTransferTestBackend()
	task := newWarmTransferTestTask(t, backend, func(options *WarmTransferTaskOptions[struct{}]) {
		options.CallerHangupSpeech = TextSpeech("caller left")
	})
	done := make(chan error, 1)
	go func() { _, err := task.Run(t.Context()); done <- err }()
	if err := task.WaitReady(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.callerEvents <- WarmTransferParticipantEvent{Identity: "caller", Kind: livekit.ParticipantInfo_STANDARD}
	err := <-done
	var toolErr *llm.ToolError
	if !errors.As(err, &toolErr) || toolErr.Message != "caller hung up before the transfer completed" {
		t.Fatalf("error=%v", err)
	}
	backend.mu.Lock()
	removes := append([][2]string(nil), backend.removes...)
	backend.mu.Unlock()
	if !reflect.DeepEqual(removes, [][2]string{{"caller-human-agent", HumanAgentIdentity}}) {
		t.Fatalf("removes=%v", removes)
	}
	session := backend.consultation.session
	session.mu.Lock()
	defer session.mu.Unlock()
	if !reflect.DeepEqual(session.said, []string{"caller left"}) || session.interrupts != 1 {
		t.Fatalf("said=%v interrupts=%d", session.said, session.interrupts)
	}
}

func TestWarmTransferSuccessfulMoveWinsLateCancellation(t *testing.T) {
	backend := newWarmTransferTestBackend()
	release := make(chan struct{})
	backend.moveBlock = release
	task := newWarmTransferTestTask(t, backend, nil)
	ctx, cancel := context.WithCancelCause(t.Context())
	type answer struct {
		result WarmTransferResult
		err    error
	}
	done := make(chan answer, 1)
	go func() { result, err := task.Run(ctx); done <- answer{result, err} }()
	if err := task.WaitReady(t.Context()); err != nil {
		t.Fatal(err)
	}
	toolDone := make(chan error, 1)
	go func() { _, err := executeTransferTool(t, task, "connect_to_caller", `{}`); toolDone <- err }()
	<-backend.moveStarted
	cancel(errors.New("application shutdown"))
	close(release)
	if err := <-toolDone; err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || got.result.HumanAgentIdentity != HumanAgentIdentity {
		t.Fatalf("late cancel result=%#v err=%v", got.result, got.err)
	}
}

func TestWarmTransferCancellationRollsBackPendingDial(t *testing.T) {
	backend := newWarmTransferTestBackend()
	block := make(chan struct{})
	backend.dialBlock = block
	task := newWarmTransferTestTask(t, backend, func(options *WarmTransferTaskOptions[struct{}]) {
		options.HoldAudio = agents.Disable[WarmTransferHoldAudio]()
	})
	ctx, cancel := context.WithCancelCause(t.Context())
	reason := errors.New("shutdown")
	done := make(chan error, 1)
	go func() { _, err := task.Run(ctx); done <- err }()
	<-backend.dialStarted
	cancel(reason)
	if err := <-done; !errors.Is(err, reason) {
		t.Fatalf("error=%v", err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if !reflect.DeepEqual(backend.io, []WarmTransferIOState{{}, {AudioInput: true, AudioOutput: false, TranscriptionOutput: true}}) {
		t.Fatalf("I/O transitions=%v", backend.io)
	}
	if !reflect.DeepEqual(backend.deletes, []string{"caller-human-agent"}) {
		t.Fatalf("deletes=%v", backend.deletes)
	}
}

func TestWarmTransferDeclineAndVoicemailTools(t *testing.T) {
	for _, tc := range []struct{ name, input, message string }{
		{"decline_transfer", `{"reason":"not available"}`, "human agent declined to connect: not available"},
		{"voicemail_detected", `{}`, "voicemail detected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := newWarmTransferTestBackend()
			task := newWarmTransferTestTask(t, backend, nil)
			done := make(chan error, 1)
			go func() { _, err := task.Run(t.Context()); done <- err }()
			if err := task.WaitReady(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := executeTransferTool(t, task, tc.name, tc.input); err != nil {
				t.Fatal(err)
			}
			var toolErr *llm.ToolError
			err := <-done
			if !errors.As(err, &toolErr) || toolErr.Message != tc.message {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestWarmTransferHelpersAndDefaults(t *testing.T) {
	name, err := ResolveHumanAgentRoomName("call-123", nil)
	if err != nil || name != "call-123-human-agent" {
		t.Fatalf("name=%q err=%v", name, err)
	}
	override := "consult"
	name, err = ResolveHumanAgentRoomName("call-123", &override)
	if err != nil || name != override {
		t.Fatalf("name=%q err=%v", name, err)
	}
	same := "call-123"
	if _, err = ResolveHumanAgentRoomName("call-123", &same); err == nil {
		t.Fatal("same room accepted")
	}

	chat := llm.NewChatContext(llm.NewChatMessage(llm.RoleSystem, "ignore"), llm.NewChatMessage(llm.RoleUser, "need help"), llm.NewChatMessage(llm.RoleAssistant, "I can help"))
	extra := TextInstruction("domain rule")
	instructions := ResolveWarmTransferInstructions(PartialWarmTransferInstructions(InstructionParts{Extra: &extra}), chat)
	for _, expected := range []string{WarmTransferPersona, "Caller: need help", "Assistant: I can help", "domain rule"} {
		if !stringsContains(instructions, expected) {
			t.Fatalf("instructions missing %q:\n%s", expected, instructions)
		}
	}
	full := "replace all"
	if got := ResolveWarmTransferInstructions(FullWarmTransferInstructions(full), chat); got != full {
		t.Fatalf("full=%q", got)
	}

	backend := newWarmTransferTestBackend()
	empty := ""
	if _, err = NewWarmTransferTask(WarmTransferTaskOptions[struct{}]{Backend: backend, SIPCallTo: "x", SIPTrunkID: agents.Use("trunk"), RoomName: &empty}); err == nil {
		t.Fatal("empty room accepted")
	}
	if _, err = NewWarmTransferTask(WarmTransferTaskOptions[struct{}]{Backend: backend, SIPTrunkID: agents.Use("trunk")}); err == nil {
		t.Fatal("empty call target accepted")
	}
}
