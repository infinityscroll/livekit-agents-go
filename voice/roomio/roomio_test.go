// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/livekit/agents-go/voice"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

type testRoomIOSession struct {
	input  *voice.AgentInput
	output *voice.AgentOutput
	bus    *voice.EventBus
}

func newTestRoomIOSession() *testRoomIOSession {
	return &testRoomIOSession{
		input: voice.NewAgentInput(nil, nil), output: voice.NewAgentOutput(nil, nil),
		bus: voice.NewEventBus(context.Background(), voice.EventBusOptions{}),
	}
}
func (s *testRoomIOSession) Input() *voice.AgentInput   { return s.input }
func (s *testRoomIOSession) Output() *voice.AgentOutput { return s.output }
func (s *testRoomIOSession) Subscribe(options voice.EventSubscriptionOptions) (*voice.EventSubscription, error) {
	return s.bus.Subscribe(options)
}
func (*testRoomIOSession) Interrupt(context.Context, bool) error { return nil }
func (*testRoomIOSession) GenerateReply(context.Context, voice.GenerateReplyOptions) (*voice.SpeechHandle, error) {
	return nil, nil
}
func (*testRoomIOSession) Close(context.Context, ...voice.CloseOptions) error { return nil }

func TestRoomIODefaultsAndUnsupportedVideo(t *testing.T) {
	t.Parallel()
	input, err := resolveInputOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if input.AudioSampleRate != 24_000 || input.AudioChannels != 1 || !*input.AudioEnabled || !*input.TextEnabled || *input.VideoEnabled || !*input.CloseOnDisconnect || *input.DeleteRoomOnClose {
		t.Fatalf("input defaults = %#v", input)
	}
	output, err := resolveOutputOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if output.AudioQueue != 200*time.Millisecond || !*output.AudioEnabled || !*output.TranscriptionEnabled || !*output.SyncTranscription {
		t.Fatalf("output defaults = %#v", output)
	}
	custom, err := resolveOutputOptions(&RoomOutputOptions{AudioPublishOptions: lksdk.TrackPublicationOptions{
		DisableDTX: true, Stereo: true, Stream: "assistant",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !custom.AudioPublishOptions.DisableDTX || !custom.AudioPublishOptions.Stereo || custom.AudioPublishOptions.Stream != "assistant" || custom.AudioPublishOptions.Name != DefaultParticipantAudioTrackName || custom.AudioPublishOptions.Source != livekit.TrackSource_MICROPHONE {
		t.Fatalf("publication options = %#v", custom.AudioPublishOptions)
	}
	video := DefaultRoomInputOptions()
	video.VideoEnabled = Bool(true)
	if _, err := resolveInputOptions(&video); !errors.Is(err, ErrUnsupportedRawVideo) {
		t.Fatalf("video error = %v", err)
	}
}

func TestRoomIOCloseBeforeStartIsImmediateAndDoesNotDisconnectRoom(t *testing.T) {
	t.Parallel()
	room, bridge := NewRoom(nil, RTCBridgeOptions{})
	io, err := NewRoomIO(context.Background(), RoomIOOptions{
		Room: room, Bridge: bridge,
		Input:  &RoomInputOptions{AudioEnabled: Bool(false), TextEnabled: Bool(false)},
		Output: &RoomOutputOptions{AudioEnabled: Bool(false), TranscriptionEnabled: Bool(false)},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := io.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := io.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if room.DisconnectReason() != livekit.DisconnectReason_UNKNOWN_REASON {
		t.Fatalf("RoomIO changed room ownership: %v", room.DisconnectReason())
	}
}

func TestRoomIOStartReconnectAndCloseLifecycle(t *testing.T) {
	t.Parallel()
	room, bridge := NewRoom(nil, RTCBridgeOptions{})
	io, err := NewRoomIO(context.Background(), RoomIOOptions{
		Room: room, Bridge: bridge,
		Input:  &RoomInputOptions{AudioEnabled: Bool(false), TextEnabled: Bool(false)},
		Output: &RoomOutputOptions{AudioEnabled: Bool(false), TranscriptionEnabled: Bool(false)},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { started <- io.Start(ctx) }()
	stopReconnect := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopReconnect:
				return
			case <-ticker.C:
				bridge.Callback().OnReconnected()
			}
		}
	}()
	select {
	case err := <-started:
		close(stopReconnect)
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(stopReconnect)
		t.Fatal("RoomIO did not start after reconnect")
	}
	if err := io.Start(ctx); err != nil {
		t.Fatalf("idempotent Start: %v", err)
	}
	if io.RawVideoSupported() {
		t.Fatal("raw video unexpectedly reported as supported")
	}
	if err := io.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRoomIOConcurrentStartAndClose(t *testing.T) {
	t.Parallel()
	room, bridge := NewRoom(nil, RTCBridgeOptions{})
	io, err := NewRoomIO(context.Background(), RoomIOOptions{
		Room: room, Bridge: bridge,
		Input:  &RoomInputOptions{AudioEnabled: Bool(false), TextEnabled: Bool(false)},
		Output: &RoomOutputOptions{AudioEnabled: Bool(false), TranscriptionEnabled: Bool(false)},
	})
	if err != nil {
		t.Fatal(err)
	}
	startResult := make(chan error, 1)
	go func() { startResult <- io.Start(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for {
		io.mu.Lock()
		called := io.startCalled
		io.mu.Unlock()
		if called {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Start did not enter lifecycle")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := io.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-startResult:
		if err == nil {
			t.Fatal("Start unexpectedly succeeded while Close won the lifecycle race")
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not unblock on Close")
	}
}

func TestRoomIOStartDeadlineReturnsPromptlyAndCleansUp(t *testing.T) {
	t.Parallel()
	room, bridge := NewRoom(nil, RTCBridgeOptions{})
	io, err := NewRoomIO(context.Background(), RoomIOOptions{
		Room: room, Bridge: bridge,
		Input:  &RoomInputOptions{AudioEnabled: Bool(false), TextEnabled: Bool(false)},
		Output: &RoomOutputOptions{AudioEnabled: Bool(false), TranscriptionEnabled: Bool(false)},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	if err := io.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("Start exceeded caller deadline by too much: %v", elapsed)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := io.Close(closeCtx); err != nil {
		t.Fatalf("eventual cleanup: %v", err)
	}
}

func TestRoomIODeleteRoomOnCloseOnceBeforeStart(t *testing.T) {
	t.Parallel()
	room, bridge := NewRoom(nil, RTCBridgeOptions{})
	calls := make(chan string, 2)
	io, err := NewRoomIO(context.Background(), RoomIOOptions{
		Room: room, Bridge: bridge,
		Input: &RoomInputOptions{
			AudioEnabled: Bool(false), TextEnabled: Bool(false), DeleteRoomOnClose: Bool(true),
		},
		Output:     &RoomOutputOptions{AudioEnabled: Bool(false), TranscriptionEnabled: Bool(false)},
		DeleteRoom: func(_ context.Context, roomName string) error { calls <- roomName; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := io.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := io.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-calls:
	default:
		t.Fatal("room deletion was not requested")
	}
	select {
	case <-calls:
		t.Fatal("room deletion ran more than once")
	default:
	}
}

func TestRoomIODeleteAndErrorCallbackPanicsAreContained(t *testing.T) {
	t.Parallel()
	room, bridge := NewRoom(nil, RTCBridgeOptions{})
	reported := make(chan error, 1)
	io, err := NewRoomIO(context.Background(), RoomIOOptions{
		Room: room, Bridge: bridge,
		Input: &RoomInputOptions{
			AudioEnabled: Bool(false), TextEnabled: Bool(false), DeleteRoomOnClose: Bool(true),
		},
		Output:     &RoomOutputOptions{AudioEnabled: Bool(false), TranscriptionEnabled: Bool(false)},
		DeleteRoom: func(context.Context, string) error { panic("delete panic") },
		OnError: func(err error) {
			select {
			case reported <- err:
			default:
			}
			panic("error callback panic")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := io.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reported:
		var panicErr *CallbackPanicError
		if !errors.As(err, &panicErr) || panicErr.Callback != "DeleteRoom" {
			t.Fatalf("reported error = %#v", err)
		}
	default:
		t.Fatal("DeleteRoom panic was not reported")
	}
}

func TestRoomIOForwardsUserTranscriptAfterSessionReady(t *testing.T) {
	t.Parallel()
	room, bridge := NewRoom(nil, RTCBridgeOptions{})
	session := newTestRoomIOSession()
	defer session.bus.Close()
	io, err := NewRoomIO(context.Background(), RoomIOOptions{
		Room: room, Bridge: bridge, Session: session, ParticipantIdentity: "user",
		Input:  &RoomInputOptions{AudioEnabled: Bool(false), TextEnabled: Bool(false)},
		Output: &RoomOutputOptions{AudioEnabled: Bool(false), TranscriptionEnabled: Bool(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := make(chan error, 1)
	go func() { started <- io.Start(ctx) }()
	for {
		bridge.Callback().OnReconnected()
		select {
		case err := <-started:
			if err != nil {
				t.Fatal(err)
			}
			goto ready
		case <-ctx.Done():
			t.Fatal(context.Cause(ctx))
		case <-time.After(time.Millisecond):
		}
	}

ready:
	publisher := &fakeTextPublisher{local: "agent", trackID: "TR_user"}
	io.mu.Lock()
	io.ownedUserText.pub = publisher
	io.mu.Unlock()
	if err := session.bus.PublishAndWait(ctx, voice.UserInputTranscribedEvent{Transcript: "hello", Final: true}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(publisher.snapshot()) != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("user transcript writers = %d", len(publisher.snapshot()))
		}
		time.Sleep(time.Millisecond)
	}
	if err := io.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if session.output.Transcription() != nil {
		t.Fatal("RoomIO did not restore the session transcription output")
	}
}

func TestRoomIOAutoSelectionLatchesAndParticipantOpsCloseSafely(t *testing.T) {
	t.Parallel()
	room, bridge := NewRoom(nil, RTCBridgeOptions{})
	room.OnParticipantUpdate([]*livekit.ParticipantInfo{
		{Sid: "PA_one", Identity: "one", Kind: livekit.ParticipantInfo_STANDARD, State: livekit.ParticipantInfo_ACTIVE},
		{Sid: "PA_two", Identity: "two", Kind: livekit.ParticipantInfo_STANDARD, State: livekit.ParticipantInfo_ACTIVE},
	})
	one, two := room.GetParticipantByIdentity("one"), room.GetParticipantByIdentity("two")
	if one == nil || two == nil {
		t.Fatal("test participants were not created")
	}
	io, err := NewRoomIO(context.Background(), RoomIOOptions{
		Room: room, Bridge: bridge,
		Input:  &RoomInputOptions{AudioEnabled: Bool(false), TextEnabled: Bool(false)},
		Output: &RoomOutputOptions{AudioEnabled: Bool(false), TranscriptionEnabled: Bool(false)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := io.linkParticipant(context.Background(), one); err != nil {
		t.Fatal(err)
	}
	io.mu.Lock()
	latched := io.desiredIdentity
	io.mu.Unlock()
	if latched != "one" {
		t.Fatalf("auto-selected identity = %q", latched)
	}
	if err := io.unlinkParticipant(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := io.linkParticipant(context.Background(), two); err != nil {
		t.Fatal(err)
	}
	if io.LinkedParticipant() != nil {
		t.Fatal("a different participant replaced the latched auto-selection")
	}

	var workers sync.WaitGroup
	for index := 0; index < 8; index++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			for iteration := 0; iteration < 50; iteration++ {
				if (index+iteration)%2 == 0 {
					_ = io.SetParticipant(context.Background(), "one")
				} else {
					_ = io.UnsetParticipant(context.Background())
				}
			}
		}(index)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := io.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
}

func TestCloseOnDisconnectReasonContract(t *testing.T) {
	t.Parallel()
	for _, reason := range []livekit.DisconnectReason{
		livekit.DisconnectReason_CLIENT_INITIATED,
		livekit.DisconnectReason_ROOM_DELETED,
		livekit.DisconnectReason_USER_REJECTED,
	} {
		if !closesSession(reason) {
			t.Errorf("reason %v should close", reason)
		}
	}
	if closesSession(livekit.DisconnectReason_DUPLICATE_IDENTITY) {
		t.Fatal("duplicate identity should not use participant-disconnect close contract")
	}
}

func TestParticipantSelectionContract(t *testing.T) {
	t.Parallel()
	kinds := []lksdk.ParticipantKind{lksdk.ParticipantConnector, lksdk.ParticipantSIP, lksdk.ParticipantStandard}
	if !participantAccepted("user", lksdk.ParticipantStandard, nil, "", "agent", kinds) {
		t.Fatal("standard participant should be auto-selected")
	}
	if participantAccepted("avatar", lksdk.ParticipantStandard, map[string]string{AttributePublishOnBehalf: "agent"}, "", "agent", kinds) {
		t.Fatal("publish-on-behalf avatar should not be auto-selected")
	}
	if !participantAccepted("avatar", lksdk.ParticipantStandard, map[string]string{AttributePublishOnBehalf: "agent"}, "avatar", "agent", kinds) {
		t.Fatal("explicit identity should override publish-on-behalf filtering")
	}
	if participantAccepted("agent-2", lksdk.ParticipantAgent, nil, "", "agent", kinds) {
		t.Fatal("agent kind should not be auto-selected by default")
	}
}
