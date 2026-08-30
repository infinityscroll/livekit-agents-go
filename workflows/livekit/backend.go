// SPDX-License-Identifier: Apache-2.0

package livekit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/voice"
	"github.com/infinityscroll/livekit-agents-go/voice/roomio"
	core "github.com/infinityscroll/livekit-agents-go/workflows"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WarmTransferHoldAudio = core.WarmTransferHoldAudio
type WarmTransferHold = core.WarmTransferHold
type WarmTransferSession = core.WarmTransferSession
type WarmTransferConsultation = core.WarmTransferConsultation
type WarmTransferDialRequest = core.WarmTransferDialRequest
type WarmTransferParticipantEvent = core.WarmTransferParticipantEvent
type WarmTransferIOState = core.WarmTransferIOState
type WarmTransferBackend = core.WarmTransferBackend
type WarmTransferPostMergeBackend = core.WarmTransferPostMergeBackend

const DefaultWarmTransferCleanupTimeout = core.DefaultWarmTransferCleanupTimeout

const DefaultLiveKitCallerEventCapacity = 16

// LiveKitHoldFactory starts application-owned hold audio on the caller room.
// The Go RTC SDK does not ship the agents-js built-in clip player, so this
// explicit boundary prevents silently substituting silence for the default.
type LiveKitHoldFactory func(context.Context, *lksdk.Room, WarmTransferHoldAudio) (WarmTransferHold, error)

// LiveKitConsultationFactory is the escape hatch for specialized session
// ownership. The default factory creates a voice.AgentSession and RoomIO using
// ConsultationSessionOptions.
type LiveKitConsultationFactory[UserData any] func(context.Context, WarmTransferDialRequest, *lksdk.Room, *roomio.RTCBridge) (WarmTransferSession, *roomio.RoomIO, error)

type LiveKitWarmTransferBackendOptions[UserData any] struct {
	// Job supplies URL/credentials/caller room. URL/APIKey/APISecret/CallerRoom
	// override it when set, which also permits use outside a worker job.
	Job           *agents.JobContext[UserData]
	URL           string
	APIKey        string
	APISecret     string
	CallerRoom    *lksdk.Room
	CallerSession interface {
		Input() *voice.AgentInput
		Output() *voice.AgentOutput
	}

	// Prefer CallerBridge: the backend creates one filtered, bounded
	// subscription and does not process irrelevant RTC data. CallerEvents is for
	// externally-owned room callbacks.
	CallerBridge        *roomio.RTCBridge
	CallerEvents        <-chan WarmTransferParticipantEvent
	CallerEventCapacity int

	HoldFactory                LiveKitHoldFactory
	ConsultationFactory        LiveKitConsultationFactory[UserData]
	ConsultationSessionOptions voice.AgentSessionOptions[UserData]
	RoomConnectOptions         []lksdk.ConnectOption
	RoomIOInput                *roomio.RoomInputOptions
	RoomIOOutput               *roomio.RoomOutputOptions
	ParentContext              context.Context
	CloseTimeout               time.Duration
	OnError                    func(error)
}

// LiveKitWarmTransferBackend implements SIP dialing and room operations with
// server-sdk-go v2.18.1. It owns consultation rooms but never the caller room;
// caller E2EE state is therefore untouched. RoomConnectOptions configure E2EE
// for the separately-owned consultation room when required.
type LiveKitWarmTransferBackend[UserData any] struct {
	options          LiveKitWarmTransferBackendOptions[UserData]
	url, key, secret string
	callerRoom       *lksdk.Room
	callerEvents     <-chan WarmTransferParticipantEvent
	ownedEvents      chan WarmTransferParticipantEvent
	callerSub        *roomio.RTCSubscription

	ctx       context.Context
	cancel    context.CancelCauseFunc
	workers   sync.WaitGroup
	workerMu  sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
}

func NewLiveKitWarmTransferBackend[UserData any](options LiveKitWarmTransferBackendOptions[UserData]) (*LiveKitWarmTransferBackend[UserData], error) {
	if options.Job != nil {
		info := options.Job.Info()
		if options.URL == "" {
			options.URL = info.URL
		}
		if options.APIKey == "" {
			options.APIKey = info.APIKey.Reveal()
		}
		if options.APISecret == "" {
			options.APISecret = info.APISecret.Reveal()
		}
		if options.CallerRoom == nil {
			options.CallerRoom = options.Job.Room()
		}
	}
	if options.URL == "" || options.APIKey == "" || options.APISecret == "" {
		return nil, errors.New("workflows: LiveKit URL, APIKey, and APISecret are required")
	}
	if options.CallerRoom == nil || options.CallerRoom.LocalParticipant == nil {
		return nil, errors.New("workflows: connected caller Room with local participant is required")
	}
	if options.CallerSession == nil {
		return nil, errors.New("workflows: caller AgentSession is required")
	}
	if options.CallerBridge == nil && options.CallerEvents == nil {
		return nil, errors.New("workflows: CallerBridge or bounded CallerEvents is required")
	}
	if options.CallerBridge != nil && options.CallerEvents != nil {
		return nil, errors.New("workflows: set CallerBridge or CallerEvents, not both")
	}
	if options.CallerEventCapacity == 0 {
		options.CallerEventCapacity = DefaultLiveKitCallerEventCapacity
	}
	if options.CallerEventCapacity < 1 {
		return nil, errors.New("workflows: CallerEventCapacity must be positive")
	}
	if options.CloseTimeout == 0 {
		options.CloseTimeout = DefaultWarmTransferCleanupTimeout
	}
	if options.CloseTimeout < 0 {
		return nil, errors.New("workflows: CloseTimeout must not be negative")
	}
	parent := options.ParentContext
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	b := &LiveKitWarmTransferBackend[UserData]{
		options: options, url: options.URL, key: options.APIKey, secret: options.APISecret,
		callerRoom: options.CallerRoom, ctx: ctx, cancel: cancel, closeDone: make(chan struct{}),
	}
	if options.CallerBridge != nil {
		sub, err := options.CallerBridge.SubscribeTypes(options.CallerEventCapacity, roomio.RTCEventParticipantDisconnected)
		if err != nil {
			cancel(err)
			return nil, fmt.Errorf("workflows: subscribe caller RTC events: %w", err)
		}
		b.callerSub = sub
		b.ownedEvents = make(chan WarmTransferParticipantEvent, options.CallerEventCapacity)
		b.callerEvents = b.ownedEvents
		b.workerMu.Lock()
		b.workers.Add(1)
		b.workerMu.Unlock()
		go b.runCallerEvents()
	} else {
		b.callerEvents = options.CallerEvents
	}
	return b, nil
}

func (b *LiveKitWarmTransferBackend[UserData]) runCallerEvents() {
	defer b.workers.Done()
	defer close(b.ownedEvents)
	for {
		event, err := b.callerSub.Recv(b.ctx)
		if err != nil {
			return
		}
		if event.Participant == nil {
			continue
		}
		value := WarmTransferParticipantEvent{Identity: event.Participant.Identity(), Kind: livekit.ParticipantInfo_Kind(event.Participant.Kind())}
		if !isCallerKind(value.Kind) {
			continue
		}
		select {
		case b.ownedEvents <- value:
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *LiveKitWarmTransferBackend[UserData]) CallerRoomName(context.Context) (string, error) {
	name := b.callerRoom.Name()
	if name == "" {
		return "", errors.New("caller room is not available")
	}
	return name, nil
}
func (b *LiveKitWarmTransferBackend[UserData]) CallerLocalIdentity(context.Context) (string, error) {
	if b.callerRoom.LocalParticipant == nil {
		return "", errors.New("caller room local participant is not available")
	}
	return b.callerRoom.LocalParticipant.Identity(), nil
}
func (b *LiveKitWarmTransferBackend[UserData]) CallerPresent(context.Context) (bool, error) {
	for _, participant := range b.callerRoom.GetRemoteParticipants() {
		if isCallerKind(livekit.ParticipantInfo_Kind(participant.Kind())) {
			return true, nil
		}
	}
	return false, nil
}
func (b *LiveKitWarmTransferBackend[UserData]) CallerDisconnected() <-chan WarmTransferParticipantEvent {
	return b.callerEvents
}

func (b *LiveKitWarmTransferBackend[UserData]) CaptureCallerIO(context.Context) (WarmTransferIOState, error) {
	return WarmTransferIOState{
		AudioInput:          b.options.CallerSession.Input().AudioEnabled(),
		AudioOutput:         b.options.CallerSession.Output().AudioEnabled(),
		TranscriptionOutput: b.options.CallerSession.Output().TranscriptionEnabled(),
	}, nil
}
func (b *LiveKitWarmTransferBackend[UserData]) SetCallerIO(_ context.Context, state WarmTransferIOState) error {
	b.options.CallerSession.Input().SetAudioEnabled(state.AudioInput)
	b.options.CallerSession.Output().SetAudioEnabled(state.AudioOutput)
	b.options.CallerSession.Output().SetTranscriptionEnabled(state.TranscriptionOutput)
	return nil
}
func (b *LiveKitWarmTransferBackend[UserData]) StartHold(ctx context.Context, audio WarmTransferHoldAudio) (WarmTransferHold, error) {
	if b.options.HoldFactory == nil {
		return nil, errors.New("workflows: server-sdk-go has no built-in hold clip player; configure HoldFactory or disable HoldAudio")
	}
	return b.options.HoldFactory(ctx, b.callerRoom, audio)
}

func (b *LiveKitWarmTransferBackend[UserData]) Dial(ctx context.Context, request WarmTransferDialRequest) (WarmTransferConsultation, error) {
	if err := context.Cause(b.ctx); err != nil {
		return nil, err
	}
	disconnected := make(chan error, 1)
	callback := lksdk.NewRoomCallback()
	callback.OnDisconnectedWithReason = func(reason lksdk.DisconnectionReason) {
		select {
		case disconnected <- fmt.Errorf("%v", reason):
		default:
		}
	}
	room, bridge := roomio.NewRoom(callback, roomio.RTCBridgeOptions{})
	token, err := auth.NewAccessToken(b.key, b.secret).
		SetIdentity(request.CallerIdentity).
		SetKind(livekit.ParticipantInfo_AGENT).
		SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: request.HumanRoomName}).
		ToJWT()
	if err != nil {
		_ = bridge.Close()
		return nil, fmt.Errorf("workflows: create consultation token: %w", err)
	}
	if err = room.JoinWithContextAndToken(ctx, b.url, token, b.options.RoomConnectOptions...); err != nil {
		_ = bridge.Close()
		return nil, fmt.Errorf("workflows: connect consultation room: %w", err)
	}
	ownerCtx, ownerCancel := context.WithCancelCause(b.ctx)
	consultation := &liveKitConsultation[UserData]{
		backend: b, room: room, bridge: bridge, roomName: request.HumanRoomName,
		disconnected: disconnected, ctx: ownerCtx, cancel: ownerCancel, closeDone: make(chan struct{}),
	}
	cleanup := true
	defer func() {
		if cleanup {
			closeCtx, cancel := context.WithTimeout(context.Background(), b.options.CloseTimeout)
			_ = consultation.Close(closeCtx)
			cancel()
		}
	}()

	var session WarmTransferSession
	var roomAdapter *roomio.RoomIO
	if b.options.ConsultationFactory != nil {
		session, roomAdapter, err = b.options.ConsultationFactory(ownerCtx, request.Clone(), room, bridge)
	} else {
		session, roomAdapter, err = b.defaultConsultation(ownerCtx, request, room, bridge)
	}
	if err != nil {
		return nil, err
	}
	if session == nil || roomAdapter == nil {
		return nil, errors.New("workflows: consultation factory returned nil session or RoomIO")
	}
	consultation.session, consultation.roomIO = session, roomAdapter

	sip := lksdk.NewSIPClient(b.url, b.key, b.secret)
	sipRequest := &livekit.CreateSIPParticipantRequest{
		SipTrunkId: request.SIPTrunkID, Trunk: cloneSIPConnection(request.SIPConnection),
		SipCallTo: request.SIPCallTo, SipNumber: request.SIPNumber, RoomName: request.HumanRoomName,
		ParticipantIdentity: request.HumanIdentity, Dtmf: valueOrEmpty(request.DTMF),
		Headers: cloneStringMap(request.SIPHeaders), WaitUntilAnswered: true,
	}
	if request.RingingTimeout != nil {
		sipRequest.RingingTimeout = durationpb.New(*request.RingingTimeout)
	}
	dialCtx, cancelDial := context.WithCancelCause(ctx)
	defer cancelDial(nil)
	roomReady := make(chan error, 1)
	sipReady := make(chan error, 1)
	go func() { roomReady <- roomAdapter.Start(dialCtx) }()
	go func() {
		_, sipErr := sip.CreateSIPParticipant(dialCtx, sipRequest)
		sipReady <- sipErr
	}()
	roomComplete, sipComplete := false, false
	for !roomComplete || !sipComplete {
		select {
		case roomErr := <-roomReady:
			roomComplete = true
			roomReady = nil
			if roomErr != nil {
				cancelDial(roomErr)
				return nil, fmt.Errorf("workflows: start consultation RoomIO: %w", roomErr)
			}
		case sipErr := <-sipReady:
			sipComplete = true
			sipReady = nil
			if sipErr != nil {
				cancelDial(sipErr)
				return nil, fmt.Errorf("workflows: create SIP participant: %w", sipErr)
			}
		case roomErr := <-disconnected:
			if roomErr == nil {
				roomErr = errors.New("consultation room disconnected")
			}
			cancelDial(roomErr)
			return nil, roomErr
		case <-ctx.Done():
			cancelDial(context.Cause(ctx))
			return nil, context.Cause(ctx)
		}
	}
	cleanup = false
	return consultation, nil
}

func (b *LiveKitWarmTransferBackend[UserData]) defaultConsultation(ctx context.Context, request WarmTransferDialRequest, room *lksdk.Room, bridge *roomio.RTCBridge) (WarmTransferSession, *roomio.RoomIO, error) {
	sessionOptions := b.options.ConsultationSessionOptions
	sessionOptions.ParentContext = ctx
	sessionOptions.Tools = request.Tools
	session, err := voice.NewAgentSession(sessionOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("workflows: create consultation session: %w", err)
	}
	turnHandling := cloneTurnHandlingOptions(request.TurnHandling)
	if request.AllowInterruptions != nil {
		if turnHandling == nil {
			value := voice.DefaultTurnHandlingOptions(false)
			turnHandling = &value
		}
		turnHandling.Interruption.Enabled = *request.AllowInterruptions
	}
	agent, err := voice.NewAgent(voice.AgentOptions[UserData]{
		ID: "warm_transfer_consultation", Instructions: request.Instructions, ChatContext: request.ChatContext,
		Tools: request.Tools, STT: request.STT, VAD: request.VAD, LLM: request.LLM, TTS: request.TTS, TurnHandling: turnHandling,
	})
	if err != nil {
		_ = session.Close(context.Background())
		return nil, nil, fmt.Errorf("workflows: create consultation agent: %w", err)
	}
	if err = session.Start(ctx, agent); err != nil {
		_ = session.Close(context.Background())
		return nil, nil, fmt.Errorf("workflows: start consultation session: %w", err)
	}
	input := cloneRoomInputOptions(b.options.RoomIOInput)
	if input == nil {
		value := roomio.DefaultRoomInputOptions()
		input = &value
	}
	input.ParticipantIdentity = request.HumanIdentity
	input.DeleteRoomOnClose = roomio.Bool(false)
	adapter, err := roomio.NewRoomIO(ctx, roomio.RoomIOOptions{
		Room: room, Bridge: bridge, Session: session, ParticipantIdentity: request.HumanIdentity,
		Input: input, Output: cloneRoomOutputOptions(b.options.RoomIOOutput),
	})
	if err != nil {
		_ = session.Close(context.Background())
		return nil, nil, fmt.Errorf("workflows: create consultation RoomIO: %w", err)
	}
	return session, adapter, nil
}

func (b *LiveKitWarmTransferBackend[UserData]) MoveParticipant(ctx context.Context, from, identity, to string) error {
	client := lksdk.NewRoomServiceClient(b.url, b.key, b.secret)
	_, err := client.MoveParticipant(ctx, &livekit.MoveParticipantRequest{Room: from, Identity: identity, DestinationRoom: to})
	return err
}
func (b *LiveKitWarmTransferBackend[UserData]) RemoveParticipant(ctx context.Context, room, identity string) error {
	client := lksdk.NewRoomServiceClient(b.url, b.key, b.secret)
	_, err := client.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: room, Identity: identity})
	return err
}
func (b *LiveKitWarmTransferBackend[UserData]) DeleteRoom(ctx context.Context, room string) error {
	client := lksdk.NewRoomServiceClient(b.url, b.key, b.secret)
	_, err := client.DeleteRoom(ctx, &livekit.DeleteRoomRequest{Room: room})
	return err
}

func (b *LiveKitWarmTransferBackend[UserData]) DeleteCallerRoomOnDisconnect(_ context.Context, room string) error {
	b.workerMu.Lock()
	if b.closed {
		b.workerMu.Unlock()
		return errors.New("workflows: LiveKit warm-transfer backend is closed")
	}
	b.workers.Add(1)
	b.workerMu.Unlock()
	go func() {
		defer b.workers.Done()
		select {
		case _, ok := <-b.callerEvents:
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), b.options.CloseTimeout)
			if err := b.DeleteRoom(ctx, room); err != nil {
				b.report(fmt.Errorf("workflows: delete caller room after disconnect: %w", err))
			}
			cancel()
		case <-b.ctx.Done():
		}
	}()
	return nil
}

func (b *LiveKitWarmTransferBackend[UserData]) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		b.workerMu.Lock()
		b.closed = true
		b.workerMu.Unlock()
		b.cancel(errors.New("workflows: LiveKit warm-transfer backend closed"))
		if b.callerSub != nil {
			_ = b.callerSub.Close()
		}
		go func() { b.workers.Wait(); close(b.closeDone) }()
	})
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-b.closeDone:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (b *LiveKitWarmTransferBackend[UserData]) report(err error) {
	if err == nil || b.options.OnError == nil {
		return
	}
	defer func() { _ = recover() }()
	b.options.OnError(err)
}

type liveKitConsultation[UserData any] struct {
	backend      *LiveKitWarmTransferBackend[UserData]
	room         *lksdk.Room
	bridge       *roomio.RTCBridge
	roomIO       *roomio.RoomIO
	session      WarmTransferSession
	roomName     string
	disconnected <-chan error
	ctx          context.Context
	cancel       context.CancelCauseFunc
	closeOnce    sync.Once
	closeDone    chan struct{}
	mu           sync.Mutex
	closeErr     error
}

func (c *liveKitConsultation[UserData]) RoomName() string             { return c.roomName }
func (c *liveKitConsultation[UserData]) Session() WarmTransferSession { return c.session }
func (c *liveKitConsultation[UserData]) Disconnected() <-chan error   { return c.disconnected }
func (c *liveKitConsultation[UserData]) Close(ctx context.Context) error {
	c.closeOnce.Do(func() { go c.closeInBackground() })
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-c.closeDone:
		c.mu.Lock()
		err := c.closeErr
		c.mu.Unlock()
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func (c *liveKitConsultation[UserData]) closeInBackground() {
	defer close(c.closeDone)
	c.cancel(errors.New("workflows: consultation closed"))
	ctx, cancel := context.WithTimeout(context.Background(), c.backend.options.CloseTimeout)
	defer cancel()
	var errs []error
	if c.roomIO != nil {
		if err := c.roomIO.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if c.session != nil {
		if err := c.session.Close(ctx, voice.CloseOptions{Reason: voice.CloseReasonUserInitiated}); err != nil {
			errs = append(errs, err)
		}
	}
	if c.room != nil {
		c.room.Disconnect()
	}
	if c.bridge != nil {
		if err := c.bridge.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	c.mu.Lock()
	c.closeErr = errors.Join(errs...)
	c.mu.Unlock()
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func isCallerKind(kind livekit.ParticipantInfo_Kind) bool {
	return kind == livekit.ParticipantInfo_STANDARD || kind == livekit.ParticipantInfo_SIP || kind == livekit.ParticipantInfo_CONNECTOR
}

func cloneSIPConnection(source *livekit.SIPOutboundConfig) *livekit.SIPOutboundConfig {
	if source == nil {
		return nil
	}
	return proto.Clone(source).(*livekit.SIPOutboundConfig)
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return map[string]string{}
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func cloneTurnHandlingOptions(source *voice.TurnHandlingOptions) *voice.TurnHandlingOptions {
	if source == nil {
		return nil
	}
	copy := *source
	if source.Interruption.FalseInterruptionTimeout != nil {
		value := *source.Interruption.FalseInterruptionTimeout
		copy.Interruption.FalseInterruptionTimeout = &value
	}
	if source.Interruption.BackchannelBoundary != nil {
		value := *source.Interruption.BackchannelBoundary
		copy.Interruption.BackchannelBoundary = &value
	}
	if source.UserTurnLimit.MaxWords != nil {
		value := *source.UserTurnLimit.MaxWords
		copy.UserTurnLimit.MaxWords = &value
	}
	if source.UserTurnLimit.MaxDuration != nil {
		value := *source.UserTurnLimit.MaxDuration
		copy.UserTurnLimit.MaxDuration = &value
	}
	return &copy
}

func cloneRoomInputOptions(source *roomio.RoomInputOptions) *roomio.RoomInputOptions {
	if source == nil {
		return nil
	}
	copy := *source
	copy.ParticipantKinds = append([]lksdk.ParticipantKind(nil), source.ParticipantKinds...)
	if source.AudioEnabled != nil {
		value := *source.AudioEnabled
		copy.AudioEnabled = &value
	}
	if source.TextEnabled != nil {
		value := *source.TextEnabled
		copy.TextEnabled = &value
	}
	if source.VideoEnabled != nil {
		value := *source.VideoEnabled
		copy.VideoEnabled = &value
	}
	if source.CloseOnDisconnect != nil {
		value := *source.CloseOnDisconnect
		copy.CloseOnDisconnect = &value
	}
	if source.DeleteRoomOnClose != nil {
		value := *source.DeleteRoomOnClose
		copy.DeleteRoomOnClose = &value
	}
	return &copy
}
func cloneRoomOutputOptions(source *roomio.RoomOutputOptions) *roomio.RoomOutputOptions {
	if source == nil {
		return nil
	}
	copy := *source
	if source.AudioEnabled != nil {
		value := *source.AudioEnabled
		copy.AudioEnabled = &value
	}
	if source.TranscriptionEnabled != nil {
		value := *source.TranscriptionEnabled
		copy.TranscriptionEnabled = &value
	}
	if source.SyncTranscription != nil {
		value := *source.SyncTranscription
		copy.SyncTranscription = &value
	}
	return &copy
}

var _ core.WarmTransferBackend = (*LiveKitWarmTransferBackend[struct{}])(nil)
var _ core.WarmTransferPostMergeBackend = (*LiveKitWarmTransferBackend[struct{}])(nil)
