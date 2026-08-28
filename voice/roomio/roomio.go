// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/voice"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

// RoomIO owns adapters and subscriptions, never the Room. Consequently it is
// compatible with rooms configured for E2EE and never disconnects or mutates
// the caller's E2EE manager during Close.
type RoomIO struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	opts    RoomIOOptions
	input   RoomInputOptions
	output  RoomOutputOptions
	room    *lksdk.Room
	bridge  *RTCBridge
	session Session
	logger  *slog.Logger

	mu                sync.Mutex
	participantMu     sync.Mutex
	started           bool
	ready             bool
	readyDone         chan struct{}
	closed            bool
	desiredIdentity   string
	linked            *lksdk.RemoteParticipant
	participantChange chan struct{}
	connected         bool
	connectionChange  chan struct{}
	textRegistered    bool
	rpcMethods        map[string]struct{}

	audioInput       voice.AudioInput
	ownedAudioInput  *ParticipantAudioInput
	audioOutput      voice.AudioOutput
	ownedAudioOutput *ParticipantAudioOutput
	agentTextOutput  voice.TextOutput
	ownedAgentText   *ParticipantTranscriptionOutput
	ownedUserText    *ParticipantTranscriptionOutput
	synchronizedText *SynchronizedTextOutput
	originalInput    voice.AudioInput
	originalAudio    voice.AudioOutput
	originalText     voice.TextOutput

	incoming       *stream.Channel[IncomingTextStream]
	rtcSub         *RTCSubscription
	sessionSub     *voice.EventSubscription
	closeRequests  *stream.Channel[voice.CloseReason]
	deleteRequests *stream.Channel[struct{}]

	rtcDone, sessionDone, closeRequestDone, deleteDone chan struct{}
	startOnce                                          sync.Once
	closeOnce                                          sync.Once
	cleanupOnce                                        sync.Once
	deleteWorkerOnce                                   sync.Once
	deleteOnce                                         sync.Once
	droppedText                                        atomic.Uint64
	startDone                                          chan struct{}
	closeDone                                          chan struct{}
	startCalled                                        bool
	startErr                                           error
	closeErr                                           error
}

func NewRoomIO(parent context.Context, options RoomIOOptions) (*RoomIO, error) {
	if options.Room == nil || options.Room.LocalParticipant == nil {
		return nil, errors.New("roomio: Room with local participant is required")
	}
	if options.Bridge == nil {
		return nil, errors.New("roomio: Bridge is required; create the room with roomio.NewRoom")
	}
	input, err := resolveInputOptions(options.Input)
	if err != nil {
		return nil, err
	}
	output, err := resolveOutputOptions(options.Output)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	identity := options.ParticipantIdentity
	if identity == "" {
		identity = input.ParticipantIdentity
	}
	r := &RoomIO{
		ctx: ctx, cancel: cancel, opts: options, input: input, output: output,
		room: options.Room, bridge: options.Bridge, session: options.Session, logger: logger,
		desiredIdentity: identity, participantChange: make(chan struct{}),
		connected:        options.Room.ConnectionState() == lksdk.ConnectionStateConnected,
		connectionChange: make(chan struct{}), readyDone: make(chan struct{}), rpcMethods: make(map[string]struct{}),
		incoming:      stream.NewChannel[IncomingTextStream](input.IncomingTextCapacity),
		closeRequests: stream.NewChannel[voice.CloseReason](1), deleteRequests: stream.NewChannel[struct{}](1),
		rtcDone: make(chan struct{}), sessionDone: make(chan struct{}),
		closeRequestDone: make(chan struct{}), deleteDone: make(chan struct{}),
		startDone: make(chan struct{}), closeDone: make(chan struct{}),
	}
	// The lifecycle workers are lazy; closed sentinels make Close-before-Start
	// immediate and are replaced just before each worker is launched.
	close(r.rtcDone)
	close(r.sessionDone)
	close(r.closeRequestDone)
	close(r.deleteDone)
	return r, nil
}

func (r *RoomIO) Start(ctx context.Context) error {
	startCtx, cancel := r.withStartDeadline(ctx)
	defer cancel()
	initiated := false
	r.startOnce.Do(func() {
		initiated = true
		r.mu.Lock()
		r.startCalled = true
		closed := r.closed
		r.mu.Unlock()
		go func() {
			var err error
			if closed {
				err = ErrRoomIOClosed
			} else {
				err = r.start(startCtx)
			}
			r.mu.Lock()
			r.startErr = err
			close(r.startDone)
			r.mu.Unlock()
		}()
	})
	select {
	case <-r.startDone:
	case <-startCtx.Done():
		if initiated {
			// Close starts its eventual cleanup worker before observing this
			// already-expired context, preserving the caller's Start deadline.
			_ = r.Close(startCtx)
		}
		return context.Cause(startCtx)
	}
	r.mu.Lock()
	started, closed, startErr := r.started, r.closed, r.startErr
	r.mu.Unlock()
	if startErr != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), r.closeTimeout())
		defer closeCancel()
		_ = r.Close(closeCtx)
		return startErr
	}
	if closed {
		return ErrRoomIOClosed
	}
	if started {
		return nil
	}
	return ErrRoomIOAlreadyStarted
}

func (r *RoomIO) start(ctx context.Context) (err error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRoomIOClosed
	}
	r.mu.Unlock()
	capacity := r.opts.EventCapacity
	if capacity <= 0 {
		capacity = DefaultParticipantEventQueue
	}
	r.rtcSub, err = r.bridge.SubscribeTypes(capacity,
		RTCEventParticipantConnected,
		RTCEventParticipantDisconnected,
		RTCEventReconnecting,
		RTCEventReconnected,
		RTCEventDisconnected,
	)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.rtcDone = make(chan struct{})
	r.closeRequestDone = make(chan struct{})
	r.mu.Unlock()
	go r.runRTC()
	go r.runCloseRequests()
	if boolValue(r.input.DeleteRoomOnClose, false) {
		r.startDeleteWorker()
	}

	if r.session != nil {
		r.sessionSub, err = r.session.Subscribe(voice.EventSubscriptionOptions{Capacity: capacity})
		if err != nil {
			return err
		}
		r.mu.Lock()
		r.sessionDone = make(chan struct{})
		r.mu.Unlock()
		go r.runSessionEvents()
	}

	if boolValue(r.input.TextEnabled, true) {
		err = r.room.RegisterTextStreamHandler(TopicChat, r.onIncomingText)
		if err != nil {
			// Match agents-js: an application-owned handler takes precedence.
			r.report(fmt.Errorf("roomio: chat text handler not installed: %w", err))
		} else {
			r.mu.Lock()
			r.textRegistered = true
			r.mu.Unlock()
		}
	}

	if boolValue(r.input.AudioEnabled, true) {
		if r.input.ExternalAudio != nil {
			r.mu.Lock()
			r.audioInput = r.input.ExternalAudio
			r.mu.Unlock()
		} else {
			owned, createErr := NewParticipantAudioInput(r.ctx, r.bridge, r.input, r.report)
			err = createErr
			if err != nil {
				return err
			}
			r.mu.Lock()
			r.ownedAudioInput = owned
			r.audioInput = owned
			r.mu.Unlock()
		}
	}

	if r.session != nil {
		r.originalInput = r.session.Input().Audio()
		r.originalAudio = r.session.Output().Audio()
		r.originalText = r.session.Output().Transcription()
	}
	externalAudio := r.output.ExternalAudio
	if externalAudio == nil {
		externalAudio = r.originalAudio
	}
	if boolValue(r.output.AudioEnabled, true) {
		if externalAudio != nil {
			r.mu.Lock()
			r.audioOutput = externalAudio
			r.mu.Unlock()
		} else {
			owned, createErr := NewParticipantAudioOutput(r.ctx, r.room, r.bridge, r.output, r.report)
			err = createErr
			if err != nil {
				return err
			}
			r.mu.Lock()
			r.ownedAudioOutput = owned
			r.audioOutput = owned
			r.mu.Unlock()
		}
	}

	if boolValue(r.output.TranscriptionEnabled, true) {
		localIdentity := r.room.LocalParticipant.Identity()
		r.mu.Lock()
		desiredIdentity := r.desiredIdentity
		r.mu.Unlock()
		agentText, createErr := NewParticipantTranscriptionOutput(r.ctx, r.room, true, localIdentity, r.output)
		err = createErr
		if err != nil {
			return err
		}
		userText, createErr := NewParticipantTranscriptionOutput(r.ctx, r.room, false, desiredIdentity, r.output)
		err = createErr
		if err != nil {
			_ = agentText.Close(context.Background())
			return err
		}
		outputs := []voice.TextOutput{agentText}
		externalText := r.output.ExternalTranscription
		if externalText == nil {
			externalText = r.originalText
		}
		if externalText != nil {
			outputs = append(outputs, externalText)
		}
		agentOutput := voice.TextOutput(NewParallelTextOutput(outputs...))
		var synchronizedText *SynchronizedTextOutput
		if boolValue(r.output.SyncTranscription, true) && r.audioOutput != nil {
			synchronizedAudio := NewSynchronizedAudioOutput(r.audioOutput)
			synchronizedText = NewSynchronizedTextOutput(r.ctx, synchronizedAudio, agentOutput)
			agentOutput = synchronizedText
			r.mu.Lock()
			r.audioOutput = synchronizedAudio
			r.mu.Unlock()
		}
		r.mu.Lock()
		r.ownedAgentText = agentText
		r.ownedUserText = userText
		r.synchronizedText = synchronizedText
		r.agentTextOutput = agentOutput
		r.mu.Unlock()
		// User transcription is driven by session events rather than attached to
		// AgentOutput, so it owns an explicit attachment for its whole lifetime.
		userText.SetAttached(true)
		userText.OnAttached()
	}

	if r.session != nil {
		if r.audioInput != nil {
			r.session.Input().SetAudio(r.audioInput)
		}
		if r.audioOutput != nil {
			r.session.Output().SetAudio(r.audioOutput)
		}
		if r.agentTextOutput != nil {
			r.session.Output().SetTranscription(r.agentTextOutput)
		}
	}
	r.mu.Lock()
	r.ready = true
	close(r.readyDone)
	r.mu.Unlock()

	if err := r.waitRoomConnected(ctx); err != nil {
		return err
	}
	if participant := r.selectExistingParticipant(); participant != nil {
		if err := r.linkParticipant(ctx, participant); err != nil {
			return err
		}
	}
	needsParticipant := boolValue(r.input.AudioEnabled, true) || boolValue(r.input.TextEnabled, true)
	if needsParticipant {
		r.mu.Lock()
		desiredIdentity := r.desiredIdentity
		r.mu.Unlock()
		if _, err := r.WaitForParticipant(ctx, desiredIdentity); err != nil {
			return err
		}
	}
	if r.ownedAudioOutput != nil {
		if err := r.ownedAudioOutput.Start(ctx); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()
	return nil
}

func (r *RoomIO) waitRoomConnected(ctx context.Context) error {
	for {
		r.mu.Lock()
		connected := r.connected || r.room.ConnectionState() == lksdk.ConnectionStateConnected
		changed := r.connectionChange
		r.mu.Unlock()
		if connected {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-r.ctx.Done():
			return ErrRoomIOClosed
		case <-changed:
		}
	}
}

func (r *RoomIO) runRTC() {
	defer close(r.rtcDone)
	if r.rtcSub == nil {
		return
	}
	defer r.rtcSub.Close()
	for {
		event, err := r.rtcSub.Recv(r.ctx)
		if err != nil {
			if context.Cause(r.ctx) == nil && !errors.Is(err, io.EOF) {
				r.report(err)
				if errors.Is(err, ErrRTCEventOverflow) {
					r.cancel(err)
					r.beginClose()
				}
			}
			return
		}
		r.handleRTCEvent(event)
	}
}

func (r *RoomIO) handleRTCEvent(event RTCEvent) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	switch event.Type {
	case RTCEventReconnecting, RTCEventDisconnected:
		r.connected = false
		close(r.connectionChange)
		r.connectionChange = make(chan struct{})
	case RTCEventReconnected:
		r.connected = true
		close(r.connectionChange)
		r.connectionChange = make(chan struct{})
	}
	ready := r.ready
	r.mu.Unlock()
	if !ready {
		return
	}
	switch event.Type {
	case RTCEventParticipantConnected:
		if event.Participant != nil && r.acceptParticipant(event.Participant) {
			r.mu.Lock()
			shouldLink := r.linked == nil
			r.mu.Unlock()
			if shouldLink {
				if err := r.linkParticipant(r.ctx, event.Participant); err != nil {
					r.report(err)
				}
			}
		}
	case RTCEventParticipantDisconnected:
		r.mu.Lock()
		linked := r.linked
		r.mu.Unlock()
		if linked != nil && event.Participant != nil && linked.Identity() == event.Participant.Identity() {
			_ = r.unlinkParticipant(r.ctx)
			if boolValue(r.input.CloseOnDisconnect, true) && closesSession(event.DisconnectReason) {
				r.closeRequests.TrySend(voice.CloseReasonParticipantDisconnected)
			}
		}
	case RTCEventReconnected:
		if participant := r.selectExistingParticipant(); participant != nil {
			r.mu.Lock()
			missing := r.linked == nil
			r.mu.Unlock()
			if missing {
				if err := r.linkParticipant(r.ctx, participant); err != nil {
					r.report(err)
				}
			}
		}
	case RTCEventDisconnected:
		reason := r.room.DisconnectReason()
		if boolValue(r.input.CloseOnDisconnect, true) && closesSession(reason) {
			r.closeRequests.TrySend(voice.CloseReasonParticipantDisconnected)
		}
	}
}

func (r *RoomIO) selectExistingParticipant() *lksdk.RemoteParticipant {
	participants := r.room.GetRemoteParticipants()
	slices.SortFunc(participants, func(a, b *lksdk.RemoteParticipant) int {
		if a.Identity() < b.Identity() {
			return -1
		}
		if a.Identity() > b.Identity() {
			return 1
		}
		return 0
	})
	for _, participant := range participants {
		if r.acceptParticipant(participant) {
			return participant
		}
	}
	return nil
}

func (r *RoomIO) acceptParticipant(participant *lksdk.RemoteParticipant) bool {
	if participant == nil {
		return false
	}
	r.mu.Lock()
	desired := r.desiredIdentity
	r.mu.Unlock()
	return participantAccepted(
		participant.Identity(), participant.Kind(), participant.Attributes(), desired,
		r.room.LocalParticipant.Identity(), r.input.ParticipantKinds,
	)
}

func participantAccepted(identity string, kind lksdk.ParticipantKind, attributes map[string]string, desired, localIdentity string, acceptedKinds []lksdk.ParticipantKind) bool {
	if desired != "" && identity != desired {
		return false
	}
	if desired == "" && attributes[AttributePublishOnBehalf] == localIdentity {
		return false
	}
	return slices.Contains(acceptedKinds, kind)
}

func (r *RoomIO) SetParticipant(ctx context.Context, identity string) error {
	if identity == "" {
		return r.UnsetParticipant(ctx)
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRoomIOClosed
	}
	r.mu.Unlock()
	r.participantMu.Lock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.participantMu.Unlock()
		return ErrRoomIOClosed
	}
	r.desiredIdentity = identity
	current := r.linked
	r.mu.Unlock()
	if current != nil && current.Identity() == identity {
		r.participantMu.Unlock()
		return nil
	}
	if current != nil {
		if err := r.unlinkParticipantLocked(ctx); err != nil {
			r.participantMu.Unlock()
			return err
		}
	}
	var linked *lksdk.RemoteParticipant
	if participant := r.room.GetParticipantByIdentity(identity); participant != nil && r.acceptParticipant(participant) {
		if err := r.linkParticipantLocked(ctx, participant); err != nil {
			r.participantMu.Unlock()
			return err
		}
		linked = participant
	}
	r.participantMu.Unlock()
	if linked != nil {
		r.notifyParticipantLinked(linked)
	}
	return nil
}

func (r *RoomIO) UnsetParticipant(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRoomIOClosed
	}
	r.mu.Unlock()
	r.participantMu.Lock()
	defer r.participantMu.Unlock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRoomIOClosed
	}
	r.desiredIdentity = ""
	r.mu.Unlock()
	return r.unlinkParticipantLocked(ctx)
}

func (r *RoomIO) linkParticipant(ctx context.Context, participant *lksdk.RemoteParticipant) error {
	r.participantMu.Lock()
	r.mu.Lock()
	before := r.linked
	r.mu.Unlock()
	err := r.linkParticipantLocked(ctx, participant)
	r.mu.Lock()
	after := r.linked
	r.mu.Unlock()
	r.participantMu.Unlock()
	if err == nil && before != after && after == participant {
		r.notifyParticipantLinked(participant)
	}
	return err
}

func (r *RoomIO) linkParticipantLocked(ctx context.Context, participant *lksdk.RemoteParticipant) error {
	if participant == nil {
		return ErrNoLinkedParticipant
	}
	r.mu.Lock()
	closed, current, desired := r.closed, r.linked, r.desiredIdentity
	r.mu.Unlock()
	if closed {
		return ErrRoomIOClosed
	}
	if current != nil || (desired != "" && desired != participant.Identity()) {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.ctx.Done():
		return ErrRoomIOClosed
	default:
	}
	r.mu.Lock()
	audioInput, userText := r.ownedAudioInput, r.ownedUserText
	r.mu.Unlock()
	if audioInput != nil {
		if err := audioInput.SetParticipant(ctx, participant); err != nil {
			return err
		}
	}
	if userText != nil {
		if err := userText.SetParticipant(ctx, participant.Identity()); err != nil {
			return err
		}
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRoomIOClosed
	}
	if r.desiredIdentity == "" {
		// Match agents-js: the first accepted auto-selected participant becomes
		// the stable target, so a disconnect cannot silently hand the agent to a
		// different participant.
		r.desiredIdentity = participant.Identity()
	}
	r.linked = participant
	close(r.participantChange)
	r.participantChange = make(chan struct{})
	r.mu.Unlock()
	return nil
}

func (r *RoomIO) unlinkParticipant(ctx context.Context) error {
	r.participantMu.Lock()
	defer r.participantMu.Unlock()
	return r.unlinkParticipantLocked(ctx)
}

func (r *RoomIO) unlinkParticipantLocked(ctx context.Context) error {
	r.mu.Lock()
	audioInput, userText := r.ownedAudioInput, r.ownedUserText
	r.mu.Unlock()
	if audioInput != nil {
		if err := audioInput.SetParticipant(ctx, nil); err != nil && !errors.Is(err, ErrParticipantAudioInputClosed) {
			return err
		}
	}
	if userText != nil {
		if err := userText.SetParticipant(ctx, ""); err != nil && !errors.Is(err, ErrParticipantTranscriptionClosed) {
			return err
		}
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRoomIOClosed
	}
	r.linked = nil
	close(r.participantChange)
	r.participantChange = make(chan struct{})
	r.mu.Unlock()
	return nil
}

func (r *RoomIO) notifyParticipantLinked(participant *lksdk.RemoteParticipant) {
	if participant != nil && r.opts.OnParticipantLinked != nil {
		if err := invokeCallback("OnParticipantLinked", func() { r.opts.OnParticipantLinked(participant) }); err != nil {
			r.report(err)
		}
	}
}

func (r *RoomIO) WaitForParticipant(ctx context.Context, identity string) (*lksdk.RemoteParticipant, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		r.mu.Lock()
		participant := r.linked
		changed := r.participantChange
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return nil, ErrRoomIOClosed
		}
		if participant != nil && (identity == "" || participant.Identity() == identity) {
			return participant, nil
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-r.ctx.Done():
			return nil, ErrRoomIOClosed
		case <-changed:
		}
	}
}

func (r *RoomIO) LinkedParticipant() *lksdk.RemoteParticipant {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.linked
}
func (r *RoomIO) IsParticipantAvailable() bool              { return r.LinkedParticipant() != nil }
func (r *RoomIO) RTCRoom() *lksdk.Room                      { return r.room }
func (r *RoomIO) LocalParticipant() *lksdk.LocalParticipant { return r.room.LocalParticipant }
func (r *RoomIO) AudioInput() voice.AudioInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.audioInput
}
func (r *RoomIO) AudioOutput() voice.AudioOutput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.audioOutput
}
func (r *RoomIO) TranscriptionOutput() voice.TextOutput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.agentTextOutput
}
func (r *RoomIO) IncomingTextStreams() stream.Reader[IncomingTextStream] { return r.incoming }
func (r *RoomIO) DroppedIncomingTextStreams() uint64                     { return r.droppedText.Load() }
func (*RoomIO) RawVideoSupported() bool                                  { return false }

func (r *RoomIO) onIncomingText(reader *lksdk.TextStreamReader, identity string) {
	r.mu.Lock()
	linked, desired, closed := r.linked, r.desiredIdentity, r.closed
	r.mu.Unlock()
	if closed || reader == nil || (linked != nil && linked.Identity() != identity) || (linked == nil && desired != "" && desired != identity) {
		return
	}
	participant := r.room.GetParticipantByIdentity(identity)
	if participant == nil || !r.acceptParticipant(participant) {
		return
	}
	input := IncomingTextStream{Reader: reader, ParticipantIdentity: identity}
	if !r.incoming.TrySend(input) {
		r.droppedText.Add(1)
		r.report(ErrIncomingTextOverflow)
		return
	}
}

func (r *RoomIO) runSessionEvents() {
	defer close(r.sessionDone)
	defer r.sessionSub.Close()
	select {
	case <-r.readyDone:
	case <-r.ctx.Done():
		return
	}
	for {
		event, err := r.sessionSub.Recv(r.ctx)
		if err != nil {
			return
		}
		switch value := event.(type) {
		case voice.AgentStateChangedEvent:
			if r.room.ConnectionState() == lksdk.ConnectionStateConnected {
				r.room.LocalParticipant.SetAttributes(map[string]string{AttributeAgentState: string(value.NewState)})
			}
		case voice.UserInputTranscribedEvent:
			r.mu.Lock()
			userText := r.ownedUserText
			r.mu.Unlock()
			if userText != nil {
				text := agents.TimedString{Text: value.Transcript, SpeakerID: value.SpeakerID}
				if err := userText.CaptureText(r.ctx, text); err != nil {
					r.report(err)
				} else if value.Final {
					if err := userText.Flush(r.ctx); err != nil {
						r.report(err)
					}
				}
			}
		case voice.CloseEvent:
			if boolValue(r.input.DeleteRoomOnClose, false) {
				r.deleteRequests.TrySend(struct{}{})
			}
		}
	}
}

func (r *RoomIO) runCloseRequests() {
	defer close(r.closeRequestDone)
	for {
		reason, err := r.closeRequests.Recv(r.ctx)
		if err != nil {
			return
		}
		if r.opts.OnCloseRequested != nil {
			if err := invokeCallback("OnCloseRequested", func() { r.opts.OnCloseRequested(reason) }); err != nil {
				r.report(err)
			}
		}
		if r.session != nil {
			ctx, cancel := context.WithTimeout(context.Background(), r.closeTimeout())
			err := r.session.Close(ctx, voice.CloseOptions{Reason: reason})
			cancel()
			if err != nil {
				r.report(err)
			}
		}
	}
}

func (r *RoomIO) runDeleteRequests() {
	defer close(r.deleteDone)
	for {
		_, err := r.deleteRequests.Recv(context.Background())
		if err != nil {
			return
		}
		r.deleteOnce.Do(func() {
			if r.opts.DeleteRoom == nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), r.closeTimeout())
			defer cancel()
			if err := invokeErrorCallback("DeleteRoom", func() error { return r.opts.DeleteRoom(ctx, r.room.Name()) }); err != nil {
				r.report(err)
			}
		})
	}
}

func (r *RoomIO) startDeleteWorker() {
	r.deleteWorkerOnce.Do(func() {
		r.mu.Lock()
		r.deleteDone = make(chan struct{})
		r.mu.Unlock()
		go r.runDeleteRequests()
	})
}

func (r *RoomIO) PublishData(ctx context.Context, packet lksdk.DataPacket, options ...lksdk.DataPublishOption) error {
	if packet == nil {
		return errors.New("roomio: data packet is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return ErrRoomIOClosed
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	return r.room.LocalParticipant.PublishDataPacket(packet, options...)
}

func (r *RoomIO) PerformRPC(ctx context.Context, params lksdk.PerformRpcParams) (*string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return nil, ErrRoomIOClosed
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		if params.ResponseTimeout == nil || remaining < *params.ResponseTimeout {
			params.ResponseTimeout = &remaining
		}
	}
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	default:
	}
	result, err := r.room.LocalParticipant.PerformRpc(params)
	if err == nil {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		default:
		}
	}
	return result, err
}

func (r *RoomIO) RegisterRPCMethod(method string, handler lksdk.RpcHandlerCtxFunc) error {
	if method == "" || handler == nil {
		return errors.New("roomio: RPC method and handler are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRoomIOClosed
	}
	if err := r.room.RegisterRpcCtxMethod(method, handler); err != nil {
		return err
	}
	r.rpcMethods[method] = struct{}{}
	return nil
}

func (r *RoomIO) UnregisterRPCMethod(method string) {
	r.mu.Lock()
	r.room.UnregisterRpcMethod(method)
	delete(r.rpcMethods, method)
	r.mu.Unlock()
}

func (r *RoomIO) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	closeCtx, cancel := r.withCloseDeadline(ctx)
	defer cancel()
	ctx = closeCtx
	r.beginClose()
	select {
	case <-r.closeDone:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	r.mu.Lock()
	err := r.closeErr
	r.mu.Unlock()
	return err
}

func (r *RoomIO) beginClose() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		close(r.participantChange)
		startCalled := r.startCalled
		r.mu.Unlock()
		if boolValue(r.input.DeleteRoomOnClose, false) {
			r.startDeleteWorker()
			r.deleteRequests.TrySend(struct{}{})
		}
		r.cancel(ErrRoomIOClosed)
		if !startCalled {
			r.startOnce.Do(func() {
				r.mu.Lock()
				r.startCalled = true
				r.startErr = ErrRoomIOClosed
				close(r.startDone)
				r.mu.Unlock()
			})
		}
		go r.finishClose()
	})
}

func (r *RoomIO) finishClose() {
	// Start may be inside PublishTrack, whose SDK API has no caller context but
	// does have a bounded internal timeout. Waiting here preserves eventual
	// cleanup even when the Close caller's shorter context expires.
	<-r.startDone
	ctx, cancel := context.WithTimeout(context.Background(), r.closeTimeout())
	defer cancel()
	var closeErrors []error
	r.cleanupOnce.Do(func() {
		r.mu.Lock()
		registered := r.textRegistered
		r.textRegistered = false
		methods := make([]string, 0, len(r.rpcMethods))
		for method := range r.rpcMethods {
			methods = append(methods, method)
		}
		slices.Sort(methods)
		r.mu.Unlock()
		if registered {
			r.room.UnregisterTextStreamHandler(TopicChat)
		}
		for _, method := range methods {
			r.room.UnregisterRpcMethod(method)
		}
		if r.session != nil {
			if sameDynamic(r.session.Input().Audio(), r.audioInput) {
				r.session.Input().SetAudio(r.originalInput)
			}
			if sameDynamic(r.session.Output().Audio(), r.audioOutput) {
				r.session.Output().SetAudio(r.originalAudio)
			}
			if sameDynamic(r.session.Output().Transcription(), r.agentTextOutput) {
				r.session.Output().SetTranscription(r.originalText)
			}
		}
		r.participantMu.Lock()
		defer r.participantMu.Unlock()
		if r.ownedAudioInput != nil {
			if err := r.ownedAudioInput.CloseContext(ctx); err != nil {
				r.report(err)
				closeErrors = append(closeErrors, err)
			}
		}
		if r.ownedAudioOutput != nil {
			if err := r.ownedAudioOutput.Close(ctx); err != nil {
				r.report(err)
				closeErrors = append(closeErrors, err)
			}
		}
		if r.synchronizedText != nil {
			r.synchronizedText.Close()
		}
		if r.ownedAgentText != nil {
			if err := r.ownedAgentText.Close(ctx); err != nil {
				r.report(err)
				closeErrors = append(closeErrors, err)
			}
		}
		if r.ownedUserText != nil {
			r.ownedUserText.SetAttached(false)
			r.ownedUserText.OnDetached()
			if err := r.ownedUserText.Close(ctx); err != nil {
				r.report(err)
				closeErrors = append(closeErrors, err)
			}
		}
		_ = r.incoming.Close()
		_ = r.closeRequests.Close()
		_ = r.deleteRequests.Close()
		if r.sessionSub != nil {
			_ = r.sessionSub.Close()
		}
		if r.rtcSub != nil {
			_ = r.rtcSub.Close()
		}
	})
	for _, done := range []<-chan struct{}{r.rtcDone, r.sessionDone, r.closeRequestDone, r.deleteDone} {
		select {
		case <-done:
		case <-ctx.Done():
			closeErrors = append(closeErrors, context.Cause(ctx))
			goto complete
		}
	}

complete:
	r.mu.Lock()
	r.closeErr = errors.Join(closeErrors...)
	r.mu.Unlock()
	close(r.closeDone)
}

func (r *RoomIO) withStartDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	timeout := r.opts.StartTimeout
	if timeout <= 0 {
		timeout = DefaultRoomIOStartTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

func (r *RoomIO) withCloseDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, r.closeTimeout())
}

func (r *RoomIO) closeTimeout() time.Duration {
	if r.opts.CloseTimeout > 0 {
		return r.opts.CloseTimeout
	}
	return DefaultRoomIOCloseTimeout
}

func (r *RoomIO) report(err error) {
	if err == nil {
		return
	}
	if r.opts.OnError != nil {
		if callbackErr := invokeCallback("OnError", func() { r.opts.OnError(err) }); callbackErr != nil {
			r.logger.Error("roomio error callback panicked", "error", callbackErr, "original_error", err)
		}
		return
	}
	r.logger.Error("roomio error", "error", err)
}

func closesSession(reason livekit.DisconnectReason) bool {
	switch reason {
	case livekit.DisconnectReason_CLIENT_INITIATED,
		livekit.DisconnectReason_ROOM_DELETED,
		livekit.DisconnectReason_USER_REJECTED:
		return true
	default:
		return false
	}
}

func sameDynamic(left, right any) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	lv, rv := reflect.ValueOf(left), reflect.ValueOf(right)
	return lv.Type() == rv.Type() && lv.Type().Comparable() && lv.Interface() == rv.Interface()
}
