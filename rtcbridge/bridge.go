// SPDX-License-Identifier: Apache-2.0

// Package rtcbridge multiplexes one LiveKit RoomCallback into bounded,
// context-first subscriptions. It is dependency-neutral so the worker can
// install it while constructing a Room, before higher-level RoomIO exists.
package rtcbridge

import (
	"context"
	"errors"
	"maps"
	"runtime/debug"
	"sync"

	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"
)

const DefaultRTCEventCapacity = 256

var (
	ErrRTCBridgeClosed  = errors.New("roomio: RTC bridge is closed")
	ErrRTCEventOverflow = errors.New("roomio: RTC event queue overflow")
)

// CallbackPanicError reports a panic recovered at an application callback
// boundary. A user callback can therefore not take down an RTC or RoomIO
// lifecycle goroutine.
type CallbackPanicError struct {
	Callback string
	Panic    any
	Stack    []byte
}

func (e *CallbackPanicError) Error() string {
	if e.Callback == "" {
		return "roomio: callback panicked"
	}
	return "roomio: " + e.Callback + " callback panicked"
}

// RTCEventType is the lossless control-event vocabulary emitted by RTCBridge.
type RTCEventType string

const (
	RTCEventParticipantConnected         RTCEventType = "participant_connected"
	RTCEventParticipantDisconnected      RTCEventType = "participant_disconnected"
	RTCEventParticipantAttributesChanged RTCEventType = "participant_attributes_changed"
	RTCEventTrackPublished               RTCEventType = "track_published"
	RTCEventTrackUnpublished             RTCEventType = "track_unpublished"
	RTCEventTrackSubscribed              RTCEventType = "track_subscribed"
	RTCEventTrackUnsubscribed            RTCEventType = "track_unsubscribed"
	RTCEventLocalTrackPublished          RTCEventType = "local_track_published"
	RTCEventLocalTrackUnpublished        RTCEventType = "local_track_unpublished"
	RTCEventLocalTrackSubscribed         RTCEventType = "local_track_subscribed"
	RTCEventReconnecting                 RTCEventType = "reconnecting"
	RTCEventReconnected                  RTCEventType = "reconnected"
	RTCEventDisconnected                 RTCEventType = "disconnected"
	RTCEventDataPacket                   RTCEventType = "data_packet"
	RTCEventTranscription                RTCEventType = "transcription"
)

// RTCEvent contains only SDK-owned handles whose public methods are safe to
// call after the callback returns. TranscriptionSegments and data payloads are
// defensively copied by the bridge.
type RTCEvent struct {
	Type RTCEventType

	Participant       *lksdk.RemoteParticipant
	ChangedAttributes map[string]string
	Track             *webrtc.TrackRemote
	Publication       *lksdk.RemoteTrackPublication
	LocalPublication  *lksdk.LocalTrackPublication
	DisconnectReason  livekit.DisconnectReason
	DataPacket        lksdk.DataPacket
	DataReceiveParams lksdk.DataReceiveParams

	TranscriptionSegments  []*lksdk.TranscriptionSegment
	TranscribedParticipant lksdk.Participant
	TranscribedPublication lksdk.TrackPublication
}

type RTCBridgeOptions struct {
	// EventCapacity is the default capacity of each subscription. A subscriber
	// that cannot keep up is failed visibly with ErrRTCEventOverflow; RTC
	// callbacks are never blocked and lifecycle events are never silently lost.
	EventCapacity   int
	OnCallbackPanic func(error)
}

// RTCSubscription is a single-consumer, explicitly closable event stream.
type RTCSubscription struct {
	bridge *RTCBridge
	id     uint64
	queue  *stream.Channel[RTCEvent]
	events map[RTCEventType]struct{}
	once   sync.Once
}

func (s *RTCSubscription) Recv(ctx context.Context) (RTCEvent, error) {
	if s == nil || s.queue == nil {
		return RTCEvent{}, ErrRTCBridgeClosed
	}
	return s.queue.Recv(ctx)
}

func (s *RTCSubscription) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		if s.bridge != nil {
			s.bridge.remove(s.id)
		} else if s.queue != nil {
			_ = s.queue.Close()
		}
	})
	return nil
}

// RTCBridge multiplexes one RoomCallback into bounded RoomIO subscriptions.
// The callback returned by Callback must be installed when the room is created;
// server-sdk-go v2.18.1 does not expose post-construction callback registration.
type RTCBridge struct {
	callback *lksdk.RoomCallback
	capacity int
	onPanic  func(error)

	mu     sync.RWMutex
	subs   map[uint64]*RTCSubscription
	nextID uint64
	closed bool
	once   sync.Once
}

// NewRTCBridge composes base callbacks with a nonblocking bounded event bridge.
// base may be nil. Existing callback behavior is preserved, with panics isolated
// and reported through OnCallbackPanic.
func NewRTCBridge(base *lksdk.RoomCallback, options RTCBridgeOptions) *RTCBridge {
	capacity := options.EventCapacity
	if capacity <= 0 {
		capacity = DefaultRTCEventCapacity
	}
	b := &RTCBridge{
		capacity: capacity,
		onPanic:  options.OnCallbackPanic,
		subs:     make(map[uint64]*RTCSubscription),
	}
	callback := lksdk.NewRoomCallback()
	callback.Merge(base)
	b.install(callback)
	b.callback = callback
	return b
}

// NewRoom creates a server-SDK Room with the bridge callback installed. RoomIO
// never disconnects this room; the caller retains room and E2EE ownership.
func NewRoom(base *lksdk.RoomCallback, options RTCBridgeOptions) (*lksdk.Room, *RTCBridge) {
	bridge := NewRTCBridge(base, options)
	return lksdk.NewRoom(bridge.Callback()), bridge
}

func (b *RTCBridge) Callback() *lksdk.RoomCallback {
	if b == nil {
		return nil
	}
	return b.callback
}

func (b *RTCBridge) Subscribe(capacity int) (*RTCSubscription, error) {
	return b.subscribe(capacity, nil)
}

// SubscribeTypes limits a subscription to the listed event types. Internal
// media actors use this to avoid allocating and dispatching high-volume data or
// transcription events they do not consume. With no event types it is
// equivalent to Subscribe.
func (b *RTCBridge) SubscribeTypes(capacity int, events ...RTCEventType) (*RTCSubscription, error) {
	if len(events) == 0 {
		return b.subscribe(capacity, nil)
	}
	filter := make(map[RTCEventType]struct{}, len(events))
	for _, event := range events {
		filter[event] = struct{}{}
	}
	return b.subscribe(capacity, filter)
}

func (b *RTCBridge) subscribe(capacity int, events map[RTCEventType]struct{}) (*RTCSubscription, error) {
	if b == nil {
		return nil, ErrRTCBridgeClosed
	}
	if capacity <= 0 {
		capacity = b.capacity
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrRTCBridgeClosed
	}
	b.nextID++
	sub := &RTCSubscription{bridge: b, id: b.nextID, queue: stream.NewChannel[RTCEvent](capacity), events: events}
	b.subs[sub.id] = sub
	return sub, nil
}

func (b *RTCBridge) Close() error {
	if b == nil {
		return nil
	}
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true
		subs := b.subs
		b.subs = make(map[uint64]*RTCSubscription)
		b.mu.Unlock()
		for _, sub := range subs {
			_ = sub.queue.Abort(ErrRTCBridgeClosed)
		}
	})
	return nil
}

func (b *RTCBridge) remove(id uint64) {
	b.mu.Lock()
	sub := b.subs[id]
	delete(b.subs, id)
	b.mu.Unlock()
	if sub != nil {
		_ = sub.queue.Close()
	}
}

func (b *RTCBridge) publish(event RTCEvent) {
	b.mu.RLock()
	subs := make([]*RTCSubscription, 0, len(b.subs))
	for _, sub := range b.subs {
		subs = append(subs, sub)
	}
	b.mu.RUnlock()
	for _, sub := range subs {
		if sub.events != nil {
			if _, accepted := sub.events[event.Type]; !accepted {
				continue
			}
		}
		if !sub.queue.TrySend(event) {
			// A full control queue is fatal rather than a silent lifecycle loss.
			_ = sub.queue.Abort(ErrRTCEventOverflow)
		}
	}
}

func (b *RTCBridge) invoke(callback func()) {
	if err := invokeCallback("RTC", callback); err != nil && b.onPanic != nil {
		b.reportPanic(err)
	}
}

func (b *RTCBridge) reportPanic(err error) {
	defer func() { _ = recover() }()
	b.onPanic(err)
}

func invokeCallback(name string, callback func()) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newCallbackPanicError(name, recovered)
		}
	}()
	callback()
	return nil
}

func newCallbackPanicError(name string, recovered any) *CallbackPanicError {
	return &CallbackPanicError{Callback: name, Panic: recovered, Stack: debug.Stack()}
}

func (b *RTCBridge) install(cb *lksdk.RoomCallback) {
	// The server SDK invokes legacy/discovery callbacks on RTC goroutines too.
	// Isolate every pass-through callback, not only the events RoomIO consumes.
	disconnectedLegacy := cb.OnDisconnected
	cb.OnDisconnected = func() { b.invoke(disconnectedLegacy) }
	activeSpeakers := cb.OnActiveSpeakersChanged
	cb.OnActiveSpeakersChanged = func(participants []lksdk.Participant) {
		b.invoke(func() { activeSpeakers(participants) })
	}
	roomMetadata := cb.OnRoomMetadataChanged
	cb.OnRoomMetadataChanged = func(metadata string) { b.invoke(func() { roomMetadata(metadata) }) }
	recordingStatus := cb.OnRecordingStatusChanged
	cb.OnRecordingStatusChanged = func(recording bool) { b.invoke(func() { recordingStatus(recording) }) }
	roomMoved := cb.OnRoomMoved
	cb.OnRoomMoved = func(roomName, token string) { b.invoke(func() { roomMoved(roomName, token) }) }
	roomMovedWithSID := cb.OnRoomMovedWithSID
	cb.OnRoomMovedWithSID = func(roomName, roomSID, token string) {
		b.invoke(func() { roomMovedWithSID(roomName, roomSID, token) })
	}
	trackMuted := cb.OnTrackMuted
	cb.OnTrackMuted = func(publication lksdk.TrackPublication, participant lksdk.Participant) {
		b.invoke(func() { trackMuted(publication, participant) })
	}
	trackUnmuted := cb.OnTrackUnmuted
	cb.OnTrackUnmuted = func(publication lksdk.TrackPublication, participant lksdk.Participant) {
		b.invoke(func() { trackUnmuted(publication, participant) })
	}
	metadataChanged := cb.OnMetadataChanged
	cb.OnMetadataChanged = func(oldMetadata string, participant lksdk.Participant) {
		b.invoke(func() { metadataChanged(oldMetadata, participant) })
	}
	attributesChanged := cb.OnAttributesChanged
	cb.OnAttributesChanged = func(changed map[string]string, participant lksdk.Participant) {
		b.invoke(func() { attributesChanged(changed, participant) })
		if remote, ok := participant.(*lksdk.RemoteParticipant); ok {
			b.publish(RTCEvent{
				Type: RTCEventParticipantAttributesChanged, Participant: remote,
				ChangedAttributes: maps.Clone(changed),
			})
		}
	}
	speakingChanged := cb.OnIsSpeakingChanged
	cb.OnIsSpeakingChanged = func(participant lksdk.Participant) {
		b.invoke(func() { speakingChanged(participant) })
	}
	qualityChanged := cb.OnConnectionQualityChanged
	cb.OnConnectionQualityChanged = func(update *livekit.ConnectionQualityInfo, participant lksdk.Participant) {
		b.invoke(func() { qualityChanged(update, participant) })
	}
	subscriptionFailed := cb.OnTrackSubscriptionFailed
	cb.OnTrackSubscriptionFailed = func(sid string, participant *lksdk.RemoteParticipant) {
		b.invoke(func() { subscriptionFailed(sid, participant) })
	}
	dataReceived := cb.OnDataReceived
	cb.OnDataReceived = func(data []byte, params lksdk.DataReceiveParams) {
		b.invoke(func() { dataReceived(data, params) })
	}

	participantConnected := cb.OnParticipantConnected
	cb.OnParticipantConnected = func(participant *lksdk.RemoteParticipant) {
		b.invoke(func() { participantConnected(participant) })
		b.publish(RTCEvent{Type: RTCEventParticipantConnected, Participant: participant})
	}
	participantDisconnected := cb.OnParticipantDisconnected
	cb.OnParticipantDisconnected = func(participant *lksdk.RemoteParticipant) {
		b.invoke(func() { participantDisconnected(participant) })
		var reason livekit.DisconnectReason
		if participant != nil {
			reason = participant.DisconnectReason()
		}
		b.publish(RTCEvent{Type: RTCEventParticipantDisconnected, Participant: participant, DisconnectReason: reason})
	}
	trackPublished := cb.OnTrackPublished
	cb.OnTrackPublished = func(publication *lksdk.RemoteTrackPublication, participant *lksdk.RemoteParticipant) {
		b.invoke(func() { trackPublished(publication, participant) })
		b.publish(RTCEvent{Type: RTCEventTrackPublished, Participant: participant, Publication: publication})
	}
	trackUnpublished := cb.OnTrackUnpublished
	cb.OnTrackUnpublished = func(publication *lksdk.RemoteTrackPublication, participant *lksdk.RemoteParticipant) {
		b.invoke(func() { trackUnpublished(publication, participant) })
		b.publish(RTCEvent{Type: RTCEventTrackUnpublished, Participant: participant, Publication: publication})
	}
	trackSubscribed := cb.OnTrackSubscribed
	cb.OnTrackSubscribed = func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, participant *lksdk.RemoteParticipant) {
		b.invoke(func() { trackSubscribed(track, publication, participant) })
		b.publish(RTCEvent{Type: RTCEventTrackSubscribed, Participant: participant, Track: track, Publication: publication})
	}
	trackUnsubscribed := cb.OnTrackUnsubscribed
	cb.OnTrackUnsubscribed = func(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication, participant *lksdk.RemoteParticipant) {
		b.invoke(func() { trackUnsubscribed(track, publication, participant) })
		b.publish(RTCEvent{Type: RTCEventTrackUnsubscribed, Participant: participant, Track: track, Publication: publication})
	}
	localPublished := cb.OnLocalTrackPublished
	cb.OnLocalTrackPublished = func(publication *lksdk.LocalTrackPublication, participant *lksdk.LocalParticipant) {
		b.invoke(func() { localPublished(publication, participant) })
		b.publish(RTCEvent{Type: RTCEventLocalTrackPublished, LocalPublication: publication})
	}
	localUnpublished := cb.OnLocalTrackUnpublished
	cb.OnLocalTrackUnpublished = func(publication *lksdk.LocalTrackPublication, participant *lksdk.LocalParticipant) {
		b.invoke(func() { localUnpublished(publication, participant) })
		b.publish(RTCEvent{Type: RTCEventLocalTrackUnpublished, LocalPublication: publication})
	}
	localSubscribed := cb.OnLocalTrackSubscribed
	cb.OnLocalTrackSubscribed = func(publication *lksdk.LocalTrackPublication, participant *lksdk.LocalParticipant) {
		b.invoke(func() { localSubscribed(publication, participant) })
		b.publish(RTCEvent{Type: RTCEventLocalTrackSubscribed, LocalPublication: publication})
	}
	reconnecting := cb.OnReconnecting
	cb.OnReconnecting = func() {
		b.invoke(reconnecting)
		b.publish(RTCEvent{Type: RTCEventReconnecting})
	}
	reconnected := cb.OnReconnected
	cb.OnReconnected = func() {
		b.invoke(reconnected)
		b.publish(RTCEvent{Type: RTCEventReconnected})
	}
	disconnected := cb.OnDisconnectedWithReason
	cb.OnDisconnectedWithReason = func(reason lksdk.DisconnectionReason) {
		b.invoke(func() { disconnected(reason) })
		// Room.DisconnectReason exposes the raw value, but the callback does not.
		b.publish(RTCEvent{Type: RTCEventDisconnected})
	}
	dataPacket := cb.OnDataPacket
	cb.OnDataPacket = func(packet lksdk.DataPacket, params lksdk.DataReceiveParams) {
		b.invoke(func() { dataPacket(packet, params) })
		b.publish(RTCEvent{Type: RTCEventDataPacket, DataPacket: cloneDataPacket(packet), DataReceiveParams: params})
	}
	transcription := cb.OnTranscriptionReceived
	cb.OnTranscriptionReceived = func(segments []*lksdk.TranscriptionSegment, participant lksdk.Participant, publication lksdk.TrackPublication) {
		b.invoke(func() { transcription(segments, participant, publication) })
		b.publish(RTCEvent{
			Type: RTCEventTranscription, TranscriptionSegments: cloneTranscriptionSegments(segments),
			TranscribedParticipant: participant, TranscribedPublication: publication,
		})
	}
}

func cloneTranscriptionSegments(source []*lksdk.TranscriptionSegment) []*lksdk.TranscriptionSegment {
	result := make([]*lksdk.TranscriptionSegment, len(source))
	for index, segment := range source {
		if segment != nil {
			copySegment := *segment
			result[index] = &copySegment
		}
	}
	return result
}

func cloneDataPacket(packet lksdk.DataPacket) lksdk.DataPacket {
	switch value := packet.(type) {
	case *lksdk.UserDataPacket:
		if value == nil {
			return (*lksdk.UserDataPacket)(nil)
		}
		return &lksdk.UserDataPacket{Payload: append([]byte(nil), value.Payload...), Topic: value.Topic}
	case *livekit.SipDTMF:
		if value == nil {
			return (*livekit.SipDTMF)(nil)
		}
		return proto.Clone(value).(*livekit.SipDTMF)
	case *livekit.ChatMessage:
		if value == nil {
			return (*livekit.ChatMessage)(nil)
		}
		return proto.Clone(value).(*livekit.ChatMessage)
	default:
		return packet
	}
}
