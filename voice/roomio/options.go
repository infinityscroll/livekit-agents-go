// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"errors"
	"log/slog"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/voice"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	lkmedia "github.com/livekit/server-sdk-go/v2/pkg/media"
)

const (
	DefaultAudioSampleRate       = 24_000
	DefaultAudioChannels         = 1
	DefaultAudioInputCapacity    = 64
	DefaultAudioOutputCapacity   = 256
	DefaultAudioOutputQueue      = 200 * time.Millisecond
	DefaultTextQueueCapacity     = 128
	DefaultIncomingTextCapacity  = 16
	DefaultRoomIOStartTimeout    = 30 * time.Second
	DefaultRoomIOCloseTimeout    = 5 * time.Second
	DefaultTextOperationTimeout  = 5 * time.Second
	DefaultParticipantEventQueue = DefaultRTCEventCapacity

	TopicChat          = "lk.chat"
	TopicTranscription = "lk.transcription"

	AttributePublishOnBehalf         = "lk.publish_on_behalf"
	AttributeAgentState              = "lk.agent.state"
	AttributeTranscriptionTrackID    = "lk.transcribed_track_id"
	AttributeTranscriptionFinal      = "lk.transcription_final"
	AttributeTranscriptionSegmentID  = "lk.segment_id"
	AttributeTranscribedParticipant  = "lk.transcribed_participant_identity"
	DefaultParticipantAudioTrackName = "roomio_audio"
)

var (
	ErrRoomIONotStarted        = errors.New("roomio: not started")
	ErrRoomIOAlreadyStarted    = errors.New("roomio: already started")
	ErrRoomIOClosed            = errors.New("roomio: closed")
	ErrNoLinkedParticipant     = errors.New("roomio: no linked participant")
	ErrIncomingTextOverflow    = errors.New("roomio: incoming text stream queue overflow")
	ErrUnsupportedRawVideo     = errors.New("roomio: raw video is unsupported by server-sdk-go v2.18.1")
	ErrTextReaderNotCancelable = errors.New("roomio: server-sdk-go text readers do not support context cancellation")
)

// Bool is a convenience for optional boolean fields whose nil value selects a
// parity default.
func Bool(value bool) *bool { return &value }

// Session is the narrow AgentSession surface RoomIO consumes. Every
// *voice.AgentSession[T] satisfies it without an adapter.
type Session interface {
	Input() *voice.AgentInput
	Output() *voice.AgentOutput
	Subscribe(voice.EventSubscriptionOptions) (*voice.EventSubscription, error)
	Interrupt(context.Context, bool) error
	GenerateReply(context.Context, voice.GenerateReplyOptions) (*voice.SpeechHandle, error)
	Close(context.Context, ...voice.CloseOptions) error
}

// AudioFrameProcessor is the Go noise-cancellation/filter integration point.
// Process must honor ctx. A processor is caller-owned unless CloseProcessor is
// set on RoomInputOptions.
type AudioFrameProcessor interface {
	Process(context.Context, agents.AudioFrame) (agents.AudioFrame, error)
	Close() error
}

type DecryptorFactory func(*lksdk.RemoteTrackPublication) (lkmedia.Decryptor, error)

// IncomingTextStream is delivered without starting a helper goroutine. The
// pinned SDK's ReadAll has no context or Close method, so callers that read it
// own that blocking operation and should rely on sender completion or room
// teardown. RoomIO itself never leaks a reader goroutine.
type IncomingTextStream struct {
	Reader              *lksdk.TextStreamReader
	ParticipantIdentity string
}

type RoomInputOptions struct {
	AudioSampleRate int
	AudioChannels   int
	AudioCapacity   int

	TextEnabled  *bool
	AudioEnabled *bool
	VideoEnabled *bool

	ParticipantIdentity string
	ParticipantKinds    []lksdk.ParticipantKind
	CloseOnDisconnect   *bool
	DeleteRoomOnClose   *bool

	FrameProcessor AudioFrameProcessor
	CloseProcessor bool
	Decryptor      DecryptorFactory

	// ExternalAudio is attached but never closed by RoomIO.
	ExternalAudio        voice.AudioInput
	IncomingTextCapacity int
}

func DefaultRoomInputOptions() RoomInputOptions {
	return RoomInputOptions{
		AudioSampleRate: DefaultAudioSampleRate,
		AudioChannels:   DefaultAudioChannels,
		AudioCapacity:   DefaultAudioInputCapacity,
		TextEnabled:     Bool(true), AudioEnabled: Bool(true), VideoEnabled: Bool(false),
		ParticipantKinds: []lksdk.ParticipantKind{
			lksdk.ParticipantConnector,
			lksdk.ParticipantSIP,
			lksdk.ParticipantStandard,
		},
		CloseOnDisconnect: Bool(true), DeleteRoomOnClose: Bool(false),
		IncomingTextCapacity: DefaultIncomingTextCapacity,
	}
}

type RoomOutputOptions struct {
	TranscriptionEnabled *bool
	AudioEnabled         *bool
	SyncTranscription    *bool

	AudioSampleRate      int
	AudioChannels        int
	AudioQueue           time.Duration
	AudioCapacity        int
	TextCapacity         int
	TextOperationTimeout time.Duration
	JSONFormat           bool

	AudioPublishOptions lksdk.TrackPublicationOptions
	WaitForSubscription *bool
	Encryptor           lkmedia.Encryptor

	// External sinks are attached but never closed by RoomIO. When Session is
	// present, its already-configured sinks are used as the fallback values.
	ExternalAudio         voice.AudioOutput
	ExternalTranscription voice.TextOutput

	// ExpressiveEnabled is evaluated for each chunk because expressive mode can
	// be latched after RoomIO construction.
	ExpressiveEnabled func() bool
}

func DefaultRoomOutputOptions() RoomOutputOptions {
	return RoomOutputOptions{
		TranscriptionEnabled: Bool(true), AudioEnabled: Bool(true), SyncTranscription: Bool(true),
		AudioSampleRate: DefaultAudioSampleRate, AudioChannels: DefaultAudioChannels,
		AudioQueue: DefaultAudioOutputQueue, AudioCapacity: DefaultAudioOutputCapacity,
		TextCapacity: DefaultTextQueueCapacity, TextOperationTimeout: DefaultTextOperationTimeout,
		WaitForSubscription: Bool(true),
		AudioPublishOptions: lksdk.TrackPublicationOptions{
			Name: DefaultParticipantAudioTrackName, Source: livekit.TrackSource_MICROPHONE,
		},
	}
}

type RoomIOOptions struct {
	Room    *lksdk.Room
	Bridge  *RTCBridge
	Session Session

	ParticipantIdentity string
	Input               *RoomInputOptions
	Output              *RoomOutputOptions

	EventCapacity int
	StartTimeout  time.Duration
	CloseTimeout  time.Duration
	Logger        *slog.Logger

	// Callbacks are panic-isolated and execute serially on bounded RoomIO
	// actors. They must return promptly; blocking one delays later lifecycle
	// events without creating an unbounded callback goroutine.
	OnParticipantLinked func(*lksdk.RemoteParticipant)
	OnCloseRequested    func(voice.CloseReason)
	OnError             func(error)
	// DeleteRoom must honor its context. It is invoked at most once.
	DeleteRoom func(context.Context, string) error
}

func resolveInputOptions(input *RoomInputOptions) (RoomInputOptions, error) {
	resolved := DefaultRoomInputOptions()
	if input == nil {
		return resolved, nil
	}
	if input.AudioSampleRate != 0 {
		resolved.AudioSampleRate = input.AudioSampleRate
	}
	if input.AudioChannels != 0 {
		resolved.AudioChannels = input.AudioChannels
	}
	if input.AudioCapacity != 0 {
		resolved.AudioCapacity = input.AudioCapacity
	}
	if input.TextEnabled != nil {
		resolved.TextEnabled = Bool(*input.TextEnabled)
	}
	if input.AudioEnabled != nil {
		resolved.AudioEnabled = Bool(*input.AudioEnabled)
	}
	if input.VideoEnabled != nil {
		resolved.VideoEnabled = Bool(*input.VideoEnabled)
	}
	if input.CloseOnDisconnect != nil {
		resolved.CloseOnDisconnect = Bool(*input.CloseOnDisconnect)
	}
	if input.DeleteRoomOnClose != nil {
		resolved.DeleteRoomOnClose = Bool(*input.DeleteRoomOnClose)
	}
	if input.ParticipantIdentity != "" {
		resolved.ParticipantIdentity = input.ParticipantIdentity
	}
	if input.ParticipantKinds != nil {
		resolved.ParticipantKinds = append([]lksdk.ParticipantKind(nil), input.ParticipantKinds...)
	}
	resolved.FrameProcessor = input.FrameProcessor
	resolved.CloseProcessor = input.CloseProcessor
	resolved.Decryptor = input.Decryptor
	resolved.ExternalAudio = input.ExternalAudio
	if input.IncomingTextCapacity != 0 {
		resolved.IncomingTextCapacity = input.IncomingTextCapacity
	}
	if resolved.AudioSampleRate <= 0 || resolved.AudioChannels < 1 || resolved.AudioChannels > 2 || resolved.AudioCapacity <= 0 || resolved.IncomingTextCapacity <= 0 {
		return RoomInputOptions{}, errors.New("roomio: invalid input audio format or queue capacity")
	}
	if len(resolved.ParticipantKinds) == 0 {
		return RoomInputOptions{}, errors.New("roomio: ParticipantKinds cannot be empty")
	}
	if *resolved.VideoEnabled {
		return RoomInputOptions{}, ErrUnsupportedRawVideo
	}
	return resolved, nil
}

func resolveOutputOptions(output *RoomOutputOptions) (RoomOutputOptions, error) {
	resolved := DefaultRoomOutputOptions()
	if output == nil {
		return resolved, nil
	}
	if output.TranscriptionEnabled != nil {
		resolved.TranscriptionEnabled = Bool(*output.TranscriptionEnabled)
	}
	if output.AudioEnabled != nil {
		resolved.AudioEnabled = Bool(*output.AudioEnabled)
	}
	if output.SyncTranscription != nil {
		resolved.SyncTranscription = Bool(*output.SyncTranscription)
	}
	if output.AudioSampleRate != 0 {
		resolved.AudioSampleRate = output.AudioSampleRate
	}
	if output.AudioChannels != 0 {
		resolved.AudioChannels = output.AudioChannels
	}
	if output.AudioQueue != 0 {
		resolved.AudioQueue = output.AudioQueue
	}
	if output.AudioCapacity != 0 {
		resolved.AudioCapacity = output.AudioCapacity
	}
	if output.TextCapacity != 0 {
		resolved.TextCapacity = output.TextCapacity
	}
	if output.TextOperationTimeout != 0 {
		resolved.TextOperationTimeout = output.TextOperationTimeout
	}
	resolved.JSONFormat = output.JSONFormat
	if output.AudioPublishOptions != (lksdk.TrackPublicationOptions{}) {
		resolved.AudioPublishOptions = output.AudioPublishOptions
		if resolved.AudioPublishOptions.Name == "" {
			resolved.AudioPublishOptions.Name = DefaultParticipantAudioTrackName
		}
		if resolved.AudioPublishOptions.Source == livekit.TrackSource_UNKNOWN {
			resolved.AudioPublishOptions.Source = livekit.TrackSource_MICROPHONE
		}
	}
	if output.WaitForSubscription != nil {
		resolved.WaitForSubscription = Bool(*output.WaitForSubscription)
	}
	resolved.Encryptor = output.Encryptor
	resolved.ExternalAudio = output.ExternalAudio
	resolved.ExternalTranscription = output.ExternalTranscription
	resolved.ExpressiveEnabled = output.ExpressiveEnabled
	if resolved.AudioSampleRate <= 0 || resolved.AudioChannels < 1 || resolved.AudioChannels > 2 || resolved.AudioQueue < 0 || resolved.AudioCapacity <= 0 || resolved.TextCapacity <= 0 || resolved.TextOperationTimeout <= 0 {
		return RoomOutputOptions{}, errors.New("roomio: invalid output audio format, duration, or queue capacity")
	}
	if resolved.Encryptor != nil && resolved.AudioPublishOptions.Encryption == livekit.Encryption_NONE {
		return RoomOutputOptions{}, errors.New("roomio: encrypted audio requires a non-NONE publication encryption type")
	}
	return resolved, nil
}

func boolValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}
