// SPDX-License-Identifier: Apache-2.0

//go:build cgo

package backgroundaudio

import (
	"context"
	"errors"
	"fmt"
	"sync"

	agents "github.com/infinityscroll/livekit-agents-go"
	mediabase "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/livekit"
	protolg "github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
	lkmedia "github.com/livekit/server-sdk-go/v2/pkg/media"
)

type LiveKitPublisherOptions struct {
	TrackPublishOptions lksdk.TrackPublicationOptions
	Encryptor           AudioEncryptor
}

// LiveKitPublisher adapts a connected server-sdk-go Room to Publisher. The
// room remains caller-owned. server-sdk-go's PublishTrack/UnpublishTrack calls
// do not accept contexts; this adapter checks cancellation immediately before
// and after those bounded SDK operations and never creates a potentially
// leaking wrapper goroutine.
type LiveKitPublisher struct {
	room    *lksdk.Room
	options LiveKitPublisherOptions
}

func NewLiveKitPublisher(room *lksdk.Room, options LiveKitPublisherOptions) (*LiveKitPublisher, error) {
	if room == nil || room.LocalParticipant == nil {
		return nil, errors.New("backgroundaudio: connected LiveKit room with local participant is required")
	}
	return &LiveKitPublisher{room: room, options: options}, nil
}

func (p *LiveKitPublisher) Publish(ctx context.Context, request PublishRequest) (FrameSink, Publication, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return nil, Publication{}, err
	}
	if request.SampleRate != MixerSampleRate || request.Channels != MixerChannels || request.Name == "" {
		return nil, Publication{}, fmt.Errorf("backgroundaudio: invalid publish request: %+v", request)
	}
	var trackOptions []lkmedia.PCMLocalTrackOption
	if p.options.Encryptor != nil {
		trackOptions = append(trackOptions, lkmedia.WithEncryptor(p.options.Encryptor))
	}
	track, err := lkmedia.NewPCMLocalTrack(request.SampleRate, request.Channels, protolg.GetLogger(), trackOptions...)
	if err != nil {
		return nil, Publication{}, err
	}
	options := p.options.TrackPublishOptions
	options.Name = request.Name
	if options.Source == livekit.TrackSource_UNKNOWN {
		options.Source = livekit.TrackSource_MICROPHONE
	}
	publication, err := p.room.LocalParticipant.PublishTrack(track, &options)
	if err != nil {
		_ = track.Close()
		return nil, Publication{}, err
	}
	result := Publication{SID: publication.SID(), Name: publication.Name()}
	if err := context.Cause(ctx); err != nil {
		_ = p.room.LocalParticipant.UnpublishTrack(result.SID)
		_ = track.Close()
		return nil, Publication{}, err
	}
	return &liveKitFrameSink{track: track}, result, nil
}

func (p *LiveKitPublisher) CurrentPublication(ctx context.Context, name string) (Publication, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return Publication{}, false, err
	}
	for _, publication := range p.room.LocalParticipant.TrackPublications() {
		if publication.Name() == name {
			return Publication{SID: publication.SID(), Name: publication.Name()}, true, nil
		}
	}
	return Publication{}, false, nil
}

func (p *LiveKitPublisher) Unpublish(ctx context.Context, sid string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if sid == "" {
		return nil
	}
	err := p.room.LocalParticipant.UnpublishTrack(sid)
	if err != nil {
		return err
	}
	return context.Cause(ctx)
}

// StartRoom is the direct room-oriented convenience equivalent of the
// TypeScript start({room, agentSession, trackPublishOptions}) call.
func (p *BackgroundAudioPlayer) StartRoom(ctx context.Context, room *lksdk.Room, session AgentSession, options LiveKitPublisherOptions) error {
	publisher, err := NewLiveKitPublisher(room, options)
	if err != nil {
		return err
	}
	return p.Start(ctx, BackgroundAudioStartOptions{Publisher: publisher, AgentSession: session})
}

type liveKitFrameSink struct {
	track *lkmedia.PCMLocalTrack
	once  sync.Once
	err   error
}

// immediateCapture marks that WriteSample copies PCM before returning, allowing
// the mixer to reuse its output block and avoid one 9.6 KiB allocation per tick.
func (*liveKitFrameSink) immediateCapture() {}

func (s *liveKitFrameSink) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if frame.SampleRate != MixerSampleRate || frame.Channels != MixerChannels || frame.SamplesPerChannel != len(frame.Data) {
		return fmt.Errorf("%w: LiveKit sink requires 48 kHz mono PCM16", agents.ErrInvalidAudioFormat)
	}
	if err := s.track.WriteSample(mediabase.PCM16Sample(frame.Data)); err != nil {
		return err
	}
	return context.Cause(ctx)
}

func (s *liveKitFrameSink) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	s.once.Do(func() { s.err = s.track.Close() })
	return s.err
}

var _ Publisher = (*LiveKitPublisher)(nil)
var _ FrameSink = (*liveKitFrameSink)(nil)
