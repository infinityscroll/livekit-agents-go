// SPDX-License-Identifier: Apache-2.0

//go:build !cgo

package backgroundaudio

import (
	"context"

	lksdk "github.com/livekit/server-sdk-go/v2"
)

// LiveKitPublisherOptions is retained in no-cgo builds so applications can
// cross-compile code that conditionally configures the adapter.
type LiveKitPublisherOptions struct {
	TrackPublishOptions lksdk.TrackPublicationOptions
	Encryptor           AudioEncryptor
}

type LiveKitPublisher struct{}

func NewLiveKitPublisher(room *lksdk.Room, options LiveKitPublisherOptions) (*LiveKitPublisher, error) {
	return nil, ErrLiveKitMediaUnavailable
}

func (p *LiveKitPublisher) Publish(ctx context.Context, request PublishRequest) (FrameSink, Publication, error) {
	return nil, Publication{}, ErrLiveKitMediaUnavailable
}

func (p *LiveKitPublisher) CurrentPublication(ctx context.Context, name string) (Publication, bool, error) {
	return Publication{}, false, ErrLiveKitMediaUnavailable
}

func (p *LiveKitPublisher) Unpublish(ctx context.Context, sid string) error {
	return ErrLiveKitMediaUnavailable
}

func (p *BackgroundAudioPlayer) StartRoom(ctx context.Context, room *lksdk.Room, session AgentSession, options LiveKitPublisherOptions) error {
	return ErrLiveKitMediaUnavailable
}

var _ Publisher = (*LiveKitPublisher)(nil)
