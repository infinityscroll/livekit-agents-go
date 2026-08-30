// SPDX-License-Identifier: Apache-2.0

package avatar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/infinityscroll/livekit-agents-go/inference"
	"github.com/infinityscroll/livekit-agents-go/voice"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

type InferenceSessionOptions struct {
	Gateway           inference.AvatarSessionOptions
	ParentContext     context.Context
	ReadyWaiter       ReadyWaiter
	RemoveParticipant ParticipantRemover
	Logger            *slog.Logger
}

type InferenceSessionStartOptions struct {
	LiveKitURL string
}

type inferenceCloseOperation struct {
	done chan struct{}
	err  error
}

// InferenceSession is the one-step voice adapter around the cycle-free
// inference.AvatarSession gateway lifecycle.
type InferenceSession[UserData any] struct {
	gateway *inference.AvatarSession
	base    *AvatarSession[UserData]
	waiter  ReadyWaiter
	logger  *slog.Logger

	mu        sync.Mutex
	claimed   bool
	startDone chan struct{}
	created   bool
	closed    bool
	output    *DataStreamAudioOutput
	closeOp   *inferenceCloseOperation
}

func NewInferenceSession[UserData any](options InferenceSessionOptions) (*InferenceSession[UserData], error) {
	gateway, err := inference.NewAvatarSession(options.Gateway)
	if err != nil {
		return nil, err
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	base := NewAvatarSession[UserData](AvatarSessionOptions{
		AvatarIdentity: gateway.AvatarIdentity(), Provider: gateway.Provider(),
		ParentContext: options.ParentContext, ReadyWaiter: options.ReadyWaiter,
		RemoveParticipant: options.RemoveParticipant, Logger: logger,
	})
	return &InferenceSession[UserData]{gateway: gateway, base: base, waiter: options.ReadyWaiter, logger: logger}, nil
}

func (s *InferenceSession[UserData]) AvatarIdentity() string            { return s.gateway.AvatarIdentity() }
func (s *InferenceSession[UserData]) Provider() string                  { return s.gateway.Provider() }
func (s *InferenceSession[UserData]) Gateway() *inference.AvatarSession { return s.gateway }
func (s *InferenceSession[UserData]) Base() *AvatarSession[UserData]    { return s.base }

func (s *InferenceSession[UserData]) AudioOutput() *DataStreamAudioOutput {
	s.mu.Lock()
	output := s.output
	s.mu.Unlock()
	return output
}

func (s *InferenceSession[UserData]) Start(ctx context.Context, agent *voice.AgentSession[UserData], room *lksdk.Room, options ...InferenceSessionStartOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.claimed || s.created || s.closed {
		s.mu.Unlock()
		return inference.ErrAvatarSessionAlreadyStarted
	}
	s.claimed = true
	s.startDone = make(chan struct{})
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.claimed = false
		if s.startDone != nil {
			close(s.startDone)
			s.startDone = nil
		}
		s.mu.Unlock()
	}()

	if room == nil || room.LocalParticipant == nil || room.ConnectionState() != lksdk.ConnectionStateConnected {
		return errors.New("inference avatar requires a connected Room")
	}
	if err := s.base.Start(ctx, agent, room); err != nil {
		return err
	}
	liveKitURL := os.Getenv("LIVEKIT_URL")
	if len(options) != 0 && options[0].LiveKitURL != "" {
		liveKitURL = options[0].LiveKitURL
	}
	info, err := s.gateway.Start(ctx, inference.AvatarSessionStartOptions{
		LiveKitURL: liveKitURL, RoomName: room.Name(), RoomSID: room.SID(),
		AgentIdentity: room.LocalParticipant.Identity(),
	})
	if err != nil {
		_ = s.base.RollbackStart(context.Background())
		return err
	}
	s.mu.Lock()
	s.created = true
	s.mu.Unlock()
	if info.AvatarIdentity != "" && info.AvatarIdentity != s.AvatarIdentity() {
		s.logger.Warn("avatar gateway minted a different participant identity",
			"requested_identity", s.AvatarIdentity(), "minted_identity", info.AvatarIdentity)
	}
	output, err := NewDataStreamAudioOutput(DataStreamAudioOutputOptions{
		Room: room, ReadyWaiter: s.waiter, DestinationIdentity: s.AvatarIdentity(),
		SampleRate: info.SampleRate, WaitRemoteTrack: lksdk.TrackKindVideo,
		WaitPlaybackStart: true, DisableClearTimeout: true,
	})
	if err != nil {
		return fmt.Errorf("initialize inference avatar audio output after gateway create (Close must still be called): %w", err)
	}
	s.mu.Lock()
	s.output = output
	s.mu.Unlock()
	agent.Output().SetAudio(output)
	return nil
}

func (s *InferenceSession[UserData]) WaitForJoin(ctx context.Context, options ...WaitForJoinOptions) error {
	return s.base.WaitForJoin(ctx, options...)
}

func (s *InferenceSession[UserData]) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	if s.claimed {
		done := s.startDone
		s.mu.Unlock()
		select {
		case <-done:
			return s.Close(ctx)
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	if operation := s.closeOp; operation != nil {
		s.mu.Unlock()
		select {
		case <-operation.done:
			return operation.err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	operation := &inferenceCloseOperation{done: make(chan struct{})}
	s.closeOp = operation
	output := s.output
	s.mu.Unlock()

	var outputErr error
	if output != nil {
		outputErr = output.Close(ctx)
	}
	gatewayErr := s.gateway.Close(ctx)
	baseErr := s.base.Close(ctx)
	err := errors.Join(gatewayErr, outputErr, baseErr)
	s.mu.Lock()
	operation.err = err
	if err == nil {
		s.closed = true
	}
	s.closeOp = nil
	close(operation.done)
	s.mu.Unlock()
	return err
}

func (s *InferenceSession[UserData]) AClose(ctx context.Context) error { return s.Close(ctx) }
