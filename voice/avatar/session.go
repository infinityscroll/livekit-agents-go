// SPDX-License-Identifier: Apache-2.0

package avatar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/voice"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/twitchtv/twirp"
)

const DefaultWaitForJoinTimeout = 30 * time.Second

var (
	ErrAvatarSessionActive = errors.New("avatar session is already active")
	ErrAvatarJoinTimeout   = errors.New("timed out waiting for avatar participant")
)

type ParticipantRemover func(context.Context, string, string) error

type AvatarSessionOptions struct {
	AvatarIdentity    string
	Provider          string
	ParentContext     context.Context
	ReadyWaiter       ReadyWaiter
	RemoveParticipant ParticipantRemover
	Logger            *slog.Logger
}

type WaitForJoinOptions struct {
	// Timeout defaults to 30 seconds. A negative duration waits indefinitely;
	// zero explicitly performs a non-blocking deadline check.
	Timeout *time.Duration
}

type avatarBaseCloseOperation struct {
	done chan struct{}
	err  error
}

// AvatarSession owns the provider-independent avatar lifecycle. The generic
// parameter matches AgentSession user data and enables typed job shutdown
// registration without a process-global job context.
type AvatarSession[UserData any] struct {
	identity string
	provider string
	parent   context.Context
	waiter   ReadyWaiter
	remover  ParticipantRemover
	logger   *slog.Logger
	metrics  agents.EventEmitter[metrics.Avatar]

	mu                    sync.Mutex
	agent                 *voice.AgentSession[UserData]
	room                  *lksdk.Room
	unsubscribe           func()
	lifetimeCtx           context.Context
	lifetimeCancel        context.CancelCauseFunc
	joinDone              chan struct{}
	joinErr               error
	active                bool
	closed                bool
	activeJob             *agents.JobContext[UserData]
	shutdownRegistrations map[*agents.JobContext[UserData]]struct{}
	closeOperation        *avatarBaseCloseOperation
}

func NewAvatarSession[UserData any](options AvatarSessionOptions) *AvatarSession[UserData] {
	identity := options.AvatarIdentity
	if identity == "" {
		identity = "unknown"
	}
	provider := options.Provider
	if provider == "" {
		provider = "unknown"
	}
	parent := options.ParentContext
	if parent == nil {
		parent = context.Background()
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &AvatarSession[UserData]{
		identity: identity, provider: provider, parent: parent,
		waiter: options.ReadyWaiter, remover: options.RemoveParticipant,
		logger: logger, shutdownRegistrations: make(map[*agents.JobContext[UserData]]struct{}),
	}
}

func (s *AvatarSession[UserData]) AvatarIdentity() string { return s.identity }
func (s *AvatarSession[UserData]) Provider() string       { return s.provider }

func (s *AvatarSession[UserData]) OnMetrics(fn func(metrics.Avatar)) func() {
	return s.metrics.Subscribe(fn)
}

// Start attaches lifecycle and metrics tracking. Provider implementations
// should call this before provisioning and RollbackStart if provisioning fails.
func (s *AvatarSession[UserData]) Start(ctx context.Context, agent *voice.AgentSession[UserData], room *lksdk.Room) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if agent == nil || room == nil {
		return errors.New("avatar session requires an AgentSession and Room")
	}
	s.mu.Lock()
	if s.active || s.closed {
		s.mu.Unlock()
		return ErrAvatarSessionActive
	}
	if agent.Started() && agent.Output().Audio() != nil {
		s.logger.Warn("AvatarSession.Start called after AgentSession.Start; existing audio output will be replaced",
			"audio_output", fmt.Sprintf("%T", agent.Output().Audio()))
	}
	lifetimeCtx, cancel := context.WithCancelCause(s.parent)
	s.agent, s.room = agent, room
	s.lifetimeCtx, s.lifetimeCancel = lifetimeCtx, cancel
	s.joinDone = make(chan struct{})
	s.joinErr = nil
	s.active = true
	s.closed = false
	s.mu.Unlock()

	unsubscribe, err := agent.OnEvent(s.handleAgentEvent, voice.EventSubscriptionOptions{Capacity: 16, DropTelemetry: true})
	if err != nil {
		_ = s.RollbackStart(context.Background())
		return fmt.Errorf("subscribe to avatar session events: %w", err)
	}
	s.mu.Lock()
	if !s.active {
		s.mu.Unlock()
		unsubscribe()
		return ErrAvatarSessionActive
	}
	s.unsubscribe = unsubscribe
	s.mu.Unlock()

	if job, ok := agents.JobFromContext[UserData](ctx); ok {
		s.registerJobShutdown(job)
	}
	if s.identity == "unknown" {
		s.logger.Warn("cannot wait for avatar join; avatar identity is unknown")
		s.finishJoin(nil)
		return nil
	}
	go s.waitForAvatar(lifetimeCtx, room)
	return nil
}

func (s *AvatarSession[UserData]) registerJobShutdown(job *agents.JobContext[UserData]) {
	s.mu.Lock()
	s.activeJob = job
	_, registered := s.shutdownRegistrations[job]
	if !registered {
		s.shutdownRegistrations[job] = struct{}{}
	}
	s.mu.Unlock()
	if registered {
		return
	}
	if err := job.AddShutdownCallback(func(ctx context.Context, _ string) error {
		s.mu.Lock()
		active := s.activeJob == job
		s.mu.Unlock()
		if !active {
			return nil
		}
		return s.Close(ctx)
	}); err != nil {
		s.mu.Lock()
		delete(s.shutdownRegistrations, job)
		s.mu.Unlock()
		s.logger.Warn("failed to register avatar shutdown callback", "error", err)
	}
	s.mu.Lock()
	needsRemover := s.remover == nil
	s.mu.Unlock()
	if needsRemover {
		info := job.Info()
		if info.URL != "" && info.APIKey.Reveal() != "" && info.APISecret.Reveal() != "" {
			client := lksdk.NewRoomServiceClient(info.URL, info.APIKey.Reveal(), info.APISecret.Reveal())
			s.mu.Lock()
			if s.remover == nil {
				s.remover = func(ctx context.Context, roomName, identity string) error {
					_, err := client.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: roomName, Identity: identity})
					return err
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *AvatarSession[UserData]) waitForAvatar(ctx context.Context, room *lksdk.Room) {
	startedAt := time.Now()
	err := error(nil)
	if !sdkDestinationReady(room, s.identity, lksdk.TrackKindVideo) {
		if s.waiter == nil {
			err = ErrAvatarRTCBridgeRequired
		} else {
			err = s.waiter(ctx, room, s.identity, lksdk.TrackKindVideo)
		}
	}
	if err == nil {
		joinedAt := time.Now()
		s.metrics.Emit(metrics.Avatar{
			Timestamp: joinedAt, SessionStarted: startedAt, AvatarJoined: joinedAt,
			Metadata: metrics.Metadata{ModelProvider: s.provider},
		})
	}
	s.finishJoin(err)
}

func (s *AvatarSession[UserData]) finishJoin(err error) {
	s.mu.Lock()
	if s.joinDone != nil {
		s.joinErr = err
		close(s.joinDone)
		s.joinDone = nil
	}
	s.mu.Unlock()
}

func (s *AvatarSession[UserData]) WaitForJoin(ctx context.Context, options ...WaitForJoinOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	done, joinErr := s.joinDone, s.joinErr
	s.mu.Unlock()
	if done == nil {
		return joinErr
	}
	timeout := DefaultWaitForJoinTimeout
	if len(options) != 0 && options[0].Timeout != nil {
		timeout = *options[0].Timeout
	}
	if timeout < 0 {
		select {
		case <-done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	} else {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			return ErrAvatarJoinTimeout
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	s.mu.Lock()
	joinErr = s.joinErr
	s.mu.Unlock()
	return joinErr
}

func (s *AvatarSession[UserData]) handleAgentEvent(event voice.Event) {
	var item llm.ChatItem
	var createdAt time.Time
	switch value := event.(type) {
	case voice.ConversationItemAddedEvent:
		item, createdAt = value.Item, value.Time()
	case *voice.ConversationItemAddedEvent:
		if value != nil {
			item, createdAt = value.Item, value.Time()
		}
	}
	message, ok := item.(*llm.ChatMessage)
	if !ok || message.Role != llm.RoleAssistant || message.Metrics.PlaybackLatency == 0 {
		return
	}
	s.metrics.Emit(metrics.Avatar{
		Timestamp: createdAt, PlaybackLatency: message.Metrics.PlaybackLatency,
		Metadata: metrics.Metadata{ModelProvider: s.provider},
	})
}

// RollbackStart releases only local lifecycle state. It intentionally does not
// remove the participant and is safe after an ambiguous gateway create failure.
func (s *AvatarSession[UserData]) RollbackStart(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if !s.active {
		s.mu.Unlock()
		return nil
	}
	cancel, unsubscribe := s.lifetimeCancel, s.unsubscribe
	done := s.joinDone
	s.active = false
	s.agent, s.room, s.unsubscribe = nil, nil, nil
	s.activeJob = nil
	s.lifetimeCancel, s.lifetimeCtx = nil, nil
	s.mu.Unlock()
	if unsubscribe != nil {
		unsubscribe()
	}
	if cancel != nil {
		cancel(context.Canceled)
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

// Close removes the room participant when credentials/remover are available,
// then always rolls back local state. Not-found is benign; other removal errors
// are logged to match the pinned Python/TypeScript lifecycle contract.
func (s *AvatarSession[UserData]) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed && !s.active {
		s.mu.Unlock()
		return nil
	}
	if operation := s.closeOperation; operation != nil {
		s.mu.Unlock()
		select {
		case <-operation.done:
			return operation.err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	operation := &avatarBaseCloseOperation{done: make(chan struct{})}
	s.closeOperation = operation
	room, remover := s.room, s.remover
	s.mu.Unlock()

	if room != nil && room.ConnectionState() == lksdk.ConnectionStateConnected && remover != nil {
		if err := remover(ctx, room.Name(), s.identity); err != nil && !isTwirpNotFound(err) {
			s.logger.Warn("failed to remove avatar participant", "error", err, "avatar_identity", s.identity)
		}
	}
	err := s.RollbackStart(ctx)
	s.mu.Lock()
	s.closed = true
	operation.err = err
	s.closeOperation = nil
	close(operation.done)
	s.mu.Unlock()
	return err
}

func isTwirpNotFound(err error) bool {
	var twirpError twirp.Error
	return errors.As(err, &twirpError) && twirpError.Code() == twirp.NotFound
}

// AClose is the Python/TypeScript-compatible spelling.
func (s *AvatarSession[UserData]) AClose(ctx context.Context) error { return s.Close(ctx) }
