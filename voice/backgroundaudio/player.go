// SPDX-License-Identifier: Apache-2.0

package backgroundaudio

import (
	"context"
	"errors"
	"io"
	"math"
	rand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/voice"
)

const BackgroundAudioTrackName = "background_audio"

var (
	ErrNotStarted              = errors.New("backgroundaudio: player is not started")
	ErrAlreadyStarted          = errors.New("backgroundaudio: player is already started")
	ErrClosed                  = errors.New("backgroundaudio: player is closed")
	ErrTooManyStreams          = errors.New("backgroundaudio: maximum concurrent streams reached")
	ErrLiveKitMediaUnavailable = errors.New("backgroundaudio: LiveKit PCM track publishing requires cgo Opus support")
)

// FrameSink consumes fixed-format mixer frames. voice.AudioOutput satisfies
// this interface directly. CaptureFrame must honor ctx and may retain the frame
// after returning.
type FrameSink interface {
	CaptureFrame(context.Context, agents.AudioFrame) error
}

// ClosableFrameSink is implemented by publisher-owned sinks that need explicit
// teardown. A plain voice.AudioOutput is caller-owned and is not closed.
type ClosableFrameSink interface {
	FrameSink
	Close(context.Context) error
}

// immediateFrameSink is deliberately private: only sinks whose implementation
// is audited to consume or copy the frame before returning may enable mixer
// buffer reuse.
type immediateFrameSink interface {
	FrameSink
	immediateCapture()
}

type Publication struct {
	SID  string
	Name string
}

type PublishRequest struct {
	Name       string
	SampleRate int
	Channels   int
}

// AudioEncryptor matches server-sdk-go media encryptors without importing its
// cgo-only media package into no-cgo builds.
type AudioEncryptor interface {
	EncryptSample([]byte) ([]byte, error)
}

// Publisher is the narrow room/track surface consumed by the player. Use
// NewLiveKitPublisher for server-sdk-go rooms or provide an application adapter.
type Publisher interface {
	Publish(context.Context, PublishRequest) (FrameSink, Publication, error)
	CurrentPublication(context.Context, string) (Publication, bool, error)
	Unpublish(context.Context, string) error
}

// AgentSession is the narrow state-event surface consumed by the player.
// Every *voice.AgentSession[T] satisfies it.
type AgentSession interface {
	Subscribe(voice.EventSubscriptionOptions) (*voice.EventSubscription, error)
}

type BackgroundAudioPlayerOptions struct {
	AmbientSound  Sound
	ThinkingSound Sound

	StreamTimeout    time.Duration
	OperationTimeout time.Duration
	// BlockDuration defaults to the agents-js mixer block of 100 ms. It is
	// exposed primarily for deterministic testing and specialized transports.
	BlockDuration  time.Duration
	BufferDuration time.Duration
	TaskTimeout    time.Duration
	MaxStreams     int
	FFmpegPath     string

	BuiltinResolver BuiltinResolver
	// Random returns a value in [0,1). The package-level rand/v2 generator is
	// used by default and no generator is initialized during construction.
	Random  func() float64
	OnError func(error)
}

type resolvedPlayerOptions struct {
	ambientSound, thinkingSound Sound
	streamTimeout               time.Duration
	operationTimeout            time.Duration
	blockDuration               time.Duration
	bufferDuration              time.Duration
	taskTimeout                 time.Duration
	maxStreams                  int
	ffmpegPath                  string
	builtinResolver             BuiltinResolver
	random                      func() float64
	onError                     func(error)
}

func resolvePlayerOptions(options BackgroundAudioPlayerOptions) (resolvedPlayerOptions, error) {
	resolved := resolvedPlayerOptions{
		ambientSound: options.AmbientSound, thinkingSound: options.ThinkingSound,
		streamTimeout: options.StreamTimeout, operationTimeout: options.OperationTimeout,
		blockDuration: options.BlockDuration, bufferDuration: options.BufferDuration,
		taskTimeout: options.TaskTimeout, maxStreams: options.MaxStreams,
		ffmpegPath: options.FFmpegPath, builtinResolver: options.BuiltinResolver,
		random: options.Random, onError: options.OnError,
	}
	if resolved.streamTimeout == 0 {
		resolved.streamTimeout = DefaultStreamTimeout
	}
	if resolved.operationTimeout == 0 {
		resolved.operationTimeout = DefaultOperationTimeout
	}
	if resolved.blockDuration == 0 {
		resolved.blockDuration = DefaultBlockDuration
	}
	if resolved.bufferDuration == 0 {
		resolved.bufferDuration = DefaultBufferDuration
	}
	if resolved.taskTimeout == 0 {
		resolved.taskTimeout = DefaultTaskTimeout
	}
	if resolved.maxStreams == 0 {
		resolved.maxStreams = DefaultMaxStreams
	}
	if resolved.random == nil {
		resolved.random = rand.Float64
	}
	if resolved.streamTimeout < 0 || resolved.operationTimeout < 0 || resolved.blockDuration <= 0 || resolved.bufferDuration <= 0 || resolved.taskTimeout < 0 || resolved.maxStreams < 1 {
		return resolvedPlayerOptions{}, errors.New("backgroundaudio: timeouts, durations, and MaxStreams must be positive")
	}
	if resolved.blockDuration > resolved.bufferDuration {
		return resolvedPlayerOptions{}, errors.New("backgroundaudio: BlockDuration cannot exceed BufferDuration")
	}
	if resolved.blockDuration > time.Second || time.Second%resolved.blockDuration != 0 {
		return resolvedPlayerOptions{}, errors.New("backgroundaudio: BlockDuration must evenly divide one second and not exceed it")
	}
	return resolved, nil
}

type BackgroundAudioStartOptions struct {
	Publisher    Publisher
	AgentSession AgentSession
}

type playerState uint8

const (
	playerNew playerState = iota
	playerStarting
	playerStarted
	playerClosing
	playerClosed
)

// PlayHandle represents one playout. Stop is synchronous and idempotent;
// WaitForPlayout reports stream/decoder failures while a manual stop succeeds.
type PlayHandle struct {
	done       chan struct{}
	completed  atomic.Bool
	stopOnce   sync.Once
	finishOnce sync.Once
	mu         sync.Mutex
	err        error
	stop       func()
}

func newPlayHandle(stop func()) *PlayHandle {
	return &PlayHandle{done: make(chan struct{}), stop: stop}
}

func completedPlayHandle() *PlayHandle {
	handle := newPlayHandle(nil)
	handle.finish(nil)
	return handle
}

func (h *PlayHandle) Done() bool { return h != nil && h.completed.Load() }

func (h *PlayHandle) Stop() {
	if h == nil {
		return
	}
	h.stopOnce.Do(func() {
		h.finish(nil)
		if h.stop != nil {
			h.stop()
		}
	})
}

func (h *PlayHandle) WaitForPlayout(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-h.done:
		return h.Err()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (h *PlayHandle) Wait(ctx context.Context) error { return h.WaitForPlayout(ctx) }

func (h *PlayHandle) Err() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	err := h.err
	h.mu.Unlock()
	return err
}

func (h *PlayHandle) finish(err error) {
	h.finishOnce.Do(func() {
		h.mu.Lock()
		h.err = err
		h.mu.Unlock()
		h.completed.Store(true)
		close(h.done)
	})
}

// BackgroundAudioPlayer owns mixer/source workers and its published track, but
// never owns the room, publisher, session, or caller-provided stream sources.
type BackgroundAudioPlayer struct {
	operations sync.Mutex
	plays      sync.RWMutex
	mu         sync.Mutex
	randomMu   sync.Mutex
	options    resolvedPlayerOptions
	state      playerState

	ctx    context.Context
	cancel context.CancelCauseFunc

	publisher     Publisher
	sink          FrameSink
	publication   Publication
	session       AgentSession
	sessionSub    *voice.EventSubscription
	mixer         *audioMixer
	workers       *workerTracker
	store         *builtinStore
	autoCloseStop func() bool

	ambientHandle  *PlayHandle
	thinkingHandle *PlayHandle
	active         int
}

func NewBackgroundAudioPlayer(options BackgroundAudioPlayerOptions) (*BackgroundAudioPlayer, error) {
	resolved, err := resolvePlayerOptions(options)
	if err != nil {
		return nil, err
	}
	return &BackgroundAudioPlayer{
		options: resolved, state: playerNew, workers: newWorkerTracker(),
		store: &builtinStore{resolver: resolved.builtinResolver},
	}, nil
}

func (p *BackgroundAudioPlayer) Start(ctx context.Context, options BackgroundAudioStartOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if isNilInterface(options.Publisher) {
		return errors.New("backgroundaudio: Publisher is required")
	}
	p.operations.Lock()
	defer p.operations.Unlock()
	p.plays.Lock()
	defer p.plays.Unlock()
	p.mu.Lock()
	switch p.state {
	case playerStarted, playerStarting:
		p.mu.Unlock()
		return ErrAlreadyStarted
	case playerClosing, playerClosed:
		p.mu.Unlock()
		return ErrClosed
	}
	p.state = playerStarting
	p.mu.Unlock()

	lifetime, cancel := context.WithCancelCause(ctx)
	sink, publication, err := options.Publisher.Publish(ctx, PublishRequest{Name: BackgroundAudioTrackName, SampleRate: MixerSampleRate, Channels: MixerChannels})
	if err != nil {
		cancel(err)
		p.mu.Lock()
		p.state = playerNew
		p.mu.Unlock()
		return err
	}
	if isNilInterface(sink) || publication.SID == "" {
		cancel(ErrInvalidSource)
		var cleanupErrs []error
		if !isNilInterface(sink) {
			cleanupErrs = append(cleanupErrs, closeFrameSink(ctx, sink))
		}
		if publication.SID != "" {
			cleanupErrs = append(cleanupErrs, options.Publisher.Unpublish(ctx, publication.SID))
		}
		p.mu.Lock()
		p.state = playerNew
		p.mu.Unlock()
		return errors.Join(errors.New("backgroundaudio: publisher returned an invalid sink or publication"), errors.Join(cleanupErrs...))
	}
	if err := context.Cause(ctx); err != nil {
		cleanupErr := errors.Join(
			options.Publisher.Unpublish(context.Background(), publication.SID),
			closeFrameSink(context.Background(), sink),
		)
		cancel(err)
		p.mu.Lock()
		p.state = playerNew
		p.mu.Unlock()
		return errors.Join(err, cleanupErr)
	}
	mixer := newAudioMixer(lifetime, sink, p.options)
	var subscription *voice.EventSubscription
	if !isNilInterface(options.AgentSession) {
		subscription, err = options.AgentSession.Subscribe(voice.EventSubscriptionOptions{Capacity: 16})
		if err != nil {
			mixer.Close(err)
			waitCtx, waitCancel := boundedContext(ctx, p.options.taskTimeout)
			waitErr := mixer.Wait(waitCtx)
			waitCancel()
			cleanupErr := errors.Join(waitErr, closeFrameSink(ctx, sink), options.Publisher.Unpublish(ctx, publication.SID))
			cancel(err)
			p.mu.Lock()
			p.state = playerNew
			p.mu.Unlock()
			return errors.Join(err, cleanupErr)
		}
	}
	p.mu.Lock()
	p.ctx, p.cancel = lifetime, cancel
	p.publisher, p.sink, p.publication = options.Publisher, sink, publication
	p.session, p.sessionSub, p.mixer = options.AgentSession, subscription, mixer
	p.state = playerStarted
	p.autoCloseStop = context.AfterFunc(lifetime, func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), p.options.operationTimeout)
		defer cleanupCancel()
		_ = p.Close(cleanupCtx)
	})
	p.mu.Unlock()

	if subscription != nil {
		p.workers.Add()
		go p.runSessionEvents(subscription)
	}
	if !p.options.ambientSound.Empty() {
		selected, ok, playErr := p.selectSound(p.options.ambientSound)
		if playErr == nil && !ok {
			p.mu.Lock()
			p.ambientHandle = completedPlayHandle()
			p.mu.Unlock()
			return nil
		}
		var handle *PlayHandle
		if playErr == nil {
			handle, playErr = p.playResolved(ctx, selected, selected.source.fileLike())
		}
		if playErr != nil {
			return errors.Join(playErr, p.shutdown(ctx, playErr))
		}
		p.mu.Lock()
		p.ambientHandle = handle
		p.mu.Unlock()
	}
	return nil
}

// Play starts one source or one probability-selected config. Loop repeats file
// and built-in sources. It is ignored for async streams, matching agents-js;
// async streams must provide their own looping behavior.
func (p *BackgroundAudioPlayer) Play(ctx context.Context, sound Sound, loop bool) (*PlayHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.plays.RLock()
	defer p.plays.RUnlock()
	selected, ok, err := p.selectSound(sound)
	if err != nil {
		return nil, err
	}
	if !ok {
		return completedPlayHandle(), nil
	}
	return p.playResolved(ctx, selected, loop)
}

func (p *BackgroundAudioPlayer) PlaySource(ctx context.Context, source AudioSource, loop bool) (*PlayHandle, error) {
	return p.Play(ctx, SourceSound(source), loop)
}

func (p *BackgroundAudioPlayer) playResolved(ctx context.Context, selected resolvedConfig, loop bool) (*PlayHandle, error) {
	p.mu.Lock()
	if p.state != playerStarted {
		state := p.state
		p.mu.Unlock()
		if state == playerClosed || state == playerClosing {
			return nil, ErrClosed
		}
		return nil, ErrNotStarted
	}
	if p.active >= p.options.maxStreams {
		p.mu.Unlock()
		return nil, ErrTooManyStreams
	}
	parent, mixer := p.ctx, p.mixer
	p.active++
	p.mu.Unlock()

	playCtx, cancel := context.WithCancelCause(parent)
	stopLink := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	opened, err := selected.source.open(playCtx, sourceOpenOptions{
		loop: loop, blockDuration: p.options.blockDuration, bufferDuration: p.options.bufferDuration,
		ffmpegPath: p.options.ffmpegPath, resolveBuiltin: p.store.resolve,
	})
	if err != nil {
		stopLink()
		cancel(err)
		p.decrementActive()
		return nil, err
	}
	id := nextStreamID.Add(1)
	capacity := max(1, int((p.options.bufferDuration+p.options.blockDuration-1)/p.options.blockDuration))
	queue := stream.NewChannel[agents.AudioFrame](capacity)
	handle := newPlayHandle(func() {
		cancel(ErrPlayStopped)
		_ = queue.Abort(ErrPlayStopped)
		mixer.remove(id)
	})
	value := &mixStream{id: id, queue: queue, handle: handle, cancel: cancel, onDone: p.decrementActive}
	if err := mixer.add(ctx, value); err != nil {
		stopLink()
		cancel(err)
		_ = queue.Abort(err)
		if opened.close != nil {
			err = errors.Join(err, opened.close())
		}
		value.complete(err)
		return nil, err
	}
	p.workers.Add()
	go p.feedStream(playCtx, opened, value, selected.volume, stopLink)
	return handle, nil
}

func (p *BackgroundAudioPlayer) feedStream(ctx context.Context, opened openedSource, value *mixStream, volume float64, stopLink func() bool) {
	defer p.workers.Done()
	defer stopLink()
	converter := newFrameConverter(max(1, int(time.Duration(MixerSampleRate)*p.options.blockDuration/time.Second)), volume)
	var sourceCloseOnce sync.Once
	var sourceCloseErr error
	finish := func(err error) {
		sourceCloseOnce.Do(func() {
			if opened.close != nil {
				sourceCloseErr = opened.close()
			}
		})
		err = errors.Join(err, sourceCloseErr)
		if err == nil {
			_ = value.queue.Close()
		} else {
			_ = value.queue.Abort(err)
		}
	}
	for {
		recvCtx, recvCancel := context.WithTimeoutCause(ctx, p.options.streamTimeout, ErrStreamTimeout)
		frame, err := opened.reader.Recv(recvCtx)
		cause := context.Cause(recvCtx)
		recvCancel()
		if err != nil {
			if errors.Is(err, io.EOF) {
				for _, finalFrame := range converter.flush() {
					if sendErr := value.queue.Send(ctx, finalFrame); sendErr != nil {
						finish(sendErr)
						return
					}
				}
				finish(nil)
				return
			}
			if cause != nil {
				err = cause
			}
			finish(err)
			return
		}
		frames, err := converter.push(frame)
		if err != nil {
			finish(err)
			return
		}
		for _, converted := range frames {
			if err := value.queue.Send(ctx, converted); err != nil {
				finish(err)
				return
			}
		}
	}
}

func (p *BackgroundAudioPlayer) selectSound(sound Sound) (resolvedConfig, bool, error) {
	if sound.Empty() {
		return resolvedConfig{}, false, nil
	}
	resolved := make([]resolvedConfig, len(sound.Configs))
	for index, config := range sound.Configs {
		value, err := config.resolved()
		if err != nil {
			return resolvedConfig{}, false, err
		}
		resolved[index] = value
	}
	if len(resolved) == 1 && !sound.Weighted {
		return resolved[0], true, nil
	}
	var total float64
	for _, value := range resolved {
		total += value.probability
	}
	if total <= 0 {
		return resolvedConfig{}, false, nil
	}
	if total < 1 && p.randomFloat64() > total {
		return resolvedConfig{}, false, nil
	}
	normalize := 1.0
	if total > 1 {
		normalize = total
	}
	r := p.randomFloat64() * min(total, 1.0)
	cumulative := 0.0
	for _, value := range resolved {
		if value.probability <= 0 {
			continue
		}
		cumulative += value.probability / normalize
		if r <= cumulative {
			return value, true, nil
		}
	}
	// Preserve agents-js's final fallback exactly, even when the last config's
	// probability is non-positive.
	return resolved[len(resolved)-1], true, nil
}

func (p *BackgroundAudioPlayer) randomFloat64() float64 {
	p.randomMu.Lock()
	value := p.options.random()
	p.randomMu.Unlock()
	if math.IsNaN(value) || value < 0 || value >= 1 {
		// Keep a broken injected RNG from violating selection invariants.
		value = math.Mod(math.Abs(value), 1)
		if math.IsNaN(value) {
			value = 0
		}
	}
	return value
}

func (p *BackgroundAudioPlayer) runSessionEvents(subscription *voice.EventSubscription) {
	defer p.workers.Done()
	defer subscription.Close()
	for {
		event, err := subscription.Recv(p.ctx)
		if err != nil {
			return
		}
		var changed voice.AgentStateChangedEvent
		switch value := event.(type) {
		case voice.AgentStateChangedEvent:
			changed = value
		case *voice.AgentStateChangedEvent:
			if value == nil {
				continue
			}
			changed = *value
		default:
			continue
		}
		p.handleAgentState(changed.NewState)
	}
}

func (p *BackgroundAudioPlayer) handleAgentState(state voice.AgentState) {
	if p.options.thinkingSound.Empty() {
		return
	}
	p.mu.Lock()
	current := p.thinkingHandle
	p.mu.Unlock()
	if state == voice.AgentStateThinking {
		if current != nil && !current.Done() {
			return
		}
		selected, ok, err := p.selectSound(p.options.thinkingSound)
		if err != nil {
			p.reportError(err)
			return
		}
		if !ok {
			return
		}
		handle, err := p.playResolved(p.ctx, selected, selected.source.fileLike())
		if err != nil {
			p.reportError(err)
			return
		}
		p.mu.Lock()
		p.thinkingHandle = handle
		p.mu.Unlock()
		return
	}
	if current != nil {
		current.Stop()
	}
}

func (p *BackgroundAudioPlayer) reportError(err error) {
	if err == nil || p.options.onError == nil {
		return
	}
	func() {
		defer func() { _ = recover() }()
		p.options.onError(err)
	}()
}

func (p *BackgroundAudioPlayer) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.operations.Lock()
	defer p.operations.Unlock()
	// Reject new Play calls before waiting for any source currently opening.
	// Session-event playback is part of a tracked worker and observes this state
	// without contending on plays, so Close can wait for that worker safely.
	p.mu.Lock()
	if p.state == playerStarted || p.state == playerStarting {
		p.state = playerClosing
	}
	cancel, autoCloseStop, subscription := p.cancel, p.autoCloseStop, p.sessionSub
	p.mu.Unlock()
	if autoCloseStop != nil {
		autoCloseStop()
	}
	if subscription != nil {
		_ = subscription.Close()
	}
	if cancel != nil {
		cancel(ErrClosed)
	}
	p.plays.Lock()
	defer p.plays.Unlock()
	return p.shutdown(ctx, ErrClosed)
}

func (p *BackgroundAudioPlayer) shutdown(ctx context.Context, cause error) error {
	p.mu.Lock()
	if p.state == playerClosed {
		p.mu.Unlock()
		return nil
	}
	if p.state == playerNew {
		p.state = playerClosed
		p.mu.Unlock()
		return p.store.close()
	}
	p.state = playerClosing
	cancel, mixer := p.cancel, p.mixer
	autoCloseStop := p.autoCloseStop
	subscription, sink, publisher := p.sessionSub, p.sink, p.publisher
	publication := p.publication
	ambient, thinking := p.ambientHandle, p.thinkingHandle
	p.mu.Unlock()
	if autoCloseStop != nil {
		autoCloseStop()
	}

	if ambient != nil {
		ambient.Stop()
	}
	if thinking != nil {
		thinking.Stop()
	}
	if subscription != nil {
		_ = subscription.Close()
	}
	if cancel != nil {
		cancel(cause)
	}
	if mixer != nil {
		mixer.Close(cause)
	}
	waitCtx, waitCancel := boundedContext(ctx, p.options.taskTimeout)
	var errs []error
	if mixer != nil {
		if err := mixer.Wait(waitCtx); err != nil && !errors.Is(err, ErrClosed) {
			errs = append(errs, err)
		}
	}
	if err := p.workers.Wait(waitCtx); err != nil {
		errs = append(errs, err)
	}
	waitCancel()
	if publisher != nil {
		current, ok, err := publisher.CurrentPublication(ctx, BackgroundAudioTrackName)
		if err != nil {
			errs = append(errs, err)
		} else if ok {
			publication = current
		}
		if publication.SID != "" {
			if err := publisher.Unpublish(ctx, publication.SID); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if sink != nil {
		if err := closeFrameSink(ctx, sink); err != nil {
			errs = append(errs, err)
		}
	}
	if err := p.store.close(); err != nil {
		errs = append(errs, err)
	}
	p.mu.Lock()
	p.state = playerClosed
	p.ctx, p.cancel, p.publisher, p.sink, p.session, p.sessionSub, p.mixer, p.autoCloseStop = nil, nil, nil, nil, nil, nil, nil, nil
	p.mu.Unlock()
	return errors.Join(errs...)
}

func closeFrameSink(ctx context.Context, sink FrameSink) error {
	if closer, ok := sink.(ClosableFrameSink); ok {
		return closer.Close(ctx)
	}
	return nil
}

var _ FrameSink = (voice.AudioOutput)(nil)

func (p *BackgroundAudioPlayer) Publication(ctx context.Context) (Publication, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	publisher, cached := p.publisher, p.publication
	started := p.state == playerStarted
	p.mu.Unlock()
	if !started || publisher == nil {
		return Publication{}, false, ErrNotStarted
	}
	current, ok, err := publisher.CurrentPublication(ctx, BackgroundAudioTrackName)
	if err != nil {
		return Publication{}, false, err
	}
	if ok {
		return current, true, nil
	}
	return cached, cached.SID != "", nil
}

func (p *BackgroundAudioPlayer) decrementActive() {
	p.mu.Lock()
	if p.active > 0 {
		p.active--
	}
	p.mu.Unlock()
}

func boundedContext(parent context.Context, duration time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if duration <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, duration)
}
