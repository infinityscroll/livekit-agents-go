// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"

	agents "github.com/livekit/agents-go"
	mediabase "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	lkmedia "github.com/livekit/server-sdk-go/v2/pkg/media"
	"github.com/pion/webrtc/v4"
)

var ErrParticipantAudioInputClosed = errors.New("roomio: participant audio input is closed")

// ParticipantAudioInput decodes the selected participant's microphone into a
// bounded PCM stream. Media callbacks never block: overload discards the oldest
// frame and is observable through DroppedFrames.
type ParticipantAudioInput struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	bridge *RTCBridge
	opts   RoomInputOptions
	onErr  func(error)

	frames     *realtimeQueue[agents.AudioFrame]
	processing *realtimeQueue[agents.AudioFrame]
	attached   atomic.Bool
	closed     atomic.Bool

	mu          sync.Mutex
	participant *lksdk.RemoteParticipant
	publication *lksdk.RemoteTrackPublication
	remote      *lkmedia.PCMRemoteTrack
	generation  uint64

	sub           *RTCSubscription
	done          chan struct{}
	processorDone chan struct{}
	once          sync.Once
}

func NewParticipantAudioInput(parent context.Context, bridge *RTCBridge, options RoomInputOptions, onError func(error)) (*ParticipantAudioInput, error) {
	if bridge == nil {
		return nil, errors.New("roomio: RTC bridge is required for participant audio")
	}
	resolved, err := resolveInputOptions(&options)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	sub, err := bridge.SubscribeTypes(DefaultParticipantEventQueue,
		RTCEventTrackPublished,
		RTCEventTrackUnpublished,
		RTCEventTrackSubscribed,
		RTCEventTrackUnsubscribed,
		RTCEventParticipantDisconnected,
	)
	if err != nil {
		cancel(err)
		return nil, err
	}
	input := &ParticipantAudioInput{
		ctx: ctx, cancel: cancel, bridge: bridge, opts: resolved, onErr: onError,
		frames: newRealtimeQueue[agents.AudioFrame](resolved.AudioCapacity),
		sub:    sub, done: make(chan struct{}),
	}
	if resolved.FrameProcessor != nil {
		input.processing = newRealtimeQueue[agents.AudioFrame](resolved.AudioCapacity)
		input.processorDone = make(chan struct{})
		go input.processFrames()
	} else {
		input.processorDone = make(chan struct{})
		close(input.processorDone)
	}
	go input.run()
	return input, nil
}

func (i *ParticipantAudioInput) Recv(ctx context.Context) (agents.AudioFrame, error) {
	return i.frames.Recv(ctx)
}

func (i *ParticipantAudioInput) SetAttached(attached bool) { i.attached.Store(attached) }
func (i *ParticipantAudioInput) OnAttached()               { i.attached.Store(true) }
func (i *ParticipantAudioInput) OnDetached()               { i.attached.Store(false) }

func (i *ParticipantAudioInput) DroppedFrames() uint64 {
	dropped := i.frames.Dropped()
	if i.processing != nil {
		dropped += i.processing.Dropped()
	}
	return dropped
}

func (i *ParticipantAudioInput) Participant() *lksdk.RemoteParticipant {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.participant
}

// SetParticipant switches the microphone source synchronously. Passing nil
// unlinks the current participant and immediately stops admitting its frames.
func (i *ParticipantAudioInput) SetParticipant(ctx context.Context, participant *lksdk.RemoteParticipant) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	if i.closed.Load() {
		return ErrParticipantAudioInputClosed
	}
	i.mu.Lock()
	if i.participant == participant {
		i.mu.Unlock()
		return nil
	}
	i.detachLocked()
	i.participant = participant
	i.mu.Unlock()
	if participant == nil {
		return nil
	}
	return i.selectMicrophone(participant)
}

func (i *ParticipantAudioInput) selectMicrophone(participant *lksdk.RemoteParticipant) error {
	publications := participant.TrackPublications()
	slices.SortFunc(publications, func(a, b lksdk.TrackPublication) int {
		if a.SID() < b.SID() {
			return -1
		}
		if a.SID() > b.SID() {
			return 1
		}
		return 0
	})
	for _, publication := range publications {
		remote, ok := publication.(*lksdk.RemoteTrackPublication)
		if !ok || remote.Kind() != lksdk.TrackKindAudio || remote.Source() != livekit.TrackSource_MICROPHONE {
			continue
		}
		if err := remote.SetSubscribed(true); err != nil {
			return err
		}
		i.mu.Lock()
		if i.participant == participant && !i.closed.Load() {
			i.publication = remote
			if track := remote.TrackRemote(); track != nil {
				err := i.attachTrackLocked(track, remote)
				i.mu.Unlock()
				return err
			}
		}
		i.mu.Unlock()
		// A concurrent participant switch may win while SetSubscribed is in
		// flight. Undo the stale subscription so it cannot consume bandwidth or
		// decoder CPU after losing the generation race.
		_ = remote.SetSubscribed(false)
		return nil
	}
	return nil
}

func (i *ParticipantAudioInput) attachTrackLocked(track *webrtc.TrackRemote, publication *lksdk.RemoteTrackPublication) error {
	if i.remote != nil {
		i.remote.Close()
		i.remote = nil
	}
	i.generation++
	generation := i.generation
	writer := &remotePCMWriter{input: i, generation: generation}
	options := []lkmedia.PCMRemoteTrackOption{
		lkmedia.WithTargetSampleRate(i.opts.AudioSampleRate),
		lkmedia.WithTargetChannels(i.opts.AudioChannels),
	}
	if i.opts.Decryptor != nil {
		decryptor, err := i.opts.Decryptor(publication)
		if err != nil {
			return err
		}
		if decryptor != nil {
			options = append(options, lkmedia.WithDecryptor(decryptor))
		}
	}
	remote, err := lkmedia.NewPCMRemoteTrack(track, writer, options...)
	if err != nil {
		return err
	}
	i.remote = remote
	i.publication = publication
	return nil
}

func (i *ParticipantAudioInput) detachLocked() {
	i.generation++
	if i.publication != nil {
		_ = i.publication.SetSubscribed(false)
	}
	if i.remote != nil {
		i.remote.Close()
		i.remote = nil
	}
	i.publication = nil
	i.participant = nil
}

func (i *ParticipantAudioInput) run() {
	defer close(i.done)
	defer i.sub.Close()
	for {
		event, err := i.sub.Recv(i.ctx)
		if err != nil {
			if context.Cause(i.ctx) == nil && !errors.Is(err, io.EOF) {
				i.report(err)
				if errors.Is(err, ErrRTCEventOverflow) {
					i.beginClose(err)
				}
			}
			return
		}
		i.handleRTCEvent(event)
	}
}

func (i *ParticipantAudioInput) handleRTCEvent(event RTCEvent) {
	i.mu.Lock()
	participant := i.participant
	publication := i.publication
	if participant == nil || event.Participant == nil || event.Participant.Identity() != participant.Identity() {
		i.mu.Unlock()
		return
	}
	switch event.Type {
	case RTCEventTrackPublished:
		needSelect := publication == nil
		i.mu.Unlock()
		if needSelect {
			if err := i.selectMicrophone(participant); err != nil {
				i.report(err)
			}
		}
		return
	case RTCEventTrackSubscribed:
		if event.Publication == publication && event.Track != nil {
			err := i.attachTrackLocked(event.Track, event.Publication)
			i.mu.Unlock()
			if err != nil {
				i.report(err)
			}
			return
		}
	case RTCEventTrackUnpublished, RTCEventTrackUnsubscribed:
		if event.Publication == publication {
			if i.remote != nil {
				i.remote.Close()
				i.remote = nil
			}
			i.publication = nil
			i.generation++
			i.mu.Unlock()
			if err := i.selectMicrophone(participant); err != nil {
				i.report(err)
			}
			return
		}
	case RTCEventParticipantDisconnected:
		i.detachLocked()
	}
	i.mu.Unlock()
}

func (i *ParticipantAudioInput) admit(generation uint64, samples mediabase.PCM16Sample) {
	if len(samples) == 0 || !i.attached.Load() || i.closed.Load() {
		return
	}
	i.mu.Lock()
	active := generation == i.generation && i.participant != nil
	i.mu.Unlock()
	if !active {
		return
	}
	data := append([]int16(nil), samples...)
	frame, err := agents.NewAudioFrame(data, i.opts.AudioSampleRate, i.opts.AudioChannels)
	if err != nil {
		i.report(err)
		return
	}
	if i.processing != nil {
		i.processing.Push(frame)
		return
	}
	i.frames.Push(frame)
}

func (i *ParticipantAudioInput) processFrames() {
	defer close(i.processorDone)
	for {
		frame, err := i.processing.Recv(i.ctx)
		if err != nil {
			return
		}
		processed, err := i.processFrame(frame)
		if err != nil {
			if context.Cause(i.ctx) == nil {
				i.report(err)
			}
			continue
		}
		if !i.attached.Load() {
			continue
		}
		i.frames.Push(processed)
	}
}

func (i *ParticipantAudioInput) processFrame(frame agents.AudioFrame) (processed agents.AudioFrame, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newCallbackPanicError("AudioFrameProcessor.Process", recovered)
		}
	}()
	return i.opts.FrameProcessor.Process(i.ctx, frame)
}

func (i *ParticipantAudioInput) report(err error) {
	if err != nil && i.onErr != nil {
		_ = invokeCallback("OnError", func() { i.onErr(err) })
	}
}

func (i *ParticipantAudioInput) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultRoomIOCloseTimeout)
	defer cancel()
	return i.CloseContext(ctx)
}

func (i *ParticipantAudioInput) CloseContext(ctx context.Context) error {
	if i == nil {
		return nil
	}
	i.beginClose(ErrParticipantAudioInputClosed)
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-i.done:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	select {
	case <-i.processorDone:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (i *ParticipantAudioInput) beginClose(cause error) {
	i.once.Do(func() {
		if cause == nil {
			cause = ErrParticipantAudioInputClosed
		}
		i.closed.Store(true)
		i.cancel(cause)
		i.mu.Lock()
		i.detachLocked()
		i.mu.Unlock()
		if i.processing != nil {
			i.processing.Close(cause)
		}
		if errors.Is(cause, ErrParticipantAudioInputClosed) {
			i.frames.Close(nil)
		} else {
			i.frames.Close(cause)
		}
		if i.opts.CloseProcessor && i.opts.FrameProcessor != nil {
			if err := invokeErrorCallback("AudioFrameProcessor.Close", i.opts.FrameProcessor.Close); err != nil {
				i.report(err)
			}
		}
	})
}

type remotePCMWriter struct {
	input      *ParticipantAudioInput
	generation uint64
}

func (w *remotePCMWriter) WriteSample(sample mediabase.PCM16Sample) error {
	w.input.admit(w.generation, sample)
	return nil
}

func (*remotePCMWriter) Close() error { return nil }

var _ interface {
	Recv(context.Context) (agents.AudioFrame, error)
	SetAttached(bool)
	OnAttached()
	OnDetached()
	Close() error
} = (*ParticipantAudioInput)(nil)

// Media decoders can remain blocked in TrackRemote.ReadRTP until the RTC SDK
// closes the track. The input owns every goroutine it creates; this limitation
// is confined to server-sdk-go's internal decoder goroutine.
