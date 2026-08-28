// SPDX-License-Identifier: Apache-2.0

package avatar

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/voice"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

const DefaultClearBufferTimeout = 2 * time.Second

var (
	ErrDataStreamAudioOutputClosed = errors.New("avatar DataStream audio output is closed")
	ErrAvatarRTCBridgeRequired     = errors.New("avatar RTC bridge is required while waiting for a participant or track")
)

type RPCInvocation struct {
	CallerIdentity string
	Payload        string
}

type ByteStreamOptions struct {
	Name                string
	Topic               string
	DestinationIdentity string
	Attributes          map[string]string
}

type ByteStreamWriter interface {
	Write(context.Context, []byte) error
	Close(context.Context) error
}

// DataStreamTransport isolates RTC mechanics for deterministic tests and
// custom room implementations. RegisterRPC must multiplex identities for the
// same method and return an idempotent unregister function.
type DataStreamTransport interface {
	WaitReady(context.Context, string, lksdk.TrackKind) error
	OpenByteStream(context.Context, ByteStreamOptions) (ByteStreamWriter, error)
	PerformRPC(context.Context, string, string, string) (string, error)
	RegisterRPC(string, string, func(RPCInvocation) string) (func(), error)
}

// ReadyWaiter bridges the server SDK's lack of post-construction Room callback
// registration. It is only needed when the destination has not already joined.
// voice/avatar/roomioadapter provides one for roomio.RTCBridge.
type ReadyWaiter func(context.Context, *lksdk.Room, string, lksdk.TrackKind) error

type DataStreamAudioOutputOptions struct {
	Room                *lksdk.Room
	ReadyWaiter         ReadyWaiter
	Transport           DataStreamTransport
	DestinationIdentity string
	SampleRate          int
	WaitRemoteTrack     lksdk.TrackKind
	WaitPlaybackStart   bool
	ClearBufferTimeout  time.Duration
	DisableClearTimeout bool
}

type dataStreamStartOperation struct {
	done chan struct{}
	err  error
}

type managedByteStreamWriter struct {
	writer ByteStreamWriter
	once   sync.Once
	err    error
}

func (w *managedByteStreamWriter) close(ctx context.Context) error {
	w.once.Do(func() { w.err = w.writer.Close(ctx) })
	return w.err
}

// DataStreamAudioOutput sends strict little-endian PCM16 in one bounded RTC
// byte stream per speech segment and uses avatar RPCs for playout accounting.
type DataStreamAudioOutput struct {
	managed      *voice.ManagedAudioOutput
	transport    DataStreamTransport
	destination  string
	waitTrack    lksdk.TrackKind
	waitStarted  bool
	clearTimeout time.Duration
	noTimeout    bool

	startMu sync.Mutex
	started bool
	startOp *dataStreamStartOperation

	opMu            sync.Mutex
	writer          *managedByteStreamWriter
	activeWriter    atomic.Pointer[managedByteStreamWriter]
	streamRate      int
	streamChannels  int
	pushedDuration  time.Duration
	firstFrame      bool
	closed          bool
	clearGeneration uint64
	clearHandled    uint64
	clearTimer      *time.Timer
	unregister      []func()
	closeOnce       sync.Once
	closeErr        error
	closing         atomic.Bool
}

func NewDataStreamAudioOutput(options DataStreamAudioOutputOptions) (*DataStreamAudioOutput, error) {
	if options.DestinationIdentity == "" {
		return nil, errors.New("avatar DataStream destination identity is required")
	}
	transport := options.Transport
	if transport == nil {
		if options.Room == nil || options.Room.LocalParticipant == nil {
			return nil, errors.New("avatar DataStream output requires a connected Room or a custom Transport")
		}
		transport = &sdkDataStreamTransport{room: options.Room, waitReady: options.ReadyWaiter}
	}
	clearTimeout := options.ClearBufferTimeout
	if clearTimeout <= 0 && !options.DisableClearTimeout {
		clearTimeout = DefaultClearBufferTimeout
	}
	output := &DataStreamAudioOutput{
		transport: transport, destination: options.DestinationIdentity,
		waitTrack: options.WaitRemoteTrack, waitStarted: options.WaitPlaybackStart,
		clearTimeout: clearTimeout, noTimeout: options.DisableClearTimeout,
	}
	managed, err := voice.NewManagedAudioOutput(voice.AudioOutputOptions{
		SampleRate: options.SampleRate, Capabilities: voice.AudioOutputCapabilities{Pause: false},
		Capture: output.capture, Flush: output.flush, ClearBuffer: output.clearBuffer,
	})
	if err != nil {
		return nil, err
	}
	output.managed = managed
	finished, err := transport.RegisterRPC(RPCPlaybackFinished, output.destination, output.handlePlaybackFinished)
	if err != nil {
		return nil, fmt.Errorf("register avatar playback-finished RPC: %w", err)
	}
	output.unregister = append(output.unregister, finished)
	if output.waitStarted {
		started, err := transport.RegisterRPC(RPCPlaybackStarted, output.destination, output.handlePlaybackStarted)
		if err != nil {
			finished()
			return nil, fmt.Errorf("register avatar playback-started RPC: %w", err)
		}
		output.unregister = append(output.unregister, started)
	}
	return output, nil
}

func (o *DataStreamAudioOutput) ensureStarted(ctx context.Context) error {
	o.startMu.Lock()
	if o.started {
		o.startMu.Unlock()
		return nil
	}
	if operation := o.startOp; operation != nil {
		o.startMu.Unlock()
		select {
		case <-operation.done:
			return operation.err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	operation := &dataStreamStartOperation{done: make(chan struct{})}
	o.startOp = operation
	o.startMu.Unlock()

	err := o.transport.WaitReady(ctx, o.destination, o.waitTrack)
	o.startMu.Lock()
	operation.err = err
	if err == nil {
		o.started = true
	}
	o.startOp = nil
	close(operation.done)
	o.startMu.Unlock()
	return err
}

func (o *DataStreamAudioOutput) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	return o.managed.CaptureFrame(ctx, frame)
}

func (o *DataStreamAudioOutput) capture(ctx context.Context, frame agents.AudioFrame) error {
	if o.closing.Load() {
		return ErrDataStreamAudioOutputClosed
	}
	if err := validateAvatarFrame(frame); err != nil {
		return err
	}
	if err := o.ensureStarted(ctx); err != nil {
		return err
	}
	o.opMu.Lock()
	defer o.opMu.Unlock()
	if o.closed || o.closing.Load() {
		return ErrDataStreamAudioOutputClosed
	}
	if o.writer == nil {
		writer, err := o.transport.OpenByteStream(ctx, ByteStreamOptions{
			Name: agents.ShortUUID("AUDIO_"), Topic: AudioStreamTopic,
			DestinationIdentity: o.destination,
			Attributes: map[string]string{
				"sample_rate":  strconv.Itoa(frame.SampleRate),
				"num_channels": strconv.Itoa(frame.Channels),
			},
		})
		if err != nil {
			return err
		}
		o.writer = &managedByteStreamWriter{writer: writer}
		o.activeWriter.Store(o.writer)
		o.streamRate, o.streamChannels = frame.SampleRate, frame.Channels
		o.pushedDuration = 0
		o.firstFrame = false
	}
	if frame.SampleRate != o.streamRate || frame.Channels != o.streamChannels {
		return fmt.Errorf("avatar DataStream audio format changed within a segment: got %d Hz/%d channels, want %d Hz/%d channels", frame.SampleRate, frame.Channels, o.streamRate, o.streamChannels)
	}
	pcm := make([]byte, len(frame.Data)*2)
	for index, sample := range frame.Data {
		binary.LittleEndian.PutUint16(pcm[index*2:], uint16(sample))
	}
	if err := o.writer.writer.Write(ctx, pcm); err != nil {
		return err
	}
	o.pushedDuration += frame.Duration()
	if !o.firstFrame {
		o.firstFrame = true
		if !o.waitStarted {
			o.managed.NotifyPlaybackStarted(time.Now())
		}
	}
	return nil
}

func validateAvatarFrame(frame agents.AudioFrame) error {
	if frame.SampleRate <= 0 || frame.Channels <= 0 || frame.SamplesPerChannel <= 0 || len(frame.Data) != frame.SamplesPerChannel*frame.Channels {
		return fmt.Errorf("%w: rate=%d channels=%d samplesPerChannel=%d data=%d", agents.ErrInvalidAudioFormat, frame.SampleRate, frame.Channels, frame.SamplesPerChannel, len(frame.Data))
	}
	return nil
}

func (o *DataStreamAudioOutput) Flush(ctx context.Context) error { return o.managed.Flush(ctx) }

func (o *DataStreamAudioOutput) flush(ctx context.Context) error {
	o.opMu.Lock()
	defer o.opMu.Unlock()
	if o.closed || o.closing.Load() {
		return ErrDataStreamAudioOutputClosed
	}
	if o.writer == nil {
		return nil
	}
	writer := o.writer
	err := writer.close(ctx)
	o.activeWriter.CompareAndSwap(writer, nil)
	o.writer = nil
	o.streamRate, o.streamChannels = 0, 0
	o.firstFrame = false
	return err
}

func (o *DataStreamAudioOutput) ClearBuffer(ctx context.Context) error {
	return o.managed.ClearBuffer(ctx)
}

func (o *DataStreamAudioOutput) clearBuffer(ctx context.Context) error {
	o.startMu.Lock()
	started := o.started
	o.startMu.Unlock()
	if !started {
		return nil
	}
	o.opMu.Lock()
	if o.closed {
		o.opMu.Unlock()
		return ErrDataStreamAudioOutputClosed
	}
	o.clearGeneration++
	generation := o.clearGeneration
	pushed := o.pushedDuration
	if o.clearTimer != nil {
		o.clearTimer.Stop()
		o.clearTimer = nil
	}
	o.opMu.Unlock()

	_, err := o.transport.PerformRPC(ctx, o.destination, RPCClearBuffer, "")
	if err != nil {
		o.finishClearedGeneration(generation, pushed)
		return fmt.Errorf("perform avatar clear-buffer RPC: %w", err)
	}
	if o.noTimeout {
		return nil
	}
	o.opMu.Lock()
	if !o.closed && o.clearHandled < generation && o.clearGeneration == generation {
		o.clearTimer = time.AfterFunc(o.clearTimeout, func() { o.finishClearedGeneration(generation, pushed) })
	}
	o.opMu.Unlock()
	return nil
}

func (o *DataStreamAudioOutput) finishClearedGeneration(generation uint64, pushed time.Duration) {
	o.opMu.Lock()
	if o.closed || o.clearHandled >= generation || o.clearGeneration != generation {
		o.opMu.Unlock()
		return
	}
	o.clearHandled = generation
	if o.clearTimer != nil {
		o.clearTimer.Stop()
		o.clearTimer = nil
	}
	o.opMu.Unlock()
	if o.managed.PendingPlayoutSegments() != 0 {
		_ = o.managed.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: pushed, Interrupted: true})
	}
}

func (o *DataStreamAudioOutput) handlePlaybackFinished(invocation RPCInvocation) string {
	if invocation.CallerIdentity != o.destination {
		return "reject"
	}
	o.opMu.Lock()
	if o.clearGeneration > o.clearHandled {
		o.clearHandled = o.clearGeneration
	}
	if o.clearTimer != nil {
		o.clearTimer.Stop()
		o.clearTimer = nil
	}
	closed := o.closed
	o.opMu.Unlock()
	if closed {
		return "reject"
	}
	_ = o.managed.NotifyPlaybackFinished(ParsePlaybackFinishedPayload(invocation.Payload))
	return "ok"
}

func (o *DataStreamAudioOutput) handlePlaybackStarted(invocation RPCInvocation) string {
	if invocation.CallerIdentity != o.destination {
		return "reject"
	}
	o.opMu.Lock()
	closed := o.closed
	o.opMu.Unlock()
	if closed {
		return "reject"
	}
	o.managed.NotifyPlaybackStarted(time.Now())
	return "ok"
}

func (o *DataStreamAudioOutput) WaitForPlayout(ctx context.Context) (voice.PlaybackFinishedEvent, error) {
	return o.managed.WaitForPlayout(ctx)
}
func (o *DataStreamAudioOutput) Pause(ctx context.Context) error  { return o.managed.Pause(ctx) }
func (o *DataStreamAudioOutput) Resume(ctx context.Context) error { return o.managed.Resume(ctx) }
func (o *DataStreamAudioOutput) CanPause() bool                   { return false }
func (o *DataStreamAudioOutput) SampleRate() int                  { return o.managed.SampleRate() }
func (o *DataStreamAudioOutput) SetAttached(attached bool)        { o.managed.SetAttached(attached) }
func (o *DataStreamAudioOutput) OnAttached()                      { o.managed.OnAttached() }
func (o *DataStreamAudioOutput) OnDetached()                      { o.managed.OnDetached() }
func (o *DataStreamAudioOutput) OnPlaybackStarted(fn func(voice.PlaybackStartedEvent)) func() {
	return o.managed.OnPlaybackStarted(fn)
}
func (o *DataStreamAudioOutput) OnPlaybackFinished(fn func(voice.PlaybackFinishedEvent)) func() {
	return o.managed.OnPlaybackFinished(fn)
}
func (o *DataStreamAudioOutput) PendingPlayoutSegments() uint64 {
	return o.managed.PendingPlayoutSegments()
}
func (o *DataStreamAudioOutput) CapturedPlayoutSegments() uint64 {
	return o.managed.CapturedPlayoutSegments()
}

func (o *DataStreamAudioOutput) Close(ctx context.Context) error {
	o.closeOnce.Do(func() {
		o.closing.Store(true)
		// The pinned SDK's Write has no Context parameter, but Close breaks its
		// writer fuse. Do that before taking opMu so a blocked capture can always
		// return and release the operation mutex.
		if writer := o.activeWriter.Load(); writer != nil {
			o.closeErr = writer.close(ctx)
		}
		o.opMu.Lock()
		o.closed = true
		if o.clearTimer != nil {
			o.clearTimer.Stop()
			o.clearTimer = nil
		}
		writer := o.writer
		o.writer = nil
		o.activeWriter.Store(nil)
		o.opMu.Unlock()
		if writer != nil {
			if err := writer.close(ctx); o.closeErr == nil {
				o.closeErr = err
			}
		}
		for index := len(o.unregister) - 1; index >= 0; index-- {
			o.unregister[index]()
		}
		o.unregister = nil
		o.managed.Close()
	})
	return o.closeErr
}

func (o *DataStreamAudioOutput) AClose(ctx context.Context) error { return o.Close(ctx) }

var _ voice.AudioOutput = (*DataStreamAudioOutput)(nil)

type sdkDataStreamTransport struct {
	room      *lksdk.Room
	waitReady ReadyWaiter
}

func (t *sdkDataStreamTransport) WaitReady(ctx context.Context, identity string, kind lksdk.TrackKind) error {
	if sdkDestinationReady(t.room, identity, kind) {
		return nil
	}
	if t.waitReady == nil {
		return ErrAvatarRTCBridgeRequired
	}
	return t.waitReady(ctx, t.room, identity, kind)
}

func sdkDestinationReady(room *lksdk.Room, identity string, kind lksdk.TrackKind) bool {
	if room == nil {
		return false
	}
	participant := room.GetParticipantByIdentity(identity)
	if participant == nil {
		return false
	}
	if kind == "" {
		return true
	}
	for _, publication := range participant.TrackPublications() {
		if publication.Kind() == kind {
			return true
		}
	}
	return false
}

func (t *sdkDataStreamTransport) OpenByteStream(ctx context.Context, options ByteStreamOptions) (ByteStreamWriter, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if t.room == nil || t.room.LocalParticipant == nil || t.room.ConnectionState() != lksdk.ConnectionStateConnected {
		return nil, errors.New("avatar DataStream room is not connected")
	}
	name := options.Name
	writer := t.room.LocalParticipant.StreamBytes(lksdk.StreamBytesOptions{
		Topic: options.Topic, MimeType: "audio/pcm",
		DestinationIdentities: []string{options.DestinationIdentity},
		Attributes:            options.Attributes, FileName: &name,
	})
	return sdkByteStreamWriter{writer: writer}, nil
}

func (t *sdkDataStreamTransport) PerformRPC(ctx context.Context, identity, method, payload string) (string, error) {
	if err := context.Cause(ctx); err != nil {
		return "", err
	}
	params := lksdk.PerformRpcParams{DestinationIdentity: identity, Method: method, Payload: payload}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", context.DeadlineExceeded
		}
		params.ResponseTimeout = &remaining
	}
	response, err := t.room.LocalParticipant.PerformRpc(params)
	if err != nil || response == nil {
		return "", err
	}
	return *response, nil
}

func (t *sdkDataStreamTransport) RegisterRPC(method, identity string, handler func(RPCInvocation) string) (func(), error) {
	return registerSDKAvatarRPC(t.room, method, identity, handler)
}

type sdkByteStreamWriter struct{ writer *lksdk.ByteStreamWriter }

func (w sdkByteStreamWriter) Write(ctx context.Context, data []byte) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	w.writer.Write(data, nil)
	return nil
}
func (w sdkByteStreamWriter) Close(context.Context) error { w.writer.Close(); return nil }

type sdkRPCHub struct {
	handlers map[string]map[string]func(RPCInvocation) string
}

var sdkRPCHubs = struct {
	sync.Mutex
	rooms map[*lksdk.Room]*sdkRPCHub
}{rooms: make(map[*lksdk.Room]*sdkRPCHub)}

func registerSDKAvatarRPC(room *lksdk.Room, method, identity string, handler func(RPCInvocation) string) (func(), error) {
	if room == nil || room.LocalParticipant == nil || method == "" || identity == "" || handler == nil {
		return nil, errors.New("avatar RPC room, method, identity, and handler are required")
	}
	sdkRPCHubs.Lock()
	hub := sdkRPCHubs.rooms[room]
	if hub == nil {
		hub = &sdkRPCHub{handlers: make(map[string]map[string]func(RPCInvocation) string)}
		sdkRPCHubs.rooms[room] = hub
	}
	identities := hub.handlers[method]
	if identities == nil {
		identities = make(map[string]func(RPCInvocation) string)
		if err := room.RegisterRpcCtxMethod(method, func(ctx context.Context, data []byte) ([]byte, error) {
			metadata := lksdk.RPCMetadataFromContext(ctx)
			caller := ""
			if metadata != nil {
				caller = metadata.CallerIdentity
			}
			sdkRPCHubs.Lock()
			current := sdkRPCHubs.rooms[room]
			var callback func(RPCInvocation) string
			if current != nil && current.handlers[method] != nil {
				callback = current.handlers[method][caller]
			}
			sdkRPCHubs.Unlock()
			if callback == nil {
				return []byte("reject"), nil
			}
			return []byte(callback(RPCInvocation{CallerIdentity: caller, Payload: string(data)})), nil
		}); err != nil {
			if len(hub.handlers) == 0 {
				delete(sdkRPCHubs.rooms, room)
			}
			sdkRPCHubs.Unlock()
			return nil, err
		}
		hub.handlers[method] = identities
	}
	if _, exists := identities[identity]; exists {
		sdkRPCHubs.Unlock()
		return nil, fmt.Errorf("avatar RPC handler already registered for method %q and identity %q", method, identity)
	}
	identities[identity] = handler
	sdkRPCHubs.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			sdkRPCHubs.Lock()
			hub := sdkRPCHubs.rooms[room]
			if hub != nil {
				delete(hub.handlers[method], identity)
				if len(hub.handlers[method]) == 0 {
					delete(hub.handlers, method)
					room.UnregisterRpcMethod(method)
				}
				if len(hub.handlers) == 0 {
					delete(sdkRPCHubs.rooms, room)
				}
			}
			sdkRPCHubs.Unlock()
		})
	}, nil
}

var _ ByteStreamWriter = sdkByteStreamWriter{}
