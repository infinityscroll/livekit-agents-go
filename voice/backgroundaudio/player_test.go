// SPDX-License-Identifier: Apache-2.0

package backgroundaudio

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/voice"
)

func TestProbabilitySelectionMatchesAgentsJS(t *testing.T) {
	t.Parallel()
	first := Stream(&sliceReader{})
	second := Stream(&sliceReader{})
	values := []float64{.9, .1, .6}
	index := 0
	player, err := NewBackgroundAudioPlayer(BackgroundAudioPlayerOptions{Random: func() float64 {
		value := values[index]
		index++
		return value
	}})
	if err != nil {
		t.Fatal(err)
	}
	sound := Choose(Config(first).WithProbability(.2), Config(second).WithProbability(.3))
	if _, ok, err := player.selectSound(sound); err != nil || ok {
		t.Fatalf("selection above total = ok %v err %v", ok, err)
	}
	selected, ok, err := player.selectSound(sound)
	if err != nil || !ok || selected.source.reader != second.reader {
		t.Fatalf("weighted selection = %#v, %v, %v", selected, ok, err)
	}

	// A standalone AudioConfig ignores Probability, exactly like the non-array
	// agents-js path.
	selected, ok, err = player.selectSound(ConfiguredSound(Config(first).WithProbability(0)))
	if err != nil || !ok || selected.source.reader != first.reader {
		t.Fatalf("standalone config = %#v, %v, %v", selected, ok, err)
	}
}

func TestNoSelectionReturnsCompletedHandleAndPublicationLookup(t *testing.T) {
	t.Parallel()
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{Random: func() float64 { return .9 }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := newFakePublisher(newFakeSink())
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: publisher}); err != nil {
		t.Fatal(err)
	}
	handle, err := player.Play(ctx, Choose(Config(Stream(&sliceReader{})).WithProbability(.1)), false)
	if err != nil || !handle.Done() || handle.Err() != nil {
		t.Fatalf("silent handle = %#v, %v", handle, err)
	}
	publication, ok, err := player.Publication(ctx)
	if err != nil || !ok || publication.SID != "initial" {
		t.Fatalf("publication = %#v, %v, %v", publication, ok, err)
	}
	closePlayer(t, player)
}

func TestErrorCallbackPanicIsIsolated(t *testing.T) {
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{OnError: func(error) { panic("callback") }})
	player.reportError(errors.New("expected"))
}

func TestMixerSumsClipsAndSleepsWhenIdle(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	options, err := resolvePlayerOptions(BackgroundAudioPlayerOptions{BlockDuration: 10 * time.Millisecond, BufferDuration: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	mixer := newAudioMixer(ctx, sink, options)
	defer func() {
		cancel(ErrClosed)
		mixer.Close(ErrClosed)
		_ = mixer.Wait(context.Background())
	}()
	makeStream := func(id uint64, value int16) *mixStream {
		streamCtx, streamCancel := context.WithCancelCause(ctx)
		queue := stream.NewChannel[agents.AudioFrame](4)
		_ = queue.Send(streamCtx, testFrame(48_000, 1, 10, value))
		_ = queue.Close()
		return &mixStream{id: id, queue: queue, handle: newPlayHandle(nil), cancel: streamCancel}
	}
	if err := mixer.add(ctx, makeStream(1, 20_000)); err != nil {
		t.Fatal(err)
	}
	if err := mixer.add(ctx, makeStream(2, 20_000)); err != nil {
		t.Fatal(err)
	}
	frame := sink.waitFrame(t, time.Second)
	if len(frame.Data) != 480 {
		t.Fatalf("mixed block samples = %d", len(frame.Data))
	}
	for _, sample := range frame.Data {
		if sample != 32_767 {
			t.Fatalf("mixed sample = %d", sample)
		}
	}
	// Once terminal queues have drained, no silence blocks are emitted.
	time.Sleep(35 * time.Millisecond)
	if got := sink.count(); got != 1 {
		t.Fatalf("idle mixer emitted %d frames", got)
	}
}

func TestPlayerFileLoopStopAndStalePublicationCleanup(t *testing.T) {
	t.Parallel()
	path := writePCM16WAV(t, 48_000, 1, make([]int16, 480))
	sink := newFakeSink()
	publisher := newFakePublisher(sink)
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: publisher}); err != nil {
		t.Fatal(err)
	}
	handle, err := player.PlaySource(ctx, File(path), true)
	if err != nil {
		t.Fatal(err)
	}
	_ = sink.waitFrame(t, time.Second)
	_ = sink.waitFrame(t, time.Second)
	handle.Stop()
	if err := handle.WaitForPlayout(context.Background()); err != nil {
		t.Fatalf("manual stop: %v", err)
	}
	publisher.setCurrent(Publication{SID: "republished", Name: BackgroundAudioTrackName})
	closeContext, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := player.Close(closeContext); err != nil {
		t.Fatal(err)
	}
	if got := publisher.unpublishedSID(); got != "republished" {
		t.Fatalf("unpublished SID = %q", got)
	}
	if !sink.closed.Load() {
		t.Fatal("sink was not closed")
	}
	if err := player.Close(closeContext); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
}

func TestAmbientWeightedFileLoopsAutomatically(t *testing.T) {
	t.Parallel()
	path := writePCM16WAV(t, 48_000, 1, make([]int16, 480))
	sink := newFakeSink()
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{
		AmbientSound: Choose(Config(File(path)).WithProbability(1)),
		Random:       func() float64 { return 0 },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: newFakePublisher(sink)}); err != nil {
		t.Fatal(err)
	}
	_ = sink.waitFrame(t, time.Second)
	_ = sink.waitFrame(t, time.Second)
	closePlayer(t, player)
}

func TestStreamTimeoutIsReportedAndDoesNotLeakWorker(t *testing.T) {
	t.Parallel()
	var reported atomic.Int32
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{
		StreamTimeout: 20 * time.Millisecond,
		OnError: func(err error) {
			if errors.Is(err, ErrStreamTimeout) {
				reported.Add(1)
			}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: newFakePublisher(newFakeSink())}); err != nil {
		t.Fatal(err)
	}
	handle, err := player.PlaySource(ctx, Stream(blockingReader{}), false)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := handle.Wait(waitCtx); !errors.Is(err, ErrStreamTimeout) {
		t.Fatalf("play error = %v", err)
	}
	eventually(t, time.Second, func() bool { return reported.Load() == 1 })
	closePlayer(t, player)
}

func TestOutputTimeoutStopsMixerAndPlayout(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	sink.capture = func(ctx context.Context, _ agents.AudioFrame) error {
		<-ctx.Done()
		return context.Cause(ctx)
	}
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{OperationTimeout: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: newFakePublisher(sink)}); err != nil {
		t.Fatal(err)
	}
	handle, err := player.PlaySource(ctx, Stream(&sliceReader{frames: []agents.AudioFrame{testFrame(48_000, 1, 10, 1)}}), false)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := handle.Wait(waitCtx); !errors.Is(err, ErrOperationTimeout) {
		t.Fatalf("play error = %v", err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := player.Close(closeCtx); !errors.Is(err, ErrOperationTimeout) {
		t.Fatalf("close error = %v", err)
	}
}

func TestAgentThinkingStateStartsAndStopsThinkingSound(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	bus := voice.NewEventBus(context.Background(), voice.EventBusOptions{})
	defer bus.Close()
	session := fakeSession{bus: bus}
	source := StreamFactorySource(func(context.Context) (FrameReader, error) {
		return &infiniteReader{frame: testFrame(48_000, 1, 10, 7)}, nil
	})
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{ThinkingSound: SourceSound(source)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: newFakePublisher(sink), AgentSession: session}); err != nil {
		t.Fatal(err)
	}
	if err := bus.PublishAndWait(ctx, voice.NewAgentStateChangedEvent(voice.AgentStateListening, voice.AgentStateThinking, time.Now())); err != nil {
		t.Fatal(err)
	}
	_ = sink.waitFrame(t, time.Second)
	if err := bus.PublishAndWait(ctx, voice.NewAgentStateChangedEvent(voice.AgentStateThinking, voice.AgentStateSpeaking, time.Now())); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool {
		player.mu.Lock()
		defer player.mu.Unlock()
		return player.active == 0 && player.thinkingHandle != nil && player.thinkingHandle.Done()
	})
	closePlayer(t, player)
}

func TestBuiltinResolverIsLazyAndCleanupOwnedByPlayer(t *testing.T) {
	t.Parallel()
	path := writePCM16WAV(t, 48_000, 1, make([]int16, 480))
	var resolved, cleaned atomic.Int32
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{
		BuiltinResolver: func(context.Context, BuiltinAudioClip) (string, func() error, error) {
			resolved.Add(1)
			return path, func() error { cleaned.Add(1); return nil }, nil
		},
	})
	if resolved.Load() != 0 {
		t.Fatal("resolver ran during construction")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := newFakeSink()
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: newFakePublisher(sink)}); err != nil {
		t.Fatal(err)
	}
	handle, err := player.PlaySource(ctx, Builtin(BuiltinKeyboardTyping), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if resolved.Load() != 1 {
		t.Fatalf("resolver calls = %d", resolved.Load())
	}
	closePlayer(t, player)
	if cleaned.Load() != 1 {
		t.Fatalf("cleanup calls = %d", cleaned.Load())
	}
}

func TestAsyncStreamOwnershipIsExplicit(t *testing.T) {
	t.Parallel()
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: newFakePublisher(newFakeSink())}); err != nil {
		t.Fatal(err)
	}
	owned := &closableSliceReader{sliceReader: sliceReader{frames: []agents.AudioFrame{testFrame(48_000, 1, 10, 1)}}}
	handle, err := player.PlaySource(ctx, OwnedStream(owned), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, owned.closed.Load)

	borrowed := &closableSliceReader{sliceReader: sliceReader{frames: []agents.AudioFrame{testFrame(48_000, 1, 10, 1)}}}
	handle, err = player.PlaySource(ctx, Stream(borrowed), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if borrowed.closed.Load() {
		t.Fatal("caller-owned stream was closed")
	}
	closePlayer(t, player)
}

func TestConcurrentPlayStopAndClose(t *testing.T) {
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{MaxStreams: 64})
	ctx, cancel := context.WithCancel(context.Background())
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: newFakePublisher(newFakeSink())}); err != nil {
		t.Fatal(err)
	}
	source := StreamFactorySource(func(context.Context) (FrameReader, error) {
		return &infiniteReader{frame: testFrame(48_000, 1, 10, 1)}, nil
	})
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			handle, err := player.Play(ctx, SourceSound(source), false)
			if err == nil {
				handle.Stop()
				_ = handle.Wait(context.Background())
			}
		}()
	}
	wait.Wait()
	closeContext, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := player.Close(closeContext); err != nil {
		t.Fatal(err)
	}
	cancel()
}

func TestCloseBeforeStartAndStartValidation(t *testing.T) {
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{})
	if err := player.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := player.Start(context.Background(), BackgroundAudioStartOptions{Publisher: newFakePublisher(newFakeSink())}); !errors.Is(err, ErrClosed) {
		t.Fatalf("start after close = %v", err)
	}
	player = newTestPlayer(t, BackgroundAudioPlayerOptions{})
	if err := player.Start(context.Background(), BackgroundAudioStartOptions{}); err == nil {
		t.Fatal("nil publisher accepted")
	}
}

func TestStartContextCancellationAutomaticallyUnpublishes(t *testing.T) {
	t.Parallel()
	sink := newFakeSink()
	publisher := newFakePublisher(sink)
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	if err := player.Start(ctx, BackgroundAudioStartOptions{Publisher: publisher}); err != nil {
		t.Fatal(err)
	}
	cancel()
	eventually(t, time.Second, func() bool {
		player.mu.Lock()
		closed := player.state == playerClosed
		player.mu.Unlock()
		return closed && sink.closed.Load() && publisher.unpublishedSID() == "initial"
	})
	if err := player.Close(context.Background()); err != nil {
		t.Fatalf("close after automatic cleanup: %v", err)
	}
}

func TestMixerIngressBufferIsExactlyFourHundredMilliseconds(t *testing.T) {
	t.Parallel()
	player := newTestPlayer(t, BackgroundAudioPlayerOptions{BlockDuration: 100 * time.Millisecond, BufferDuration: 400 * time.Millisecond})
	ctx, cancel := context.WithCancelCause(context.Background())
	reader := &countingReader{frame: testFrame(48_000, 1, 100, 1)}
	queue := stream.NewChannel[agents.AudioFrame](4)
	value := &mixStream{queue: queue, handle: newPlayHandle(nil), cancel: cancel}
	player.workers.Add()
	go player.feedStream(ctx, openedSource{reader: reader}, value, 1, func() bool { return true })
	eventually(t, time.Second, func() bool { return reader.count.Load() == 5 })
	// Four blocks are stored; the fifth has been pulled and is applying bounded
	// backpressure in Send. No sixth frame may be requested.
	time.Sleep(20 * time.Millisecond)
	if got := reader.count.Load(); got != 5 || queue.Cap() != 4 || queue.Len() != 4 {
		t.Fatalf("reader=%d queue=%d/%d", got, queue.Len(), queue.Cap())
	}
	cancel(ErrClosed)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := player.workers.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkMixerTwoStreams(b *testing.B) {
	sink := newFakeSink()
	options, _ := resolvePlayerOptions(BackgroundAudioPlayerOptions{})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(ErrClosed)
	mixer := &audioMixer{ctx: ctx, sink: sink, blockSamples: 4_800, operationTimeout: options.operationTimeout}
	active := map[uint64]*mixStream{}
	for id := uint64(1); id <= 2; id++ {
		queue := stream.NewChannel[agents.AudioFrame](1)
		active[id] = &mixStream{id: id, queue: queue, handle: newPlayHandle(nil), cancel: func(error) {}}
	}
	frame := testFrame(48_000, 1, 100, 100)
	b.ReportAllocs()
	for b.Loop() {
		for _, value := range active {
			_ = value.queue.TrySend(frame)
		}
		if err := mixer.mixOne(active); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewBackgroundAudioPlayer(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		player, err := NewBackgroundAudioPlayer(BackgroundAudioPlayerOptions{})
		if err != nil {
			b.Fatal(err)
		}
		_ = player.Close(context.Background())
	}
}

func BenchmarkMixerTwoStreamsImmediate(b *testing.B) {
	options, _ := resolvePlayerOptions(BackgroundAudioPlayerOptions{})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(ErrClosed)
	mixer := &audioMixer{ctx: ctx, sink: discardImmediateSink{}, blockSamples: 4_800, operationTimeout: options.operationTimeout}
	active := map[uint64]*mixStream{}
	for id := uint64(1); id <= 2; id++ {
		queue := stream.NewChannel[agents.AudioFrame](1)
		active[id] = &mixStream{id: id, queue: queue, handle: newPlayHandle(nil), cancel: func(error) {}}
	}
	frame := testFrame(48_000, 1, 100, 100)
	// Warm reusable accumulator/output buffers outside the measurement.
	for _, value := range active {
		_ = value.queue.TrySend(frame)
	}
	_ = mixer.mixOne(active)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, value := range active {
			_ = value.queue.TrySend(frame)
		}
		if err := mixer.mixOne(active); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrameConverter48kMonoHot(b *testing.B) {
	frame := testFrame(48_000, 1, 10, 1_000)
	converter := newFrameConverter(480, 1)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = converter.push(frame)
	}
}

type fakeSink struct {
	mu      sync.Mutex
	frames  []agents.AudioFrame
	notify  chan struct{}
	capture func(context.Context, agents.AudioFrame) error
	closed  atomic.Bool
}

func newFakeSink() *fakeSink { return &fakeSink{notify: make(chan struct{}, 128)} }

func (s *fakeSink) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	if s.capture != nil {
		return s.capture(ctx, frame)
	}
	copyFrame := frame
	copyFrame.Data = append([]int16(nil), frame.Data...)
	s.mu.Lock()
	s.frames = append(s.frames, copyFrame)
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return context.Cause(ctx)
}

func (s *fakeSink) Close(context.Context) error { s.closed.Store(true); return nil }

func (s *fakeSink) waitFrame(t *testing.T, timeout time.Duration) agents.AudioFrame {
	t.Helper()
	select {
	case <-s.notify:
		s.mu.Lock()
		frame := s.frames[len(s.frames)-1]
		s.mu.Unlock()
		return frame
	case <-time.After(timeout):
		t.Fatal("timed out waiting for mixed frame")
		return agents.AudioFrame{}
	}
}

func (s *fakeSink) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.frames) }

type fakePublisher struct {
	mu          sync.Mutex
	sink        FrameSink
	current     Publication
	unpublished string
}

func newFakePublisher(sink FrameSink) *fakePublisher {
	return &fakePublisher{sink: sink, current: Publication{SID: "initial", Name: BackgroundAudioTrackName}}
}

func (p *fakePublisher) Publish(ctx context.Context, request PublishRequest) (FrameSink, Publication, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, Publication{}, err
	}
	if request.Name != BackgroundAudioTrackName || request.SampleRate != 48_000 || request.Channels != 1 {
		return nil, Publication{}, errors.New("unexpected publish request")
	}
	p.mu.Lock()
	publication := p.current
	p.mu.Unlock()
	return p.sink, publication, nil
}

func (p *fakePublisher) CurrentPublication(ctx context.Context, _ string) (Publication, bool, error) {
	if err := context.Cause(ctx); err != nil {
		return Publication{}, false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current, p.current.SID != "", nil
}

func (p *fakePublisher) Unpublish(ctx context.Context, sid string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	p.unpublished = sid
	p.current = Publication{}
	p.mu.Unlock()
	return nil
}

func (p *fakePublisher) setCurrent(publication Publication) {
	p.mu.Lock()
	p.current = publication
	p.mu.Unlock()
}
func (p *fakePublisher) unpublishedSID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.unpublished
}

type blockingReader struct{}

func (blockingReader) Recv(ctx context.Context) (agents.AudioFrame, error) {
	<-ctx.Done()
	return agents.AudioFrame{}, context.Cause(ctx)
}

type infiniteReader struct{ frame agents.AudioFrame }

func (r *infiniteReader) Recv(ctx context.Context) (agents.AudioFrame, error) {
	select {
	case <-ctx.Done():
		return agents.AudioFrame{}, context.Cause(ctx)
	default:
		return r.frame, nil
	}
}

type countingReader struct {
	frame agents.AudioFrame
	count atomic.Int32
}

type closableSliceReader struct {
	sliceReader
	closed atomic.Bool
}

func (r *closableSliceReader) Close() error { r.closed.Store(true); return nil }

func (r *countingReader) Recv(ctx context.Context) (agents.AudioFrame, error) {
	if err := context.Cause(ctx); err != nil {
		return agents.AudioFrame{}, err
	}
	r.count.Add(1)
	return r.frame, nil
}

type discardImmediateSink struct{}

func (discardImmediateSink) CaptureFrame(context.Context, agents.AudioFrame) error { return nil }
func (discardImmediateSink) immediateCapture()                                     {}

type fakeSession struct{ bus *voice.EventBus }

func (s fakeSession) Subscribe(options voice.EventSubscriptionOptions) (*voice.EventSubscription, error) {
	return s.bus.Subscribe(options)
}

func newTestPlayer(t *testing.T, options BackgroundAudioPlayerOptions) *BackgroundAudioPlayer {
	t.Helper()
	if options.BlockDuration == 0 {
		options.BlockDuration = 10 * time.Millisecond
	}
	if options.BufferDuration == 0 {
		options.BufferDuration = 40 * time.Millisecond
	}
	if options.TaskTimeout == 0 {
		options.TaskTimeout = 500 * time.Millisecond
	}
	player, err := NewBackgroundAudioPlayer(options)
	if err != nil {
		t.Fatal(err)
	}
	return player
}

func closePlayer(t *testing.T, player *BackgroundAudioPlayer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := player.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not satisfied")
		}
		time.Sleep(time.Millisecond)
	}
}

func writePCM16WAV(t *testing.T, sampleRate, channels int, samples []int16) string {
	t.Helper()
	path := t.TempDir() + "/audio.wav"
	dataBytes := len(samples) * 2
	data := make([]byte, 44+dataBytes)
	copy(data[0:4], "RIFF")
	binary.LittleEndian.PutUint32(data[4:8], uint32(36+dataBytes))
	copy(data[8:12], "WAVE")
	copy(data[12:16], "fmt ")
	binary.LittleEndian.PutUint32(data[16:20], 16)
	binary.LittleEndian.PutUint16(data[20:22], 1)
	binary.LittleEndian.PutUint16(data[22:24], uint16(channels))
	binary.LittleEndian.PutUint32(data[24:28], uint32(sampleRate))
	binary.LittleEndian.PutUint32(data[28:32], uint32(sampleRate*channels*2))
	binary.LittleEndian.PutUint16(data[32:34], uint16(channels*2))
	binary.LittleEndian.PutUint16(data[34:36], 16)
	copy(data[36:40], "data")
	binary.LittleEndian.PutUint32(data[40:44], uint32(dataBytes))
	for index, sample := range samples {
		binary.LittleEndian.PutUint16(data[44+index*2:], uint16(sample))
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

var _ AgentSession = fakeSession{}
var _ FrameReader = blockingReader{}
var _ FrameReader = (*infiniteReader)(nil)
var _ = io.EOF
