// SPDX-License-Identifier: Apache-2.0

package recorderio

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/voice"
)

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock() *manualClock { return &manualClock{now: time.Unix(1_700_000_000, 0)} }
func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *manualClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type queuedAudioInput struct {
	frames   *stream.Channel[agents.AudioFrame]
	attached atomic.Bool
}

func newQueuedAudioInput() *queuedAudioInput {
	return &queuedAudioInput{frames: stream.NewChannel[agents.AudioFrame](16)}
}
func (i *queuedAudioInput) Recv(ctx context.Context) (agents.AudioFrame, error) {
	return i.frames.Recv(ctx)
}
func (i *queuedAudioInput) SetAttached(value bool) { i.attached.Store(value) }
func (i *queuedAudioInput) OnAttached()            { i.attached.Store(true) }
func (i *queuedAudioInput) OnDetached()            { i.attached.Store(false) }
func (i *queuedAudioInput) Close() error           { return i.frames.Close() }

type parkedAudioInput struct {
	entered   chan struct{}
	once      sync.Once
	closeCall atomic.Int32
}

func (i *parkedAudioInput) Recv(ctx context.Context) (agents.AudioFrame, error) {
	i.once.Do(func() { close(i.entered) })
	<-ctx.Done()
	return agents.AudioFrame{}, context.Cause(ctx)
}
func (*parkedAudioInput) SetAttached(bool) {}
func (*parkedAudioInput) OnAttached()      {}
func (*parkedAudioInput) OnDetached()      {}
func (i *parkedAudioInput) Close() error {
	i.closeCall.Add(1)
	return nil
}

type testAudioOutput struct {
	sampleRate int
	canPause   bool
	hook       func(*testAudioOutput, context.Context, agents.AudioFrame) error
	clearHook  func(*testAudioOutput) error

	mu                 sync.Mutex
	open               bool
	captured, finished uint64
	last               voice.PlaybackFinishedEvent
	changed            chan struct{}
	attached           atomic.Bool
	startedEvents      agents.EventEmitter[voice.PlaybackStartedEvent]
	finishedEvents     agents.EventEmitter[voice.PlaybackFinishedEvent]
	waitStarted        chan struct{}
	waitOnce           sync.Once
}

func newTestAudioOutput(sampleRate int) *testAudioOutput {
	return &testAudioOutput{sampleRate: sampleRate, canPause: true, changed: make(chan struct{})}
}
func (o *testAudioOutput) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	if o.hook != nil {
		return o.hook(o, ctx, frame)
	}
	o.accept()
	return nil
}
func (o *testAudioOutput) accept() {
	o.mu.Lock()
	if !o.open {
		o.open = true
		o.captured++
		o.signalLocked()
	}
	o.mu.Unlock()
}
func (o *testAudioOutput) Flush(context.Context) error {
	o.AbandonOpenSegment()
	return nil
}
func (o *testAudioOutput) ClearBuffer(context.Context) error {
	if o.clearHook != nil {
		return o.clearHook(o)
	}
	return nil
}
func (o *testAudioOutput) Pause(context.Context) error  { return nil }
func (o *testAudioOutput) Resume(context.Context) error { return nil }
func (o *testAudioOutput) CanPause() bool               { return o.canPause }
func (o *testAudioOutput) SampleRate() int              { return o.sampleRate }
func (o *testAudioOutput) WaitForPlayout(ctx context.Context) (voice.PlaybackFinishedEvent, error) {
	if o.waitStarted != nil {
		o.waitOnce.Do(func() { close(o.waitStarted) })
	}
	o.mu.Lock()
	target := o.captured
	for o.finished < target {
		changed := o.changed
		o.mu.Unlock()
		select {
		case <-ctx.Done():
			return voice.PlaybackFinishedEvent{}, context.Cause(ctx)
		case <-changed:
		}
		o.mu.Lock()
	}
	last := o.last
	o.mu.Unlock()
	return last, nil
}
func (o *testAudioOutput) NotifyPlaybackFinished(event voice.PlaybackFinishedEvent) error {
	o.mu.Lock()
	if o.finished >= o.captured {
		o.mu.Unlock()
		return voice.ErrUnexpectedPlaybackFinished
	}
	o.finished++
	o.last = event
	o.open = false
	o.signalLocked()
	o.mu.Unlock()
	o.finishedEvents.Emit(event)
	return nil
}
func (o *testAudioOutput) AbandonOpenSegment() {
	o.mu.Lock()
	o.open = false
	o.mu.Unlock()
}
func (o *testAudioOutput) signalLocked() {
	close(o.changed)
	o.changed = make(chan struct{})
}
func (o *testAudioOutput) PendingPlayoutSegments() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.captured - o.finished
}
func (o *testAudioOutput) CapturedPlayoutSegments() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.captured
}
func (o *testAudioOutput) OnPlaybackStarted(fn func(voice.PlaybackStartedEvent)) func() {
	return o.startedEvents.Subscribe(fn)
}
func (o *testAudioOutput) OnPlaybackFinished(fn func(voice.PlaybackFinishedEvent)) func() {
	return o.finishedEvents.Subscribe(fn)
}
func (o *testAudioOutput) SetAttached(value bool) { o.attached.Store(value) }
func (o *testAudioOutput) OnAttached()            { o.attached.Store(true) }
func (o *testAudioOutput) OnDetached()            { o.attached.Store(false) }

type memoryEncoder struct {
	mu      sync.Mutex
	writes  [][]byte
	closed  int
	writeCh chan struct{}
}

type blockingCloseEncoder struct{ memoryEncoder }

func (*blockingCloseEncoder) Close(ctx context.Context) error {
	<-ctx.Done()
	return context.Cause(ctx)
}

type blockingWriteEncoder struct {
	started chan struct{}
	once    sync.Once
}

func (e *blockingWriteEncoder) WritePCM(ctx context.Context, _ []byte) error {
	e.once.Do(func() { close(e.started) })
	<-ctx.Done()
	return context.Cause(ctx)
}

func (*blockingWriteEncoder) Close(ctx context.Context) error { return context.Cause(ctx) }

func (e *memoryEncoder) WritePCM(ctx context.Context, value []byte) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	e.mu.Lock()
	e.writes = append(e.writes, append([]byte(nil), value...))
	if e.writeCh != nil {
		select {
		case e.writeCh <- struct{}{}:
		default:
		}
	}
	e.mu.Unlock()
	return nil
}
func (e *memoryEncoder) Close(context.Context) error {
	e.mu.Lock()
	e.closed++
	e.mu.Unlock()
	return nil
}
func (e *memoryEncoder) bytes() []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	var result []byte
	for _, write := range e.writes {
		result = append(result, write...)
	}
	return result
}

type recorderFixture struct {
	recorder *RecorderIO
	source   *queuedAudioInput
	input    *RecorderAudioInput
	output   *RecorderAudioOutput
	encoder  *memoryEncoder
	clock    *manualClock
	sink     *testAudioOutput
}

func newRecorderFixture(t *testing.T, sink voice.AudioOutput, mutate func(*RecorderOptions)) *recorderFixture {
	t.Helper()
	clock := newManualClock()
	encoder := &memoryEncoder{}
	options := RecorderOptions{
		Clock:          clock.Now,
		EncoderFactory: func(context.Context, string, int) (PCMEncoder, error) { return encoder, nil },
		WriteInterval:  time.Hour,
	}
	if mutate != nil {
		mutate(&options)
	}
	recorder, err := NewRecorderIO(options)
	if err != nil {
		t.Fatal(err)
	}
	source := newQueuedAudioInput()
	input, err := recorder.RecordInput(source)
	if err != nil {
		t.Fatal(err)
	}
	output, err := recorder.RecordOutput(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(t.Context(), filepath.Join(t.TempDir(), "nested", "recording.ogg")); err != nil {
		t.Fatal(err)
	}
	testSink, _ := sink.(*testAudioOutput)
	return &recorderFixture{recorder: recorder, source: source, input: input, output: output, encoder: encoder, clock: clock, sink: testSink}
}

func audioFrame(duration time.Duration, rate, channels int, values ...int16) agents.AudioFrame {
	samples := int(duration * time.Duration(rate) / time.Second)
	data := make([]int16, samples*channels)
	for sample := 0; sample < samples; sample++ {
		for channel := 0; channel < channels; channel++ {
			value := int16(0)
			if len(values) != 0 {
				value = values[min(channel, len(values)-1)]
			}
			data[sample*channels+channel] = value
		}
	}
	frame, _ := agents.NewAudioFrame(data, rate, channels)
	return frame
}

func TestRecorderLifecycleIsLazyAndIdempotent(t *testing.T) {
	var factoryCalls atomic.Int32
	recorder, err := NewRecorderIO(RecorderOptions{EncoderFactory: func(context.Context, string, int) (PCMEncoder, error) {
		factoryCalls.Add(1)
		return &memoryEncoder{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if factoryCalls.Load() != 0 {
		t.Fatal("constructor resolved an encoder eagerly")
	}
	if err := recorder.Start(t.Context(), filepath.Join(t.TempDir(), "out.ogg")); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("Start before wrappers = %v", err)
	}
	input := newQueuedAudioInput()
	if _, err := recorder.RecordInput(input); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.RecordOutput(newTestAudioOutput(48_000)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "a", "b", "out.ogg")
	if err := recorder.Start(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	if !recorder.Recording() {
		t.Fatal("recorder did not enter recording state")
	}
	if got, ok := recorder.OutputPath(); !ok || got != path {
		t.Fatalf("OutputPath = %q, %v", got, ok)
	}
	if err := recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Close(t.Context()); err != nil {
		t.Fatalf("idempotent Close = %v", err)
	}
	if factoryCalls.Load() != 0 {
		t.Fatal("empty recording started FFmpeg")
	}
}

func TestRecorderOptionValidation(t *testing.T) {
	for name, options := range map[string]RecorderOptions{
		"sample rate":       {SampleRate: -1},
		"write interval":    {WriteInterval: -1},
		"flush timeout":     {ClosePlayoutFlushTimeout: -1},
		"queue capacity":    {QueueCapacity: -1},
		"buffer bytes":      {MaxBufferedBytes: -1},
		"queue bytes":       {MaxQueuedBytes: -1},
		"pair does not fit": {MaxBufferedBytes: 100, MaxQueuedBytes: 199},
		"buffer frames":     {MaxBufferedFrames: -1},
		"queue frames":      {MaxQueuedFrames: -1},
		"frame pair":        {MaxBufferedFrames: 100, MaxQueuedFrames: 199},
		"queue item count":  {QueueCapacity: 3, MaxBufferedFrames: 1, MaxQueuedFrames: 2},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewRecorderIO(options); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestCloseBeforeStartReleasesWrappersAndIsTerminal(t *testing.T) {
	recorder, err := NewRecorderIO(RecorderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	input := newQueuedAudioInput()
	wrapper, err := recorder.RecordInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.RecordOutput(newTestAudioOutput(48_000)); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := wrapper.Recv(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("closed wrapper Recv = %v", err)
	}
	if err := recorder.Start(t.Context(), filepath.Join(t.TempDir(), "out.ogg")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Close = %v", err)
	}
}

func TestCloseCancelsParkedInputWithoutClosingOwnedSource(t *testing.T) {
	recorder, err := NewRecorderIO(RecorderOptions{
		WriteInterval:  time.Hour,
		EncoderFactory: func(context.Context, string, int) (PCMEncoder, error) { return &memoryEncoder{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	source := &parkedAudioInput{entered: make(chan struct{})}
	input, err := recorder.RecordInput(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.RecordOutput(newTestAudioOutput(48_000)); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(t.Context(), filepath.Join(t.TempDir(), "parked-input.ogg")); err != nil {
		t.Fatal(err)
	}
	received := make(chan error, 1)
	go func() {
		_, err := input.Recv(context.Background())
		received <- err
	}()
	<-source.entered
	if err := recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-received:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("parked Recv = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("parked input Recv leaked across Close")
	}
	if source.closeCall.Load() != 0 {
		t.Fatal("ownership-neutral wrapper closed its source")
	}
}

func TestRecorderEncodesInputLeftAndOutputRight(t *testing.T) {
	fixture := newRecorderFixture(t, newTestAudioOutput(48_000), nil)
	inputFrame := audioFrame(100*time.Millisecond, 48_000, 2, 1_000, 3_000)
	if err := fixture.source.frames.Send(t.Context(), inputFrame); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.input.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	outputFrame := audioFrame(100*time.Millisecond, 48_000, 1, 4_000)
	if err := fixture.output.CaptureFrame(t.Context(), outputFrame); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(100 * time.Millisecond)
	if err := fixture.sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 100 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	pcm := fixture.encoder.bytes()
	if len(pcm) != 4_800*4 {
		t.Fatalf("PCM bytes = %d, want %d", len(pcm), 4_800*4)
	}
	for sample := 0; sample < 4_800; sample++ {
		left := int16(binary.LittleEndian.Uint16(pcm[sample*4:]))
		right := int16(binary.LittleEndian.Uint16(pcm[sample*4+2:]))
		if left != 2_000 || right != 4_000 {
			t.Fatalf("sample %d = (%d,%d), want (2000,4000)", sample, left, right)
		}
	}
	if started, ok := fixture.recorder.RecordingStartedAt(); !ok || !started.Equal(time.Unix(1_700_000_000, 0)) {
		t.Fatalf("RecordingStartedAt = %v, %v", started, ok)
	}
}

func TestCloseFlushesTrailingInput(t *testing.T) {
	fixture := newRecorderFixture(t, newTestAudioOutput(48_000), nil)
	frame := audioFrame(20*time.Millisecond, 48_000, 1, 1234)
	if err := fixture.source.frames.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.input.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	pcm := fixture.encoder.bytes()
	if len(pcm) != 960*4 {
		t.Fatalf("tail PCM bytes = %d", len(pcm))
	}
	if got := int16(binary.LittleEndian.Uint16(pcm[0:])); got != 1234 {
		t.Fatalf("left sample = %d", got)
	}
	if got := int16(binary.LittleEndian.Uint16(pcm[2:])); got != 0 {
		t.Fatalf("right silence = %d", got)
	}
}

func TestPreStartOutputDoesNotPadRecordedInput(t *testing.T) {
	clock := newManualClock()
	encoder := &memoryEncoder{}
	recorder, err := NewRecorderIO(RecorderOptions{
		SampleRate: 1_000, Clock: clock.Now, WriteInterval: time.Hour,
		EncoderFactory: func(context.Context, string, int) (PCMEncoder, error) { return encoder, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	source := newQueuedAudioInput()
	input, err := recorder.RecordInput(source)
	if err != nil {
		t.Fatal(err)
	}
	sink := newTestAudioOutput(1_000)
	output, err := recorder.RecordOutput(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureFrame(t.Context(), audioFrame(time.Millisecond, 1_000, 1, 9)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Millisecond)
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(t.Context(), filepath.Join(t.TempDir(), "pre-start.ogg")); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Millisecond)
	frame := audioFrame(time.Millisecond, 1_000, 1, 5)
	if err := source.frames.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if pcm := encoder.bytes(); len(pcm) != 4 {
		t.Fatalf("pre-start playout added %d samples of input padding", len(pcm)/4-1)
	}
}

func TestCloseDropsUnfinishedPlayoutAfterBoundedTimeout(t *testing.T) {
	errorsSeen := make(chan error, 4)
	fixture := newRecorderFixture(t, newTestAudioOutput(48_000), func(options *RecorderOptions) {
		options.ClosePlayoutFlushTimeout = 10 * time.Millisecond
		options.OnError = func(err error) { errorsSeen <- err }
	})
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 7)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close took %v", elapsed)
	}
	select {
	case err := <-errorsSeen:
		if !errors.Is(err, ErrPlayoutFlushTimeout) {
			t.Fatalf("OnError = %v", err)
		}
	default:
		t.Fatal("missing playout timeout report")
	}
	if fixture.output.PendingPlayoutSegments() != 0 {
		t.Fatalf("pending after close = %d", fixture.output.PendingPlayoutSegments())
	}
}

func TestCloseAcceptsPlaybackFinishAlreadyInFlight(t *testing.T) {
	sink := newTestAudioOutput(48_000)
	sink.waitStarted = make(chan struct{})
	fixture := newRecorderFixture(t, sink, nil)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 4321)); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- fixture.recorder.Close(context.Background()) }()
	<-sink.waitStarted
	fixture.clock.Advance(20 * time.Millisecond)
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	pcm := fixture.encoder.bytes()
	if len(pcm) != 960*4 {
		t.Fatalf("final playout PCM bytes = %d", len(pcm))
	}
	if right := int16(binary.LittleEndian.Uint16(pcm[2:])); right != 4321 {
		t.Fatalf("final right sample = %d", right)
	}
}

func TestCloseRetainsInputCapturedWhileFinalPlayoutSettles(t *testing.T) {
	sink := newTestAudioOutput(1_000)
	sink.waitStarted = make(chan struct{})
	fixture := newRecorderFixture(t, sink, func(options *RecorderOptions) {
		options.SampleRate = 1_000
	})
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 1_000, 1, 4)); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- fixture.recorder.Close(context.Background()) }()
	<-sink.waitStarted
	input := audioFrame(20*time.Millisecond, 1_000, 1, 3)
	if err := fixture.source.frames.Send(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.input.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(20 * time.Millisecond)
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	pcm := fixture.encoder.bytes()
	if len(pcm) != 20*4 {
		t.Fatalf("PCM samples = %d, want 20", len(pcm)/4)
	}
	for sample := 0; sample < 20; sample++ {
		left := int16(binary.LittleEndian.Uint16(pcm[sample*4:]))
		right := int16(binary.LittleEndian.Uint16(pcm[sample*4+2:]))
		if left != 3 || right != 4 {
			t.Fatalf("sample %d = (%d,%d), want (3,4)", sample, left, right)
		}
	}
}

func TestInputPaddingKeepsStereoEndsAligned(t *testing.T) {
	fixture := newRecorderFixture(t, newTestAudioOutput(1_000), func(options *RecorderOptions) {
		options.SampleRate = 1_000
	})
	first := audioFrame(10*time.Millisecond, 1_000, 1, 100)
	if err := fixture.output.CaptureFrame(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(10 * time.Millisecond)
	if err := fixture.sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(10 * time.Millisecond)
	if err := fixture.source.frames.Send(t.Context(), audioFrame(10*time.Millisecond, 1_000, 1, 200)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.input.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(10*time.Millisecond, 1_000, 1, 300)); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(10 * time.Millisecond)
	if err := fixture.sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	pcm := fixture.encoder.bytes()
	if len(pcm) != 30*4 {
		t.Fatalf("aligned PCM samples = %d, want 30", len(pcm)/4)
	}
	for sample := 0; sample < 30; sample++ {
		left := int16(binary.LittleEndian.Uint16(pcm[sample*4:]))
		right := int16(binary.LittleEndian.Uint16(pcm[sample*4+2:]))
		var wantLeft, wantRight int16
		switch {
		case sample < 10:
			wantRight = 100
		case sample >= 20:
			wantLeft, wantRight = 200, 300
		}
		if left != wantLeft || right != wantRight {
			t.Fatalf("sample %d = (%d,%d), want (%d,%d)", sample, left, right, wantLeft, wantRight)
		}
	}
}

func TestConcurrentCloseIsIdempotent(t *testing.T) {
	fixture := newRecorderFixture(t, newTestAudioOutput(48_000), nil)
	const callers = 32
	errs := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs <- fixture.recorder.Close(context.Background())
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCloseDeadlineCancelsEncoderAndLaterCloseObservesResult(t *testing.T) {
	encoder := &blockingCloseEncoder{}
	recorder, err := NewRecorderIO(RecorderOptions{
		Clock:         newManualClock().Now,
		WriteInterval: time.Hour,
		EncoderFactory: func(context.Context, string, int) (PCMEncoder, error) {
			return encoder, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.RecordInput(newQueuedAudioInput()); err != nil {
		t.Fatal(err)
	}
	sink := newTestAudioOutput(48_000)
	output, err := recorder.RecordOutput(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(t.Context(), filepath.Join(t.TempDir(), "deadline.ogg")); err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureFrame(t.Context(), audioFrame(time.Millisecond, 48_000, 1, 1)); err != nil {
		t.Fatal(err)
	}
	// The manual clock does not advance, so use an interrupted zero-position
	// finish; input is added to make sure the encoder is still instantiated.
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	input := recorder.input
	if err := input.source.(*queuedAudioInput).frames.Send(t.Context(), audioFrame(time.Millisecond, 48_000, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	if err := recorder.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline Close = %v", err)
	}
	waitCtx, waitCancel := context.WithTimeout(t.Context(), time.Second)
	defer waitCancel()
	if err := recorder.Close(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("subsequent Close = %v", err)
	}
}

func TestCloseDeadlineCancelsCommitAlreadyBlockedByEncoder(t *testing.T) {
	encoder := &blockingWriteEncoder{started: make(chan struct{})}
	clock := newManualClock()
	recorder, err := NewRecorderIO(RecorderOptions{
		Clock: clock.Now, WriteInterval: time.Hour, QueueCapacity: 1,
		EncoderFactory: func(context.Context, string, int) (PCMEncoder, error) { return encoder, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.RecordInput(newQueuedAudioInput()); err != nil {
		t.Fatal(err)
	}
	sink := newTestAudioOutput(1_000)
	output, err := recorder.RecordOutput(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(t.Context(), filepath.Join(t.TempDir(), "blocked-commit.ogg")); err != nil {
		t.Fatal(err)
	}
	finishTurn := func() error {
		if err := output.CaptureFrame(t.Context(), audioFrame(time.Millisecond, 1_000, 1, 7)); err != nil {
			return err
		}
		clock.Advance(time.Millisecond)
		return sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: time.Millisecond})
	}
	if err := finishTurn(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-encoder.started:
	case <-time.After(time.Second):
		t.Fatal("encoder did not enter its blocked write")
	}
	if err := finishTurn(); err != nil {
		t.Fatal(err)
	}
	thirdFinished := make(chan error, 1)
	go func() { thirdFinished <- finishTurn() }()
	select {
	case err := <-thirdFinished:
		t.Fatalf("third commit unexpectedly bypassed the full queue: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	if err := recorder.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline Close = %v", err)
	}
	select {
	case err := <-thirdFinished:
		if err != nil {
			t.Fatalf("playback callback = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked commit leaked after Close deadline")
	}
	waitCtx, waitCancel := context.WithTimeout(t.Context(), time.Second)
	defer waitCancel()
	if err := recorder.Close(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("completed Close = %v", err)
	}
}

func TestBatchQueueBackpressureAndCancellation(t *testing.T) {
	queue := newBatchQueue(1, 10, 10)
	if err := queue.Send(t.Context(), recordBatch{bytes: 8}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := queue.Send(ctx, recordBatch{bytes: 3}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded Send = %v", err)
	}
	if _, err := queue.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := queue.Send(t.Context(), recordBatch{bytes: 10}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Send(t.Context(), recordBatch{bytes: 11}); !errors.Is(err, ErrBufferLimit) {
		t.Fatalf("oversized Send = %v", err)
	}
	if err := queue.Send(t.Context(), recordBatch{frames: 11}); !errors.Is(err, ErrBufferLimit) {
		t.Fatalf("frame-heavy Send = %v", err)
	}
	_ = queue.Close()
	if _, err := queue.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Recv(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("drained queue = %v", err)
	}
}

var _ voice.AudioInput = (*queuedAudioInput)(nil)
var _ voice.AudioOutput = (*testAudioOutput)(nil)
