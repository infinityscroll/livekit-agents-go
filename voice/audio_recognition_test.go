// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/stt"
	"github.com/livekit/agents-go/vad"
)

func TestAudioRecognitionCommitsOncePerTurn(t *testing.T) {
	audio := stream.NewChannel[agents.AudioFrame](2)
	sttOutput := stream.NewChannel[STTNodeItem](8)
	commits := make(chan RecognizedTurn, 4)
	recognition, err := NewAudioRecognition(t.Context(), AudioRecognitionOptions{
		TurnDetection: TurnDetectionSTT,
		STTNode: func(context.Context, stream.Reader[agents.AudioFrame]) (stream.Reader[STTNodeItem], error) {
			return sttOutput, nil
		},
		Callbacks: AudioRecognitionCallbacks{OnCommit: func(turn RecognizedTurn) { commits <- turn }},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := recognition.Start(audio); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recognition.Close(context.Background()) })

	sendSTTTurn(t, sttOutput, "hello")
	first := receiveTurn(t, commits)
	if first.Transcript != "hello" {
		t.Fatalf("first transcript = %q", first.Transcript)
	}
	// Providers sometimes repeat a final and EOS during rollover. They must not
	// duplicate the conversation turn.
	if err := sttOutput.Send(t.Context(), STTEventItem(stt.SpeechEvent{Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{{Text: "hello"}}})); err != nil {
		t.Fatal(err)
	}
	if err := sttOutput.Send(t.Context(), STTEventItem(stt.SpeechEvent{Type: stt.EndOfSpeech})); err != nil {
		t.Fatal(err)
	}
	select {
	case turn := <-commits:
		t.Fatalf("duplicate turn committed: %#v", turn)
	case <-time.After(25 * time.Millisecond):
	}

	sendSTTTurn(t, sttOutput, "hello")
	second := receiveTurn(t, commits)
	if second.Transcript != "hello" {
		t.Fatalf("second transcript = %q", second.Transcript)
	}
}

func TestAudioRecognitionTranscriptionTimeoutAndCloseCancelsTimers(t *testing.T) {
	provider := newSessionTestVAD()
	timeout := 15 * time.Millisecond
	timedOut := make(chan struct{}, 1)
	commits := make(chan RecognizedTurn, 1)
	recognition, err := NewAudioRecognition(t.Context(), AudioRecognitionOptions{
		VAD: provider, TurnDetection: TurnDetectionVAD, TranscriptionTimeout: &timeout,
		Endpointing: &FixedEndpointing{minDelay: time.Second, maxDelay: time.Second},
		Callbacks: AudioRecognitionCallbacks{
			OnTranscriptionTimeout: func(time.Duration, time.Time) { timedOut <- struct{}{} },
			OnCommit:               func(turn RecognizedTurn) { commits <- turn },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	audio := stream.NewChannel[agents.AudioFrame](1)
	if err := recognition.Start(audio); err != nil {
		t.Fatal(err)
	}
	provider.emit(t, vad.Event{Type: vad.StartOfSpeech, Timestamp: time.Now()})
	provider.emit(t, vad.Event{Type: vad.EndOfSpeech, Timestamp: time.Now().Add(10 * time.Millisecond)})
	waitClosed(t, timedOut, "transcription timeout")
	if err := recognition.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case turn := <-commits:
		t.Fatalf("empty timeout turn committed: %#v", turn)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestAudioRecognitionBatchBufferIsBounded(t *testing.T) {
	provider := newSessionTestBatchSTT()
	recognition, err := NewAudioRecognition(t.Context(), AudioRecognitionOptions{
		STT: provider, TurnDetection: TurnDetectionSTT, QueueCapacity: 2, MaxBufferedFrames: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	audio := stream.NewChannel[agents.AudioFrame](4)
	if err := recognition.Start(audio); err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16000, 1)
	for range 3 {
		if err := audio.Send(t.Context(), frame); err != nil {
			t.Fatal(err)
		}
	}
	_ = audio.Close()
	deadline := time.After(time.Second)
	for provider.Calls() < 2 {
		select {
		case <-deadline:
			t.Fatalf("batch recognizer calls = %d", provider.Calls())
		case <-time.After(time.Millisecond):
		}
	}
	if err := recognition.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAudioRecognitionBatchExactBoundaryCommitsAtInputEnd(t *testing.T) {
	provider := newSessionTestBatchSTT()
	commits := make(chan RecognizedTurn, 1)
	recognition, err := NewAudioRecognition(t.Context(), AudioRecognitionOptions{
		STT: provider, TurnDetection: TurnDetectionSTT, MaxBufferedFrames: 2,
		Callbacks: AudioRecognitionCallbacks{OnCommit: func(turn RecognizedTurn) { commits <- turn }},
	})
	if err != nil {
		t.Fatal(err)
	}
	audio := stream.NewChannel[agents.AudioFrame](2)
	if err := recognition.Start(audio); err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16000, 1)
	for range 2 {
		if err := audio.Send(t.Context(), frame); err != nil {
			t.Fatal(err)
		}
	}
	_ = audio.Close()
	turn := receiveTurn(t, commits)
	if turn.Transcript != "chunk" {
		t.Fatalf("transcript = %q", turn.Transcript)
	}
	if provider.Calls() != 1 {
		t.Fatalf("recognizer calls = %d", provider.Calls())
	}
	if err := recognition.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAudioRecognitionCloseCancelsConcurrentProviderStart(t *testing.T) {
	provider := newBlockingStartSTT()
	recognition, err := NewAudioRecognition(context.Background(), AudioRecognitionOptions{STT: provider})
	if err != nil {
		t.Fatal(err)
	}
	audio := stream.NewChannel[agents.AudioFrame](1)
	startResult := make(chan error, 1)
	go func() { startResult <- recognition.Start(audio) }()
	waitClosed(t, provider.entered, "STT provider start")

	if err := recognition.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-startResult:
		if !errors.Is(err, ErrRecognitionClosed) {
			t.Fatalf("start error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent Start did not return after Close")
	}
}

func TestAudioRecognitionClearRestartsStreamingPipelines(t *testing.T) {
	provider := newSessionTestStreamingSTT()
	commits := make(chan RecognizedTurn, 1)
	recognition, err := NewAudioRecognition(t.Context(), AudioRecognitionOptions{
		STT: provider, TurnDetection: TurnDetectionSTT,
		Callbacks: AudioRecognitionCallbacks{OnCommit: func(turn RecognizedTurn) { commits <- turn }},
	})
	if err != nil {
		t.Fatal(err)
	}
	audio := stream.NewChannel[agents.AudioFrame](1)
	if err := recognition.Start(audio); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recognition.Close(context.Background()) })
	if provider.StreamCount() != 1 {
		t.Fatalf("initial STT streams = %d", provider.StreamCount())
	}
	if err := recognition.Clear(t.Context()); err != nil {
		t.Fatal(err)
	}
	if provider.StreamCount() != 2 {
		t.Fatalf("STT streams after clear = %d, want restart", provider.StreamCount())
	}
	provider.emitLatest(t, stt.SpeechEvent{Type: stt.StartOfSpeech})
	provider.emitLatest(t, stt.SpeechEvent{Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{{Text: "fresh"}}})
	provider.emitLatest(t, stt.SpeechEvent{Type: stt.EndOfSpeech})
	if turn := receiveTurn(t, commits); turn.Transcript != "fresh" {
		t.Fatalf("fresh turn transcript = %q", turn.Transcript)
	}
}

func sendSTTTurn(t *testing.T, output *stream.Channel[STTNodeItem], text string) {
	t.Helper()
	for _, event := range []stt.SpeechEvent{
		{Type: stt.StartOfSpeech},
		{Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{{Text: text, Confidence: 0.9}}},
		{Type: stt.EndOfSpeech},
	} {
		if err := output.Send(t.Context(), STTEventItem(event)); err != nil {
			t.Fatal(err)
		}
	}
}

func receiveTurn(t *testing.T, commits <-chan RecognizedTurn) RecognizedTurn {
	t.Helper()
	select {
	case turn := <-commits:
		return turn
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for recognized turn")
		return RecognizedTurn{}
	}
}

type sessionTestVAD struct {
	*vad.Base
	mu     sync.Mutex
	stream *vad.BaseStream
}

func newSessionTestVAD() *sessionTestVAD {
	return &sessionTestVAD{Base: vad.NewBase("test-vad", vad.Capabilities{UpdateInterval: 10 * time.Millisecond})}
}

func (v *sessionTestVAD) Stream(ctx context.Context) (vad.VADStream, error) {
	value := vad.NewBaseStream(ctx, 8)
	v.mu.Lock()
	v.stream = value
	v.mu.Unlock()
	return value, nil
}

func (v *sessionTestVAD) Close(context.Context) error { return nil }

func (v *sessionTestVAD) emit(t *testing.T, event vad.Event) {
	t.Helper()
	v.mu.Lock()
	value := v.stream
	v.mu.Unlock()
	if value == nil {
		t.Fatal("VAD stream was not created")
	}
	if err := value.Emit(t.Context(), event); err != nil {
		t.Fatal(err)
	}
}

type sessionTestBatchSTT struct {
	*stt.Base
	calls atomicCounter
}

type sessionTestStreamingSTT struct {
	*stt.Base
	mu      sync.Mutex
	streams []*stt.BaseStream
}

func newSessionTestStreamingSTT() *sessionTestStreamingSTT {
	return &sessionTestStreamingSTT{Base: stt.NewBase("streaming", "test", "streaming", stt.Capabilities{Streaming: true})}
}

func (*sessionTestStreamingSTT) Recognize(context.Context, []agents.AudioFrame, stt.RecognizeOptions) (stt.SpeechEvent, error) {
	return stt.SpeechEvent{}, errors.New("batch recognition is unsupported")
}

func (s *sessionTestStreamingSTT) Stream(ctx context.Context, _ stt.StreamOptions) (stt.SpeechStream, error) {
	value := stt.NewBaseStream(ctx, 8)
	s.mu.Lock()
	s.streams = append(s.streams, value)
	s.mu.Unlock()
	return value, nil
}

func (*sessionTestStreamingSTT) Close(context.Context) error { return nil }

func (s *sessionTestStreamingSTT) StreamCount() int {
	s.mu.Lock()
	count := len(s.streams)
	s.mu.Unlock()
	return count
}

func (s *sessionTestStreamingSTT) emitLatest(t *testing.T, event stt.SpeechEvent) {
	t.Helper()
	s.mu.Lock()
	value := s.streams[len(s.streams)-1]
	s.mu.Unlock()
	if err := value.Emit(t.Context(), event); err != nil {
		t.Fatal(err)
	}
}

type blockingStartSTT struct {
	*stt.Base
	entered chan struct{}
}

func newBlockingStartSTT() *blockingStartSTT {
	return &blockingStartSTT{
		Base:    stt.NewBase("blocking", "test", "blocking", stt.Capabilities{Streaming: true}),
		entered: make(chan struct{}),
	}
}

func (s *blockingStartSTT) Recognize(context.Context, []agents.AudioFrame, stt.RecognizeOptions) (stt.SpeechEvent, error) {
	return stt.SpeechEvent{}, errors.New("batch recognition is unsupported")
}

func (s *blockingStartSTT) Stream(ctx context.Context, _ stt.StreamOptions) (stt.SpeechStream, error) {
	close(s.entered)
	<-ctx.Done()
	return nil, context.Cause(ctx)
}

func (s *blockingStartSTT) Close(context.Context) error { return nil }

func newSessionTestBatchSTT() *sessionTestBatchSTT {
	return &sessionTestBatchSTT{Base: stt.NewBase("batch", "test", "batch", stt.Capabilities{})}
}

func (s *sessionTestBatchSTT) Recognize(context.Context, []agents.AudioFrame, stt.RecognizeOptions) (stt.SpeechEvent, error) {
	s.calls.Add(1)
	return stt.SpeechEvent{Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{{Text: "chunk"}}}, nil
}

func (s *sessionTestBatchSTT) Stream(context.Context, stt.StreamOptions) (stt.SpeechStream, error) {
	panic("batch STT Stream called")
}

func (s *sessionTestBatchSTT) Close(context.Context) error { return nil }
func (s *sessionTestBatchSTT) Calls() int                  { return int(s.calls.Load()) }

type atomicCounter struct {
	mu    sync.Mutex
	value int64
}

func (c *atomicCounter) Add(value int64) {
	c.mu.Lock()
	c.value += value
	c.mu.Unlock()
}

func (c *atomicCounter) Load() int64 {
	c.mu.Lock()
	value := c.value
	c.mu.Unlock()
	return value
}

var _ vad.VAD = (*sessionTestVAD)(nil)
var _ stt.STT = (*sessionTestBatchSTT)(nil)
var _ stt.STT = (*sessionTestStreamingSTT)(nil)
var _ stt.STT = (*blockingStartSTT)(nil)
