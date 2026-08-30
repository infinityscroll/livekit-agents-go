// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stream"
)

func realtimeFrame(sample int16) agents.AudioFrame {
	frame, _ := agents.NewAudioFrame([]int16{sample}, 16_000, 1)
	return frame
}

func TestRealtimeAudioInputReplacementAndClose(t *testing.T) {
	first := stream.NewChannel[agents.AudioFrame](1)
	second := stream.NewChannel[agents.AudioFrame](1)
	var mu sync.Mutex
	var samples []int16
	input, err := NewRealtimeAudioInput(context.Background(), func(_ context.Context, frame agents.AudioFrame) error {
		mu.Lock()
		samples = append(samples, frame.Data[0])
		mu.Unlock()
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := input.Replace(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := first.Send(context.Background(), realtimeFrame(1)); err != nil {
		t.Fatal(err)
	}
	waitForRealtimeSamples(t, &mu, &samples, 1)
	if err := input.Replace(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err := second.Send(context.Background(), realtimeFrame(2)); err != nil {
		t.Fatal(err)
	}
	waitForRealtimeSamples(t, &mu, &samples, 2)
	if err := input.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(context.Background()); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if err := input.Replace(context.Background(), first); !errors.Is(err, stream.ErrClosed) {
		t.Fatalf("replace after close = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(samples) != 2 || samples[0] != 1 || samples[1] != 2 {
		t.Fatalf("samples = %v", samples)
	}
}

func waitForRealtimeSamples(t *testing.T, mu *sync.Mutex, samples *[]int16, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ready := len(*samples) >= count
		mu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("audio pump did not deliver frame")
}

type blockingAudioReader struct{}

func (blockingAudioReader) Recv(ctx context.Context) (agents.AudioFrame, error) {
	<-ctx.Done()
	return agents.AudioFrame{}, context.Cause(ctx)
}

func TestRealtimeAudioInputReplaceHonorsDeadline(t *testing.T) {
	stuck := &ignoresCancellationAudioReader{release: make(chan struct{})}
	input, err := NewRealtimeAudioInput(context.Background(), func(context.Context, agents.AudioFrame) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := input.Replace(context.Background(), stuck); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := input.Replace(ctx, blockingAudioReader{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("replace = %v", err)
	}
	close(stuck.release)
	if err := input.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type ignoresCancellationAudioReader struct{ release chan struct{} }

func (r *ignoresCancellationAudioReader) Recv(context.Context) (agents.AudioFrame, error) {
	<-r.release
	return agents.AudioFrame{}, io.EOF
}

func TestRealtimeSessionEvents(t *testing.T) {
	var events RealtimeSessionEvents
	var stopped, errorsSeen int
	unsubscribe := events.OnInputSpeechStopped(func(event InputSpeechStoppedEvent) {
		if event.UserTranscriptionEnabled {
			stopped++
		}
	})
	events.OnError(func(event RealtimeModelError) {
		if event.Type == "realtime_model_error" && !event.Timestamp.IsZero() {
			errorsSeen++
		}
	})
	events.EmitInputSpeechStopped(InputSpeechStoppedEvent{UserTranscriptionEnabled: true})
	events.EmitError(RealtimeModelError{Err: errors.New("provider")})
	unsubscribe()
	events.EmitInputSpeechStopped(InputSpeechStoppedEvent{UserTranscriptionEnabled: true})
	if stopped != 1 || errorsSeen != 1 {
		t.Fatalf("stopped=%d errors=%d", stopped, errorsSeen)
	}
}

func TestRealtimeModelBase(t *testing.T) {
	base := NewRealtimeModelBase(RealtimeCapabilities{AudioOutput: true}, "", "", "")
	if base.Label() != "RealtimeModel" || base.Provider() != "unknown" || base.Model() != "unknown" || !base.Capabilities().AudioOutput {
		t.Fatalf("unexpected defaults: %q %q %q %#v", base.Label(), base.Provider(), base.Model(), base.Capabilities())
	}
	base.SetModel("live")
	if base.Model() != "live" {
		t.Fatalf("model = %q", base.Model())
	}
}

// Compile-time surface oracle for provider implementations.
type fakeRealtimeSession struct {
	RealtimeSessionEvents
	model RealtimeModel
}

func (s *fakeRealtimeSession) RealtimeModel() RealtimeModel { return s.model }
func (*fakeRealtimeSession) ChatContext() *ChatContext      { return EmptyChatContext() }
func (*fakeRealtimeSession) Tools() *Context                { return EmptyToolContext() }
func (*fakeRealtimeSession) UpdateInstructions(context.Context, string) error {
	return nil
}
func (*fakeRealtimeSession) UpdateChatContext(context.Context, *ChatContext) error { return nil }
func (*fakeRealtimeSession) UpdateTools(context.Context, *Context) error           { return nil }
func (*fakeRealtimeSession) UpdateOptions(context.Context, RealtimeUpdateOptions) error {
	return nil
}
func (*fakeRealtimeSession) PushAudio(context.Context, agents.AudioFrame) error { return nil }
func (*fakeRealtimeSession) GenerateReply(context.Context, GenerateRealtimeReplyOptions) (GenerationCreatedEvent, error) {
	return GenerationCreatedEvent{}, nil
}
func (*fakeRealtimeSession) CommitAudio(context.Context) error { return nil }
func (*fakeRealtimeSession) ClearAudio(context.Context) error  { return nil }
func (*fakeRealtimeSession) Interrupt(context.Context) error   { return nil }
func (*fakeRealtimeSession) Truncate(context.Context, TruncateRealtimeMessageOptions) error {
	return nil
}
func (*fakeRealtimeSession) StartUserActivity() {}
func (*fakeRealtimeSession) SetInputAudioStream(context.Context, stream.Reader[agents.AudioFrame]) error {
	return nil
}
func (*fakeRealtimeSession) Close(context.Context) error { return nil }

var _ RealtimeSession = (*fakeRealtimeSession)(nil)
var _ = metrics.Realtime{}
