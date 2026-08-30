// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/tts"
)

type gatedVoiceLLM struct {
	*llm.Base
	release chan struct{}
	once    sync.Once
}

func newGatedVoiceLLM() *gatedVoiceLLM {
	return &gatedVoiceLLM{Base: llm.NewBase("gated-llm", "test", "gated"), release: make(chan struct{})}
}

func (m *gatedVoiceLLM) Chat(ctx context.Context, options llm.ChatOptions) (llm.LLMStream, error) {
	output := llm.NewBaseStream(ctx, m.Base, options, 2)
	go func() {
		if err := output.Emit(output.Context(), llm.ChatChunk{Delta: &llm.ChoiceDelta{Role: llm.RoleAssistant, Content: "first "}}); err != nil {
			output.Finish(err)
			return
		}
		select {
		case <-m.release:
		case <-output.Context().Done():
			output.Finish(context.Cause(output.Context()))
			return
		}
		if err := output.Emit(output.Context(), llm.ChatChunk{Delta: &llm.ChoiceDelta{Role: llm.RoleAssistant, Content: "second"}}); err != nil {
			output.Finish(err)
			return
		}
		output.Finish(nil)
	}()
	return output, nil
}

func (m *gatedVoiceLLM) unblock() { m.once.Do(func() { close(m.release) }) }

type observingStreamingTTS struct {
	*tts.Base
	firstText chan string
}

type failingPushStreamingTTS struct {
	*tts.Base
	err error
}

type failingPushSynthesis struct {
	ctx       context.Context
	err       error
	closed    chan struct{}
	closeOnce sync.Once
}

func (s *failingPushSynthesis) Recv(ctx context.Context) (tts.SynthesizedAudio, error) {
	select {
	case <-ctx.Done():
		return tts.SynthesizedAudio{}, context.Cause(ctx)
	case <-s.ctx.Done():
		return tts.SynthesizedAudio{}, context.Cause(s.ctx)
	case <-s.closed:
		return tts.SynthesizedAudio{}, io.EOF
	}
}

func (s *failingPushSynthesis) PushText(context.Context, string) error { return s.err }
func (*failingPushSynthesis) Flush(context.Context) error              { return nil }
func (*failingPushSynthesis) EndInput() error                          { return nil }
func (s *failingPushSynthesis) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func newFailingPushStreamingTTS(err error) *failingPushStreamingTTS {
	return &failingPushStreamingTTS{
		Base: tts.NewBase("failing-streaming-tts", "test", "failing", 16_000, 1, tts.Capabilities{Streaming: true}),
		err:  err,
	}
}

func (*failingPushStreamingTTS) Synthesize(context.Context, string, tts.SynthesizeOptions) (tts.ChunkedStream, error) {
	return nil, errors.New("chunked synthesis is unsupported")
}

func (t *failingPushStreamingTTS) Stream(ctx context.Context, _ tts.StreamOptions) (tts.SynthesizeStream, error) {
	return &failingPushSynthesis{ctx: ctx, err: t.err, closed: make(chan struct{})}, nil
}

func (*failingPushStreamingTTS) Close(context.Context) error { return nil }

func newObservingStreamingTTS() *observingStreamingTTS {
	return &observingStreamingTTS{
		Base:      tts.NewBase("streaming-tts", "test", "streaming", 16_000, 1, tts.Capabilities{Streaming: true}),
		firstText: make(chan string, 1),
	}
}

func (*observingStreamingTTS) Synthesize(context.Context, string, tts.SynthesizeOptions) (tts.ChunkedStream, error) {
	return nil, errors.New("chunked synthesis is unsupported")
}

func (t *observingStreamingTTS) Stream(ctx context.Context, _ tts.StreamOptions) (tts.SynthesizeStream, error) {
	output := tts.NewBaseSynthesizeStream(ctx, 2)
	go func() {
		for {
			input, err := output.Inputs().Recv(output.Context())
			if err != nil {
				if errors.Is(err, io.EOF) {
					frame, _ := agents.NewAudioFrame(make([]int16, 160), 16_000, 1)
					_ = output.Emit(output.Context(), tts.SynthesizedAudio{Frame: frame, Final: true})
					output.Finish(nil)
				} else {
					output.Finish(err)
				}
				return
			}
			if input.Text != "" {
				select {
				case t.firstText <- input.Text:
				default:
				}
			}
		}
	}()
	return output, nil
}

func (*observingStreamingTTS) Close(context.Context) error { return nil }

func TestGenerationStreamsFirstLLMDeltaIntoTTS(t *testing.T) {
	model := newGatedVoiceLLM()
	t.Cleanup(model.unblock)
	speech := newObservingStreamingTTS()
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		LLM: model, TTS: speech, DisableUserAwayTimeout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "stream_agent"})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case first := <-speech.firstText:
		if first != "first " {
			t.Fatalf("first TTS input = %q", first)
		}
	case <-time.After(time.Second):
		t.Fatal("TTS did not receive the first delta before LLM completion")
	}
	model.unblock()
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	var assistant string
	for _, item := range session.ChatContext().Items() {
		if message, ok := item.(*llm.ChatMessage); ok && message.Role == llm.RoleAssistant {
			assistant, _ = message.RawTextContent()
		}
	}
	if assistant != "first second" {
		t.Fatalf("assistant text = %q", assistant)
	}
}

func TestGenerationReportsStreamingTTSPushFailureWithoutIdleDelay(t *testing.T) {
	failure := errors.New("TTS rejected streamed text")
	speech := newFailingPushStreamingTTS(failure)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		TTS: speech, TTSReadIdleTimeout: 5 * time.Second, DisableUserAwayTimeout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := MustAgent(AgentOptions[struct{}]{
		ID: "tts_failure_agent",
		Hooks: AgentHooks[struct{}]{
			LLMNode: func(context.Context, *AgentContext[struct{}], *llm.ChatContext, *llm.Context, ModelSettings) (stream.Reader[LLMNodeItem], error) {
				return stream.FromSlice([]LLMNodeItem{LLMTextItem("hello")}), nil
			},
		},
	})
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = handle.Wait(waitCtx)
	if !errors.Is(err, failure) {
		t.Fatalf("generation error = %v, want %v", err, failure)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("streaming TTS failure took %s", elapsed)
	}
}

func TestGenerationLLMFlushCreatesOrderedSpeechSegments(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	agent := MustAgent(AgentOptions[struct{}]{
		ID: "flush_agent",
		Hooks: AgentHooks[struct{}]{
			LLMNode: func(context.Context, *AgentContext[struct{}], *llm.ChatContext, *llm.Context, ModelSettings) (stream.Reader[LLMNodeItem], error) {
				return stream.FromSlice([]LLMNodeItem{
					LLMTextItem("first"), LLMFlushItem(), LLMTextItem("second"),
				}), nil
			},
		},
	})
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	var assistant []string
	for _, item := range session.ChatContext().Items() {
		message, ok := item.(*llm.ChatMessage)
		if !ok || message.Role != llm.RoleAssistant {
			continue
		}
		text, _ := message.RawTextContent()
		assistant = append(assistant, text)
	}
	if len(assistant) != 2 || assistant[0] != "first" || assistant[1] != "second" {
		t.Fatalf("assistant segments = %#v", assistant)
	}
}

var _ llm.LLM = (*gatedVoiceLLM)(nil)
var _ tts.TTS = (*observingStreamingTTS)(nil)
var _ tts.TTS = (*failingPushStreamingTTS)(nil)
var _ tts.SynthesizeStream = (*failingPushSynthesis)(nil)
