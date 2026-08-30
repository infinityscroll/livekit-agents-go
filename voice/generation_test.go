// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/tts"
)

type sessionLLMResponse struct {
	text  string
	calls []*llm.FunctionCall
	err   error
}

type sessionTestLLM struct {
	*llm.Base
	mu        sync.Mutex
	responses []sessionLLMResponse
	calls     int
}

func newSessionTestLLM(responses []sessionLLMResponse) *sessionTestLLM {
	return &sessionTestLLM{Base: llm.NewBase("test-llm", "test", "test-model"), responses: responses}
}

func (m *sessionTestLLM) Chat(ctx context.Context, options llm.ChatOptions) (llm.LLMStream, error) {
	m.mu.Lock()
	index := m.calls
	m.calls++
	if index >= len(m.responses) {
		m.mu.Unlock()
		return nil, errors.New("unexpected LLM call")
	}
	response := m.responses[index]
	m.mu.Unlock()
	if response.err != nil {
		return nil, response.err
	}
	output := llm.NewBaseStream(ctx, m.Base, options, 4)
	go func() {
		chunk := llm.ChatChunk{ID: "request", Delta: &llm.ChoiceDelta{Role: llm.RoleAssistant, Content: response.text, ToolCalls: response.calls}}
		if err := output.Emit(output.Context(), chunk); err != nil {
			output.Finish(err)
			return
		}
		output.Finish(nil)
	}()
	return output, nil
}

func (m *sessionTestLLM) Calls() int {
	m.mu.Lock()
	value := m.calls
	m.mu.Unlock()
	return value
}

func TestGenerationToolLoopCommitsOutputs(t *testing.T) {
	call := llm.NewFunctionCall("call-1", "lookup", `{"query":"weather"}`)
	model := newSessionTestLLM([]sessionLLMResponse{{calls: []*llm.FunctionCall{call}}, {text: "It is sunny."}})
	tool := llm.MustTool(llm.FunctionToolOptions[struct {
		Query string `json:"query"`
	}, string]{
		Name: "lookup",
		Execute: func(_ context.Context, input struct {
			Query string `json:"query"`
		}, _ llm.ToolOptions) (string, error) {
			return "sunny for " + input.Query, nil
		},
	})
	tools, err := llm.NewToolContext(tool)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model, Tools: tools, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "tool_agent"})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })

	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{UserInput: "weather?"})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if model.Calls() != 2 {
		t.Fatalf("LLM calls = %d", model.Calls())
	}
	var calls, outputs, assistants int
	for _, item := range session.ChatContext().Items() {
		switch value := item.(type) {
		case *llm.FunctionCall:
			calls++
		case *llm.FunctionCallOutput:
			outputs++
			if value.IsError || value.Output != "sunny for weather" {
				t.Fatalf("tool output = %#v", value)
			}
		case *llm.ChatMessage:
			if value.Role == llm.RoleAssistant {
				assistants++
			}
		}
	}
	if calls != 1 || outputs != 1 || assistants != 1 {
		t.Fatalf("history calls=%d outputs=%d assistants=%d", calls, outputs, assistants)
	}
}

func TestGenerationToolHandoffDoesNotDeadlockDrain(t *testing.T) {
	secondEntered := make(chan struct{})
	second := MustAgent(AgentOptions[struct{}]{ID: "second_agent", Hooks: AgentHooks[struct{}]{
		OnEnter: func(context.Context, *AgentContext[struct{}]) error { close(secondEntered); return nil },
	}})
	call := llm.NewFunctionCall("handoff-1", "handoff", `{}`)
	model := newSessionTestLLM([]sessionLLMResponse{{calls: []*llm.FunctionCall{call}}})
	tool := llm.MustTool(llm.FunctionToolOptions[struct{}, llm.AgentHandoff]{
		Name: "handoff",
		Execute: func(context.Context, struct{}, llm.ToolOptions) (llm.AgentHandoff, error) {
			return llm.Handoff(second, "complete"), nil
		},
	})
	tools, _ := llm.NewToolContext(tool)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model, Tools: tools, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	first := MustAgent(AgentOptions[struct{}]{ID: "first_agent"})
	if err := session.Start(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{UserInput: "switch"})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, secondEntered, "tool handoff")
	if session.Agent() != second {
		t.Fatal("handoff agent not installed")
	}
}

type sessionTestTTS struct {
	*tts.Base
	hang bool
}

func newSessionTestTTS(hang bool) *sessionTestTTS {
	return &sessionTestTTS{Base: tts.NewBase("test-tts", "test", "test-voice", 16000, 1, tts.Capabilities{}), hang: hang}
}

func (t *sessionTestTTS) Synthesize(ctx context.Context, _ string, _ tts.SynthesizeOptions) (tts.ChunkedStream, error) {
	output := tts.NewBaseChunkedStream(ctx, 2)
	if !t.hang {
		go func() {
			frame, _ := agents.NewAudioFrame(make([]int16, 160), 16000, 1)
			_ = output.Emit(output.Context(), tts.SynthesizedAudio{Frame: frame, Final: true})
			output.Finish(nil)
		}()
	}
	return output, nil
}

func (t *sessionTestTTS) Stream(context.Context, tts.StreamOptions) (tts.SynthesizeStream, error) {
	return nil, errors.New("streaming is unsupported")
}
func (t *sessionTestTTS) Close(context.Context) error { return nil }

func TestGenerationTTSForwardingAndIdleTimeout(t *testing.T) {
	for _, test := range []struct {
		name    string
		hang    bool
		wantErr bool
	}{{name: "audio", hang: false}, {name: "idle timeout", hang: true, wantErr: true}} {
		t.Run(test.name, func(t *testing.T) {
			model := newSessionTestLLM([]sessionLLMResponse{{text: "hello"}})
			session, err := NewAgentSession(AgentSessionOptions[struct{}]{
				LLM: model, TTS: newSessionTestTTS(test.hang), TTSReadIdleTimeout: 15 * time.Millisecond,
				DisableUserAwayTimeout: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "tts_agent"})); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = session.Close(context.Background()) })
			var frames atomic.Int64
			var output *ManagedAudioOutput
			output, err = NewManagedAudioOutput(AudioOutputOptions{
				SampleRate: 16000,
				Capture:    func(context.Context, agents.AudioFrame) error { frames.Add(1); return nil },
				Flush: func(context.Context) error {
					return output.NotifyPlaybackFinished(PlaybackFinishedEvent{PlaybackPosition: 10 * time.Millisecond})
				},
				ClearBuffer: func(context.Context) error { return nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			session.Output().SetAudio(output)
			handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{UserInput: "hi"})
			if err != nil {
				t.Fatal(err)
			}
			err = handle.Wait(t.Context())
			if test.wantErr {
				if err == nil || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("idle timeout error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if frames.Load() != 1 {
				t.Fatalf("frames = %d", frames.Load())
			}
		})
	}
}

var _ llm.LLM = (*sessionTestLLM)(nil)
var _ tts.TTS = (*sessionTestTTS)(nil)
