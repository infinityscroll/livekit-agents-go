// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/stt"
)

var agentSessionAMDRegistry sync.Map

// AdaptAMDSession exposes only the lifecycle/model hooks AMD needs. It also
// powers AgentSession.AMD without adding per-session memory when AMD is unused.
func AdaptAMDSession[UserData any](session *AgentSession[UserData]) AMDSessionAdapter {
	if session == nil {
		return AMDSessionAdapter{}
	}
	var bindMu sync.Mutex
	var bound *AMD
	return AMDSessionAdapter{
		PauseReplyAuthorization:  session.PauseReplyAuthorization,
		ResumeReplyAuthorization: session.ResumeReplyAuthorization,
		Subscribe:                session.Subscribe,
		Interrupt:                session.Interrupt,
		CurrentLLM: func() llm.LLM {
			session.mu.RLock()
			activity, model := session.activity, session.models.llm
			session.mu.RUnlock()
			if activity != nil {
				if current := activity.modelsSnapshot().llm; current != nil {
					return current
				}
			}
			return model
		},
		MaxEndpointingDelay: func() time.Duration {
			session.mu.RLock()
			activity := session.activity
			session.mu.RUnlock()
			if activity == nil {
				return DefaultAMDMaxEndpointingDelay
			}
			activity.mu.RLock()
			endpointing := activity.endpointing
			activity.mu.RUnlock()
			if endpointing == nil {
				return DefaultAMDMaxEndpointingDelay
			}
			return endpointing.MaxDelay()
		},
		PublishPrediction: func(ctx context.Context, event AMDPredictionEvent) error {
			return session.events.Publish(ctx, event)
		},
		Bind: func(value *AMD) {
			bindMu.Lock()
			defer bindMu.Unlock()
			if value != nil {
				bound = value
				agentSessionAMDRegistry.Store(session, value)
				return
			}
			if bound != nil {
				agentSessionAMDRegistry.CompareAndDelete(session, bound)
				bound = nil
			}
		},
	}
}

// AMD returns the detector most recently constructed for this session, or nil.
// The registry entry is removed by AMD.Close and therefore does not retain a
// closed session.
func (s *AgentSession[UserData]) AMD() *AMD {
	if s == nil {
		return nil
	}
	value, ok := agentSessionAMDRegistry.Load(s)
	if !ok {
		return nil
	}
	amd, _ := value.(*AMD)
	return amd
}

func (r *amdRun) runListeningGate() {
	defer r.workers.Done()
	ctx, cancel := context.WithTimeout(r.ctx, r.owner.opts.trackTimeout)
	defer cancel()
	err := guardAMD("wait for participant audio", func() error {
		return r.owner.opts.listeningGate.WaitForAudio(ctx, r.owner.opts.participantIdentity)
	})
	if context.Cause(r.ctx) != nil {
		return
	}
	if err != nil {
		_ = r.send(r.ctx, amdCommand{kind: amdCommandParticipantMissing, err: err})
		return
	}
	_ = r.send(r.ctx, amdCommand{kind: amdCommandStartListening})
}

func (r *amdRun) runSessionEvents(subscription *EventSubscription) {
	defer r.workers.Done()
	for {
		event, err := subscription.Recv(r.ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) && context.Cause(r.ctx) == nil {
				r.owner.report("session event stream", err)
			}
			return
		}
		switch value := event.(type) {
		case UserStateChangedEvent:
			r.forwardUserState(value)
		case *UserStateChangedEvent:
			if value != nil {
				r.forwardUserState(*value)
			}
		case UserInputTranscribedEvent:
			if value.Final {
				_ = r.send(r.ctx, amdCommand{kind: amdCommandTranscript, text: value.Transcript, source: AMDTranscriptSourceSessionSTT})
			}
		case *UserInputTranscribedEvent:
			if value != nil && value.Final {
				_ = r.send(r.ctx, amdCommand{kind: amdCommandTranscript, text: value.Transcript, source: AMDTranscriptSourceSessionSTT})
			}
		case ConversationItemAddedEvent:
			r.forwardConversationItem(value.Item)
		case *ConversationItemAddedEvent:
			if value != nil {
				r.forwardConversationItem(value.Item)
			}
		case CloseEvent, *CloseEvent:
			_ = r.send(r.ctx, amdCommand{kind: amdCommandSessionClosed})
		}
	}
}

func (r *amdRun) forwardUserState(event UserStateChangedEvent) {
	switch event.NewState {
	case UserStateSpeaking:
		_ = r.send(r.ctx, amdCommand{kind: amdCommandSpeechStarted, at: event.Time()})
	case UserStateListening:
		if event.OldState == UserStateSpeaking {
			_ = r.send(r.ctx, amdCommand{kind: amdCommandSpeechEnded, at: event.Time()})
		}
	}
}

func (r *amdRun) forwardConversationItem(item llm.ChatItem) {
	message, ok := item.(*llm.ChatMessage)
	if !ok || message == nil || message.Role != llm.RoleUser {
		return
	}
	text, _ := message.RawTextContent()
	_ = r.send(r.ctx, amdCommand{kind: amdCommandEndOfTurn, text: text})
}

func (r *amdRun) runDedicatedSTT() {
	defer r.workers.Done()
	var speechStream stt.SpeechStream
	err := guardAMD("open dedicated STT stream", func() (streamErr error) {
		speechStream, streamErr = r.owner.stt.Stream(r.ctx, stt.StreamOptions{})
		return streamErr
	})
	if err != nil {
		if context.Cause(r.ctx) == nil {
			r.owner.report("dedicated STT stream", err)
		}
		return
	}
	if speechStream == nil {
		r.owner.report("dedicated STT stream", errors.New("STT returned a nil stream"))
		return
	}
	if !r.setSTTStream(speechStream) {
		_ = guardAMD("close late STT stream", speechStream.Close)
		return
	}

	var audio AMDAudioSubscription
	if source := r.owner.opts.audioSource; source != nil {
		audio, err = guardAMDValue("subscribe dedicated STT audio", func() (AMDAudioSubscription, error) {
			return source.SubscribeAMD(r.ctx)
		})
		if err != nil {
			if context.Cause(r.ctx) == nil {
				r.owner.report("dedicated STT audio", err)
			}
		} else if audio == nil {
			r.owner.report("dedicated STT audio", ErrAMDNoAudioSource)
		} else if !r.setAudioSubscription(audio) {
			_ = guardAMD("close late AMD audio subscription", audio.Close)
			audio = nil
		}
	}

	sendDone := make(chan struct{})
	if audio != nil {
		go func() {
			defer close(sendDone)
			r.forwardAMDFrames(audio, speechStream)
		}()
	} else {
		close(sendDone)
	}

	for {
		var event stt.SpeechEvent
		recvErr := guardAMD("receive dedicated STT event", func() (eventErr error) {
			event, eventErr = speechStream.Recv(r.ctx)
			return eventErr
		})
		if recvErr != nil {
			if !errors.Is(recvErr, io.EOF) && context.Cause(r.ctx) == nil {
				r.owner.report("dedicated STT receive", recvErr)
			}
			break
		}
		if event.Type != stt.FinalTranscript || len(event.Alternatives) == 0 {
			continue
		}
		text := event.Alternatives[0].Text
		if text != "" {
			_ = r.send(r.ctx, amdCommand{kind: amdCommandTranscript, text: text, source: AMDTranscriptSourceDedicatedSTT})
		}
	}
	_ = guardAMD("close completed STT stream", speechStream.Close)
	if audio != nil {
		_ = guardAMD("close completed AMD audio subscription", audio.Close)
	}
	<-sendDone
}

func (r *amdRun) forwardAMDFrames(audio AMDAudioSubscription, speechStream stt.SpeechStream) {
	defer func() { _ = guardAMD("end dedicated STT input", speechStream.EndInput) }()
	for {
		frame, frameErr := guardAMDValue("receive dedicated STT audio", func() (agents.AudioFrame, error) {
			return audio.Recv(r.ctx)
		})
		if frameErr != nil {
			if !errors.Is(frameErr, io.EOF) && context.Cause(r.ctx) == nil {
				r.owner.report("dedicated STT audio receive", frameErr)
			}
			return
		}
		if !r.listening.Load() {
			continue
		}
		pushErr := guardAMD("push dedicated STT audio", func() error {
			return speechStream.Push(r.ctx, frame)
		})
		if pushErr != nil {
			if context.Cause(r.ctx) == nil {
				r.owner.report("dedicated STT audio push", pushErr)
			}
			return
		}
	}
}

func (r *amdRun) setSTTStream(value stt.SpeechStream) bool {
	r.resourceMu.Lock()
	defer r.resourceMu.Unlock()
	if r.stopped {
		return false
	}
	r.sttStream = value
	return true
}

func (r *amdRun) setAudioSubscription(value AMDAudioSubscription) bool {
	r.resourceMu.Lock()
	defer r.resourceMu.Unlock()
	if r.stopped {
		return false
	}
	r.audio = value
	return true
}

func (a *AMD) currentRun() *amdRun {
	a.mu.RLock()
	run := a.active
	a.mu.RUnlock()
	return run
}

// OnUserSpeechStarted forwards a VAD start boundary into the two-gate classifier.
func (a *AMD) OnUserSpeechStarted(ctx context.Context) error {
	run := a.currentRun()
	if run == nil {
		if a.Closed() {
			return ErrAMDClosed
		}
		return nil
	}
	return run.send(ctx, amdCommand{kind: amdCommandSpeechStarted, at: time.Now()})
}

// OnUserSpeechEnded forwards a VAD end boundary. silenceDuration is the
// trailing silence already elapsed when VAD declared end-of-speech.
func (a *AMD) OnUserSpeechEnded(ctx context.Context, silenceDuration time.Duration) error {
	if silenceDuration < 0 {
		return &AMDOptionError{Field: "silenceDuration", Err: errors.New("must not be negative")}
	}
	run := a.currentRun()
	if run == nil {
		if a.Closed() {
			return ErrAMDClosed
		}
		return nil
	}
	return run.send(ctx, amdCommand{kind: amdCommandSpeechEnded, at: time.Now(), duration: silenceDuration})
}

// OnTranscript consumes a final transcript from the session STT.
func (a *AMD) OnTranscript(ctx context.Context, text string) error {
	return a.OnTranscriptFrom(ctx, text, AMDTranscriptSourceSessionSTT)
}

func (a *AMD) OnTranscriptFrom(ctx context.Context, text string, source AMDTranscriptSource) error {
	if source != AMDTranscriptSourceSessionSTT && source != AMDTranscriptSourceDedicatedSTT {
		return &AMDOptionError{Field: "transcript source", Err: fmt.Errorf("unknown source %q", source)}
	}
	run := a.currentRun()
	if run == nil {
		if a.Closed() {
			return ErrAMDClosed
		}
		return nil
	}
	return run.send(ctx, amdCommand{kind: amdCommandTranscript, text: text, source: source})
}

// OnEndOfTurn opens the end-of-turn gate and reports whether a completed
// machine verdict has taken ownership of this turn's reply.
func (a *AMD) OnEndOfTurn(ctx context.Context, _ RecognizedTurn) (bool, error) {
	run := a.currentRun()
	if run == nil {
		last, ok := a.LastPrediction()
		return ok && a.opts.interruptOnMachine && last.IsMachine, nil
	}
	response := make(chan amdCommandResponse, 1)
	err := run.send(ctx, amdCommand{kind: amdCommandEndOfTurn, response: response})
	if err != nil {
		if errors.Is(err, errAMDRunComplete) {
			result, waitErr := run.execution.Wait(ctx)
			return waitErr == nil && a.opts.interruptOnMachine && result.IsMachine, waitErr
		}
		return false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case value := <-response:
		return value.skip, value.err
	case <-ctx.Done():
		return false, context.Cause(ctx)
	case <-run.actorDone:
		result, waitErr := run.execution.Wait(ctx)
		return waitErr == nil && a.opts.interruptOnMachine && result.IsMachine, waitErr
	}
}

// OnSessionClosed forces the pinned uncertain/session_closed fallback.
func (a *AMD) OnSessionClosed(ctx context.Context) error {
	run := a.currentRun()
	if run == nil {
		return nil
	}
	return run.send(ctx, amdCommand{kind: amdCommandSessionClosed})
}
