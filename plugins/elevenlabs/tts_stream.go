// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/tokenize"
	"github.com/livekit/agents-go/tts"
)

type SynthesizeStream struct {
	*tts.BaseSynthesizeStream

	owner   *TTS
	opts    resolvedTTSOptions
	connect agents.APIConnectOptions
	id      string
	done    chan struct{}
}

type ttsInputResult struct {
	input tts.StreamInput
	err   error
}

type streamMetrics struct {
	started    time.Time
	firstByte  time.Time
	characters int64
	audio      time.Duration
	emitted    bool
}

func newSynthesizeStream(parent context.Context, owner *TTS, opts resolvedTTSOptions, connect agents.APIConnectOptions) *SynthesizeStream {
	return &SynthesizeStream{
		BaseSynthesizeStream: tts.NewBaseSynthesizeStream(parent, max(opts.inputCapacity, opts.outputCapacity)),
		owner:                owner, opts: opts, connect: connect, id: agents.ShortUUID("elctx_"), done: make(chan struct{}),
	}
}

func (s *SynthesizeStream) ContextID() string { return s.id }

func (s *SynthesizeStream) start() {
	go func() {
		defer close(s.done)
		defer s.owner.removeStream(s)
		s.run()
	}()
}

func (s *SynthesizeStream) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.done:
		return nil
	}
}

func (s *SynthesizeStream) run() {
	ctx := s.Context()
	inputCh := make(chan ttsInputResult, 1)
	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		for {
			value, err := s.Inputs().Recv(ctx)
			select {
			case inputCh <- ttsInputResult{input: value, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		_ = s.BaseSynthesizeStream.Close()
		<-inputDone
	}()

	state := &ttsContextState{
		id: s.id, ctx: ctx, events: make(chan ttsProviderResult, s.opts.providerEventCap), failed: make(chan struct{}),
	}
	connection, err := s.owner.acquireContext(ctx, state, s.opts, s.connect)
	if err != nil {
		s.fail(err, false, streamMetrics{})
		return
	}
	completed := false
	closeSent := false
	defer func() { connection.releaseContext(state, !completed && !closeSent) }()

	packetizer, err := agents.NewAudioByteStream(s.opts.sampleRate, 1, 0)
	if err != nil {
		s.fail(err, false, streamMetrics{})
		return
	}
	tokenizerState := incrementalTokenizer{tokenizer: s.opts.tokenizer}
	alignment := alignmentState{}
	var lastFrame *agents.AudioFrame
	var pendingTimed []agents.TimedString
	var xmlTokens []string
	streamStats := streamMetrics{}
	inputEnded := false
	flushOnChunk := s.opts.autoMode
	if _, ok := s.opts.tokenizer.(*tokenize.SentenceTokenizer); !ok {
		flushOnChunk = false
	}

	emitLast := func(final bool) error {
		if lastFrame == nil {
			return nil
		}
		frame := *lastFrame
		if streamStats.firstByte.IsZero() {
			streamStats.firstByte = time.Now()
		}
		streamStats.audio += frame.Duration()
		result := tts.SynthesizedAudio{
			RequestID: s.id, SegmentID: s.id, Frame: frame, Final: final,
			TimedTranscripts: pendingTimed,
		}
		if err := s.Emit(ctx, result); err != nil {
			return err
		}
		streamStats.emitted = true
		lastFrame = nil
		pendingTimed = nil
		return nil
	}

	sendToken := func(token string, flush bool) error {
		if token == "" {
			return nil
		}
		if s.opts.enableSSMLParsing {
			startsXML := strings.HasPrefix(token, "<phoneme") || strings.HasPrefix(token, "<break")
			if startsXML || len(xmlTokens) != 0 {
				xmlTokens = append(xmlTokens, token)
				joined := strings.Join(xmlTokens, " ")
				if !strings.Contains(joined, "</phoneme>") && !strings.Contains(joined, "/>") {
					return nil
				}
				token = joined
				xmlTokens = nil
			}
		}
		if streamStats.started.IsZero() {
			streamStats.started = time.Now()
		}
		return connection.sendContent(ctx, state, token+" ", flush)
	}

	processTokens := func(tokens []string, flush bool) error {
		for _, token := range tokens {
			if err := sendToken(token, flush); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			s.finishMetrics(streamStats, true)
			s.Finish(nil)
			return
		case <-state.failed:
			err := state.failureError()
			s.fail(err, false, streamStats)
			return
		case input := <-inputCh:
			if input.err != nil {
				if !errors.Is(input.err, io.EOF) && !errors.Is(input.err, context.Canceled) {
					s.fail(input.err, false, streamStats)
					return
				}
				if inputEnded {
					continue
				}
				inputEnded = true
				if err := processTokens(tokenizerState.flush(), flushOnChunk); err != nil {
					s.fail(err, false, streamStats)
					return
				}
				// An incomplete XML fragment is intentionally not sent, matching the
				// upstream stream contract.
				xmlTokens = nil
				if streamStats.started.IsZero() {
					streamStats.started = time.Now()
				}
				if err := connection.sendContent(ctx, state, "", true); err != nil {
					s.fail(err, false, streamStats)
					return
				}
				if err := connection.sendCloseContext(ctx, state); err != nil {
					s.fail(err, false, streamStats)
					return
				}
				closeSent = true
				continue
			}
			if input.input.Text != "" {
				streamStats.characters += int64(utf8.RuneCountInString(input.input.Text))
				if err := processTokens(tokenizerState.push(input.input.Text), flushOnChunk); err != nil {
					s.fail(err, false, streamStats)
					return
				}
			}
			if input.input.Flush {
				if err := processTokens(tokenizerState.flush(), flushOnChunk); err != nil {
					s.fail(err, false, streamStats)
					return
				}
			}
		case provider := <-state.events:
			if provider.err != nil {
				s.fail(provider.err, false, streamStats)
				return
			}
			event := provider.event
			selected := event.Alignment
			if s.opts.preferredAlignment == NormalizedAlignment {
				selected = event.NormalizedAlignment
			}
			if selected != nil {
				words, err := alignment.add(selected, false)
				if err != nil {
					s.fail(err, false, streamStats)
					return
				}
				pendingTimed = append(pendingTimed, words...)
			}
			if len(event.Audio) != 0 {
				for _, frame := range packetizer.Write(event.Audio) {
					if err := emitLast(false); err != nil {
						s.fail(err, errors.Is(err, context.Canceled), streamStats)
						return
					}
					copyFrame := frame
					lastFrame = &copyFrame
				}
			}
			if event.Final {
				words, err := alignment.add(nil, true)
				if err != nil {
					s.fail(err, false, streamStats)
					return
				}
				pendingTimed = append(pendingTimed, words...)
				frames, err := packetizer.Flush()
				if err != nil {
					s.fail(&ProtocolError{Message: "incomplete PCM websocket response", Cause: err}, false, streamStats)
					return
				}
				for _, frame := range frames {
					if err := emitLast(false); err != nil {
						s.fail(err, errors.Is(err, context.Canceled), streamStats)
						return
					}
					copyFrame := frame
					lastFrame = &copyFrame
				}
				if err := emitLast(true); err != nil {
					s.fail(err, errors.Is(err, context.Canceled), streamStats)
					return
				}
				completed = true
				connection.releaseContext(state, false)
				s.finishMetrics(streamStats, false)
				s.Finish(nil)
				return
			}
		}
	}
}

func (s *SynthesizeStream) fail(err error, cancelled bool, stats streamMetrics) {
	if err == nil {
		err = agents.NewAPIConnectionError("ElevenLabs streaming synthesis failed", false, nil)
	}
	if cancelled || errors.Is(err, context.Canceled) {
		s.finishMetrics(stats, true)
		s.Finish(nil)
		return
	}
	s.finishMetrics(stats, false)
	s.owner.EmitError(tts.ErrorEvent{Timestamp: time.Now(), Label: s.owner.Label(), Err: err, Recoverable: false})
	s.Finish(err)
}

func (s *SynthesizeStream) finishMetrics(stats streamMetrics, cancelled bool) {
	if stats.started.IsZero() {
		stats.started = time.Now()
	}
	ttfb := time.Duration(0)
	if !stats.firstByte.IsZero() {
		ttfb = stats.firstByte.Sub(stats.started)
	}
	s.owner.EmitMetrics(metrics.TTS{
		Label: s.owner.Label(), RequestID: s.id, SegmentID: s.id, Timestamp: time.Now(),
		TimeToFirstByte: ttfb, Duration: time.Since(stats.started), AudioDuration: stats.audio,
		Cancelled: cancelled, CharactersCount: stats.characters, Streamed: true,
		Metadata: metrics.Metadata{ModelProvider: s.owner.Provider(), ModelName: s.opts.model.String()},
	})
}

func (s *SynthesizeStream) Close() error { return s.BaseSynthesizeStream.Close() }

func (m TTSModel) String() string { return string(m) }

var _ tts.SynthesizeStream = (*SynthesizeStream)(nil)
