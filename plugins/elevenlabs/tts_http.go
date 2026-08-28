// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/tts"
)

type ChunkedStream struct {
	*tts.BaseChunkedStream

	owner   *TTS
	text    string
	opts    resolvedTTSOptions
	connect agents.APIConnectOptions
	done    chan struct{}
}

func newChunkedStream(parent context.Context, owner *TTS, text string, opts resolvedTTSOptions, connect agents.APIConnectOptions) *ChunkedStream {
	return &ChunkedStream{
		BaseChunkedStream: tts.NewBaseChunkedStream(parent, opts.outputCapacity),
		owner:             owner, text: text, opts: opts, connect: connect, done: make(chan struct{}),
	}
}

func (s *ChunkedStream) start() {
	go func() {
		defer close(s.done)
		defer s.owner.removeChunk(s)
		s.run()
	}()
}

func (s *ChunkedStream) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.done:
		return nil
	}
}

func (s *ChunkedStream) run() {
	ctx := s.Context()
	var lastErr error
	for attempt := 0; attempt <= s.connect.MaxRetries; attempt++ {
		attemptCtx, cancel := withAttemptTimeout(ctx, s.connect.Timeout)
		emitted, err := s.synthesizeOnce(attemptCtx)
		cancel()
		if err == nil {
			s.Finish(nil)
			return
		}
		if ctx.Err() != nil {
			s.Finish(nil)
			return
		}
		lastErr = normalizeNetworkError(err)
		if emitted || !isRetryable(lastErr) || attempt == s.connect.MaxRetries {
			break
		}
		if err := waitRetry(ctx, retryDelay(s.connect, attempt)); err != nil {
			s.Finish(nil)
			return
		}
	}
	if lastErr == nil {
		lastErr = agents.NewAPIConnectionError("ElevenLabs synthesis failed", false, nil)
	}
	s.owner.EmitError(tts.ErrorEvent{Timestamp: time.Now(), Label: s.owner.Label(), Err: lastErr, Recoverable: false})
	s.Finish(lastErr)
}

func (s *ChunkedStream) synthesizeOnce(ctx context.Context) (bool, error) {
	clientRequestID := agents.ShortUUID("el_")
	metrics := tts.BeginRequest(s.owner.Base, clientRequestID, clientRequestID, s.text, false)
	voiceSettings := s.opts.voiceSettings
	payload := map[string]any{
		"text":                     s.text,
		"model_id":                 s.opts.model,
		"apply_text_normalization": s.opts.applyTextNormalization,
	}
	if voiceSettings != nil {
		payload["voice_settings"] = voiceSettings
	}
	if s.opts.language != "" {
		payload["language_code"] = agents.BaseLanguage(string(s.opts.language))
	}
	if s.opts.applyLanguageTextNormalization != nil {
		payload["apply_language_text_normalization"] = *s.opts.applyLanguageTextNormalization
	}
	if len(s.opts.pronunciationDictionaries) != 0 {
		payload["pronunciation_dictionary_locators"] = s.opts.pronunciationDictionaries
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	u := endpointURL(s.opts.baseURL, "text-to-speech", s.opts.voiceID, "stream")
	query := u.Query()
	query.Set("output_format", string(s.opts.encoding))
	query.Set("enable_logging", boolString(s.opts.enableLogging))
	if s.opts.streamingLatency != nil {
		query.Set("optimize_streaming_latency", fmt.Sprintf("%d", *s.opts.streamingLatency))
	}
	u.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	request.Header.Set(AuthorizationHeader, s.opts.apiKey)
	request.Header.Set("Content-Type", "application/json")
	if s.opts.userAgent != "" {
		request.Header.Set("User-Agent", s.opts.userAgent)
	}
	response, err := s.opts.httpClient.Do(request)
	if err != nil {
		metrics.Finish(errors.Is(err, context.Canceled), 0, 0)
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := readBounded(response.Body, s.opts.maxResponseBytes)
		metrics.Finish(false, 0, 0)
		if readErr != nil {
			return false, readErr
		}
		return false, apiStatusError(response, body)
	}
	if err := requirePCMContentType(response.Header.Get("Content-Type")); err != nil {
		metrics.Finish(false, 0, 0)
		return false, err
	}
	providerRequestID := requestID(response.Header)
	if providerRequestID == "" {
		providerRequestID = clientRequestID
	}
	packetizer, err := agents.NewAudioByteStream(s.opts.sampleRate, 1, 0)
	if err != nil {
		metrics.Finish(false, 0, 0)
		return false, err
	}
	buffer := make([]byte, 32*1024)
	var lastFrame *agents.AudioFrame
	emitted := false
	emitFrame := func(final bool) error {
		if lastFrame == nil {
			return nil
		}
		frame := *lastFrame
		metrics.AddAudio(frame)
		if err := s.Emit(ctx, tts.SynthesizedAudio{RequestID: providerRequestID, SegmentID: clientRequestID, Frame: frame, Final: final}); err != nil {
			return err
		}
		emitted = true
		lastFrame = nil
		return nil
	}
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			for _, frame := range packetizer.Write(buffer[:n]) {
				if err := emitFrame(false); err != nil {
					metrics.Finish(errors.Is(err, context.Canceled), 0, 0)
					return emitted, err
				}
				frameCopy := frame
				lastFrame = &frameCopy
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			metrics.Finish(false, 0, 0)
			return emitted, readErr
		}
	}
	frames, err := packetizer.Flush()
	if err != nil {
		metrics.Finish(false, 0, 0)
		return emitted, &ProtocolError{Message: "incomplete PCM response", Cause: err}
	}
	for _, frame := range frames {
		if err := emitFrame(false); err != nil {
			metrics.Finish(errors.Is(err, context.Canceled), 0, 0)
			return emitted, err
		}
		frameCopy := frame
		lastFrame = &frameCopy
	}
	if err := emitFrame(true); err != nil {
		metrics.Finish(errors.Is(err, context.Canceled), 0, 0)
		return emitted, err
	}
	if !emitted {
		metrics.Finish(false, 0, 0)
		return false, &ProtocolError{Message: "empty PCM response"}
	}
	metrics.Finish(false, 0, 0)
	return true, nil
}

func (s *ChunkedStream) Close() error { return s.BaseChunkedStream.Close() }

var _ tts.ChunkedStream = (*ChunkedStream)(nil)
