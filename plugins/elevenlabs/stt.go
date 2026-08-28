// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stt"
)

type STT struct {
	*stt.Base

	mu      sync.RWMutex
	opts    resolvedSTTOptions
	streams map[*SpeechStream]struct{}
	closed  bool
}

func NewSTT(options STTOptions) (*STT, error) {
	resolved, err := resolveSTTOptions(options)
	if err != nil {
		return nil, err
	}
	capabilities := stt.Capabilities{
		Streaming:      resolved.model == ScribeV2Realtime,
		InterimResults: true,
		Keyterms:       true,
	}
	if resolved.includeTimestamps && capabilities.Streaming {
		capabilities.AlignedTranscript = stt.AlignedTranscriptWord
	}
	return &STT{
		Base:    stt.NewBase("elevenlabs.STT", "ElevenLabs", string(resolved.model), capabilities),
		opts:    resolved,
		streams: make(map[*SpeechStream]struct{}),
	}, nil
}

func (s *STT) Recognize(ctx context.Context, frames []agents.AudioFrame, options stt.RecognizeOptions) (stt.SpeechEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return stt.SpeechEvent{}, io.ErrClosedPipe
	}
	opts := cloneResolvedSTTOptions(s.opts)
	s.mu.RUnlock()

	wav, merged, err := createWAV(frames)
	if err != nil {
		return stt.SpeechEvent{}, err
	}
	language := options.Language
	if language == "" {
		language = opts.language
	} else {
		language = agents.NormalizeLanguage(string(language))
	}
	connect := options.ConnectOptions.Resolve()
	var lastErr error
	for attempt := 0; attempt <= connect.MaxRetries; attempt++ {
		attemptCtx, cancel := withAttemptTimeout(ctx, connect.Timeout)
		started := time.Now()
		event, requestErr := s.recognizeOnce(attemptCtx, opts, wav, language)
		cancel()
		if requestErr == nil {
			s.EmitMetrics(metrics.STT{
				Label: s.Label(), RequestID: event.RequestID, Timestamp: time.Now(),
				Duration: time.Since(started), AudioDuration: merged.Duration(), Streamed: false,
				Metadata: metrics.Metadata{ModelProvider: s.Provider(), ModelName: s.Model()},
			})
			return event, nil
		}
		lastErr = normalizeNetworkError(requestErr)
		if ctx.Err() != nil {
			return stt.SpeechEvent{}, context.Cause(ctx)
		}
		if !isRetryable(lastErr) || attempt == connect.MaxRetries {
			break
		}
		if err := waitRetry(ctx, retryDelay(connect, attempt)); err != nil {
			return stt.SpeechEvent{}, err
		}
	}
	if lastErr == nil {
		lastErr = agents.NewAPIConnectionError("ElevenLabs transcription failed", false, nil)
	}
	s.EmitError(stt.ErrorEvent{Timestamp: time.Now(), Label: s.Label(), Err: lastErr, Recoverable: false})
	return stt.SpeechEvent{}, lastErr
}

func (s *STT) recognizeOnce(ctx context.Context, opts resolvedSTTOptions, wav []byte, language agents.LanguageCode) (stt.SpeechEvent, error) {
	body := bytes.NewBuffer(nil)
	writer := multipart.NewWriter(body)
	fileHeader := make(textproto.MIMEHeader)
	fileHeader.Set("Content-Disposition", `form-data; name="file"; filename="audio.wav"`)
	fileHeader.Set("Content-Type", "audio/x-wav")
	part, err := writer.CreatePart(fileHeader)
	if err != nil {
		return stt.SpeechEvent{}, err
	}
	if _, err = part.Write(wav); err != nil {
		return stt.SpeechEvent{}, err
	}
	fields := [][2]string{
		{"model_id", string(opts.model)},
		{"tag_audio_events", boolString(opts.tagAudioEvents)},
	}
	if language != "" {
		fields = append(fields, [2]string{"language_code", string(language)})
	}
	for _, keyterm := range mergedKeyterms(opts.userKeyterms, opts.sessionKeyterms) {
		fields = append(fields, [2]string{"keyterms", keyterm})
	}
	if opts.noVerbatim {
		fields = append(fields, [2]string{"no_verbatim", "true"})
	}
	for _, field := range fields {
		if err := writer.WriteField(field[0], field[1]); err != nil {
			return stt.SpeechEvent{}, err
		}
	}
	if err := writer.Close(); err != nil {
		return stt.SpeechEvent{}, err
	}

	u := endpointURL(opts.baseURL, "speech-to-text")
	query := u.Query()
	query.Set("enable_logging", boolString(opts.enableLogging))
	u.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return stt.SpeechEvent{}, err
	}
	request.Header.Set(AuthorizationHeader, opts.apiKey)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	if opts.userAgent != "" {
		request.Header.Set("User-Agent", opts.userAgent)
	}
	response, err := opts.httpClient.Do(request)
	if err != nil {
		return stt.SpeechEvent{}, err
	}
	defer response.Body.Close()
	responseBody, err := readBounded(response.Body, opts.maxResponseBytes)
	if err != nil {
		return stt.SpeechEvent{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return stt.SpeechEvent{}, apiStatusError(response, responseBody)
	}
	if err := requireJSONContentType(response.Header.Get("Content-Type")); err != nil {
		return stt.SpeechEvent{}, err
	}
	var provider batchSTTResponse
	if err := json.Unmarshal(responseBody, &provider); err != nil {
		return stt.SpeechEvent{}, &ProtocolError{Message: "invalid batch STT JSON", Cause: err}
	}
	return batchSpeechEvent(provider, language, requestID(response.Header)), nil
}

func (s *STT) Stream(ctx context.Context, options stt.StreamOptions) (stt.SpeechStream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	if s.opts.model != ScribeV2Realtime {
		model := s.opts.model
		s.mu.Unlock()
		return nil, fmt.Errorf("elevenlabs: STT model %s does not support realtime streaming", model)
	}
	language := options.Language
	if language == "" {
		language = s.opts.language
	} else {
		language = agents.NormalizeLanguage(string(language))
	}
	stream := newSpeechStream(ctx, s, cloneResolvedSTTOptions(s.opts), options.ConnectOptions.Resolve(), language)
	s.streams[stream] = struct{}{}
	s.mu.Unlock()
	stream.start()
	return stream, nil
}

func (s *STT) UpdateOptions(options STTUpdateOptions) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return io.ErrClosedPipe
	}
	next := cloneResolvedSTTOptions(s.opts)
	if err := applySTTUpdate(&next, options); err != nil {
		s.mu.Unlock()
		return err
	}
	s.opts = next
	snapshot := cloneResolvedSTTOptions(next)
	streams := make([]*SpeechStream, 0, len(s.streams))
	for stream := range s.streams {
		streams = append(streams, stream)
	}
	s.mu.Unlock()
	for _, stream := range streams {
		stream.requestOptions(snapshot)
	}
	return nil
}

func applySTTUpdate(opts *resolvedSTTOptions, options STTUpdateOptions) error {
	if options.TagAudioEvents != nil {
		opts.tagAudioEvents = *options.TagAudioEvents
	}
	if value, ok := options.ServerVAD.Value(); ok {
		if err := validateVADOptions(&value); err != nil {
			return err
		}
		opts.serverVAD = cloneVADOptions(&value)
	} else if options.ServerVAD.IsDisabled() {
		opts.serverVAD = nil
	}
	if value, ok := options.Keyterms.Value(); ok {
		if err := validateKeyterms(mergedKeyterms(value, opts.sessionKeyterms), opts.model == ScribeV2Realtime); err != nil {
			return err
		}
		opts.userKeyterms = cloneStrings(value)
	} else if options.Keyterms.IsDisabled() {
		opts.userKeyterms = nil
	}
	if options.NoVerbatim != nil {
		opts.noVerbatim = *options.NoVerbatim
	}
	return nil
}

// UpdateSessionKeyterms applies framework-managed keyterms without replacing
// terms supplied directly by the application.
func (s *STT) UpdateSessionKeyterms(keyterms []string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return io.ErrClosedPipe
	}
	merged := mergedKeyterms(s.opts.userKeyterms, keyterms)
	if err := validateKeyterms(merged, s.opts.model == ScribeV2Realtime); err != nil {
		s.mu.Unlock()
		return err
	}
	s.opts.sessionKeyterms = cloneStrings(keyterms)
	snapshot := cloneResolvedSTTOptions(s.opts)
	streams := make([]*SpeechStream, 0, len(s.streams))
	for stream := range s.streams {
		streams = append(streams, stream)
	}
	s.mu.Unlock()
	for _, stream := range streams {
		stream.requestOptions(snapshot)
	}
	return nil
}

func (s *STT) removeStream(stream *SpeechStream) {
	s.mu.Lock()
	delete(s.streams, stream)
	s.mu.Unlock()
}

func (s *STT) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	streams := make([]*SpeechStream, 0, len(s.streams))
	for stream := range s.streams {
		streams = append(streams, stream)
	}
	s.streams = make(map[*SpeechStream]struct{})
	s.mu.Unlock()
	for _, stream := range streams {
		_ = stream.Close()
	}
	for _, stream := range streams {
		if err := stream.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

type batchSTTWord struct {
	Text      string   `json:"text"`
	Start     *float64 `json:"start"`
	End       *float64 `json:"end"`
	SpeakerID *string  `json:"speaker_id"`
	Type      string   `json:"type"`
	LogProb   *float64 `json:"logprob"`
}

type batchSTTResponse struct {
	Text         string          `json:"text"`
	LanguageCode string          `json:"language_code"`
	Words        []batchSTTWord  `json:"words"`
	Detail       json.RawMessage `json:"detail"`
}

func batchSpeechEvent(response batchSTTResponse, requested agents.LanguageCode, id string) stt.SpeechEvent {
	language := agents.NormalizeLanguage(response.LanguageCode)
	if language == "" {
		language = requested
	}
	start, end := time.Duration(0), time.Duration(0)
	if len(response.Words) > 0 {
		start = secondsDuration(numberOrZero(response.Words[0].Start))
		for _, word := range response.Words {
			wordStart := secondsDuration(numberOrZero(word.Start))
			wordEnd := secondsDuration(numberOrZero(word.End))
			if wordStart < start {
				start = wordStart
			}
			if wordEnd > end {
				end = wordEnd
			}
		}
	}
	words := make([]agents.TimedString, len(response.Words))
	for i, word := range response.Words {
		words[i] = agents.NewTimedString(word.Text, secondsDuration(numberOrZero(word.Start)), secondsDuration(numberOrZero(word.End)))
	}
	speakerID := ""
	if len(response.Words) > 0 && response.Words[0].SpeakerID != nil {
		speakerID = *response.Words[0].SpeakerID
	}
	return stt.SpeechEvent{
		Type: stt.FinalTranscript, RequestID: id,
		Alternatives: []stt.SpeechData{{
			Language: language, Text: response.Text, StartTime: start, EndTime: end,
			Confidence: speechConfidence(response.Words), Words: words, SpeakerID: speakerID,
		}},
	}
}

func speechConfidence(words []batchSTTWord) float64 {
	var sum float64
	var count int
	for _, word := range words {
		if word.Type == "word" && word.LogProb != nil && !math.IsNaN(*word.LogProb) {
			sum += *word.LogProb
			count++
		}
	}
	if count == 0 {
		return 0
	}
	confidence := math.Exp(sum / float64(count))
	return math.Max(0, math.Min(1, confidence))
}

func numberOrZero(value *float64) float64 {
	if value == nil || math.IsNaN(*value) || math.IsInf(*value, 0) {
		return 0
	}
	return *value
}

func secondsDuration(value float64) time.Duration {
	if value <= 0 {
		return 0
	}
	max := float64(math.MaxInt64) / float64(time.Second)
	if value > max {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(value * float64(time.Second))
}

func cloneResolvedSTTOptions(value resolvedSTTOptions) resolvedSTTOptions {
	value.baseURL = cloneURL(value.baseURL)
	value.serverVAD = cloneVADOptions(value.serverVAD)
	value.userKeyterms = cloneStrings(value.userKeyterms)
	value.sessionKeyterms = cloneStrings(value.sessionKeyterms)
	return value
}

func cloneURL(value *url.URL) *url.URL {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

var _ stt.STT = (*STT)(nil)
