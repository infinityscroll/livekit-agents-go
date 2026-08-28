// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stt"
)

const sttKeepaliveInterval = 10 * time.Second

type SpeechStream struct {
	*stt.BaseStream

	owner    *STT
	connect  agents.APIConnectOptions
	language agents.LanguageCode

	updateMu     sync.Mutex
	pendingOpts  resolvedSTTOptions
	updateSignal chan struct{}

	done chan struct{}
	wg   sync.WaitGroup
}

type sttInputResult struct {
	input stt.StreamInput
	err   error
}

type realtimeSTTState struct {
	packetizer     *pcmPacketizer
	replayPackets  [][]byte
	replayBytes    int
	replayable     bool
	awaitingCommit bool
	ending         bool
	speaking       bool
	offset         time.Duration
	usage          time.Duration
	finalCount     uint64
	pendingOptions *resolvedSTTOptions
}

type sttSessionResult struct {
	err       error
	ended     bool
	reconnect bool
	nextOpts  *resolvedSTTOptions
}

func newSpeechStream(parent context.Context, owner *STT, opts resolvedSTTOptions, connect agents.APIConnectOptions, language agents.LanguageCode) *SpeechStream {
	return &SpeechStream{
		BaseStream:   stt.NewBaseStream(parent, max(opts.inputCapacity, opts.outputCapacity)),
		owner:        owner,
		connect:      connect,
		language:     language,
		pendingOpts:  cloneResolvedSTTOptions(opts),
		updateSignal: make(chan struct{}, 1),
		done:         make(chan struct{}),
	}
}

func (s *SpeechStream) start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(s.done)
		defer s.owner.removeStream(s)
		s.run()
	}()
}

func (s *SpeechStream) requestOptions(options resolvedSTTOptions) {
	s.updateMu.Lock()
	s.pendingOpts = cloneResolvedSTTOptions(options)
	s.updateMu.Unlock()
	select {
	case s.updateSignal <- struct{}{}:
	default:
	}
}

// UpdateOptions changes only this realtime stream. Updates become active at a
// safe committed-turn boundary so buffered audio cannot be lost.
func (s *SpeechStream) UpdateOptions(options STTUpdateOptions) error {
	next := s.latestOptions()
	if err := applySTTUpdate(&next, options); err != nil {
		return err
	}
	s.requestOptions(next)
	return nil
}

func (s *SpeechStream) latestOptions() resolvedSTTOptions {
	s.updateMu.Lock()
	options := cloneResolvedSTTOptions(s.pendingOpts)
	s.updateMu.Unlock()
	return options
}

func (s *SpeechStream) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.done:
		return nil
	}
}

func (s *SpeechStream) Close() error {
	return s.BaseStream.Close()
}

func (s *SpeechStream) run() {
	ctx := s.Context()
	inputCh := make(chan sttInputResult, 1)
	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		for {
			value, err := s.Inputs().Recv(ctx)
			select {
			case inputCh <- sttInputResult{input: value, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	opts := s.latestOptions()
	state := realtimeSTTState{
		packetizer: newPCMPacketizer(int(opts.sampleRate)/20, 1),
		replayable: true,
	}
	retries := 0
	for {
		if ctx.Err() != nil {
			s.Finish(nil)
			<-inputDone
			return
		}
		beforeFinal := state.finalCount
		connectedAt := time.Now()
		result := s.runSTTSession(ctx, opts, &state, inputCh)
		state.offset += time.Since(connectedAt)
		if state.finalCount != beforeFinal {
			retries = 0
		}
		if result.ended {
			s.flushUsage(ctx, &state)
			s.Finish(nil)
			<-inputDone
			return
		}
		if result.reconnect {
			if result.nextOpts != nil {
				opts = cloneResolvedSTTOptions(*result.nextOpts)
				state.packetizer.setFrameSamples(int(opts.sampleRate) / 20)
			}
			continue
		}
		if result.err == nil {
			result.err = agents.NewAPIConnectionError("ElevenLabs realtime STT ended unexpectedly", true, nil)
		}
		if ctx.Err() != nil {
			s.Finish(nil)
			<-inputDone
			return
		}
		if !state.replayable {
			result.err = fmt.Errorf("%w: reconnect would produce an incomplete turn", ErrReplayLimit)
		}
		if !isRetryable(result.err) || retries >= s.connect.MaxRetries {
			s.owner.EmitError(stt.ErrorEvent{Timestamp: time.Now(), Label: "elevenlabs.SpeechStream", Err: result.err, Recoverable: false})
			s.Finish(result.err)
			_ = s.BaseStream.Close()
			<-inputDone
			return
		}
		if err := waitRetry(ctx, retryDelay(s.connect, retries)); err != nil {
			s.Finish(nil)
			<-inputDone
			return
		}
		retries++
	}
}

func (s *SpeechStream) runSTTSession(ctx context.Context, opts resolvedSTTOptions, state *realtimeSTTState, inputCh <-chan sttInputResult) sttSessionResult {
	connectCtx, cancelConnect := withAttemptTimeout(ctx, s.connect.Timeout)
	conn, response, err := dialWebSocket(connectCtx, opts.wsDialer, realtimeSTTURL(opts, s.language), http.Header{
		AuthorizationHeader: []string{opts.apiKey},
		"User-Agent":        optionalHeader(opts.userAgent),
	})
	cancelConnect()
	if err != nil {
		if response != nil {
			defer response.Body.Close()
			body, readErr := readBounded(response.Body, opts.maxResponseBytes)
			if readErr == nil {
				return sttSessionResult{err: apiStatusError(response, body)}
			}
		}
		return sttSessionResult{err: normalizeNetworkError(err)}
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer conn.Close()
	conn.SetReadLimit(opts.maxWSMessageBytes)
	events := make(chan sttReadResult, opts.providerEventCap)
	readerDone := make(chan struct{})
	go readSTTMessages(sessionCtx, conn, events, readerDone)
	defer func() {
		cancel()
		_ = conn.Close()
		<-readerDone
	}()

	writePacket := func(packet []byte, commit bool) error {
		message := sttAudioPacket{
			MessageType: "input_audio_chunk", AudioBase64: base64.StdEncoding.EncodeToString(packet),
			Commit: commit, SampleRate: int(opts.sampleRate),
		}
		return writeJSON(sessionCtx, conn, opts.readWriteTimeout, message)
	}
	for _, packet := range state.replayPackets {
		if err := writePacket(packet, false); err != nil {
			return sttSessionResult{err: normalizeNetworkError(err)}
		}
	}
	if state.awaitingCommit {
		if err := writePacket(nil, true); err != nil {
			return sttSessionResult{err: normalizeNetworkError(err)}
		}
	}

	keepalive := time.NewTimer(sttKeepaliveInterval)
	defer keepalive.Stop()
	for {
		select {
		case <-ctx.Done():
			return sttSessionResult{ended: true}
		case <-keepalive.C:
			if err := writePacket(nil, false); err != nil {
				return sttSessionResult{err: normalizeNetworkError(err)}
			}
			keepalive.Reset(sttKeepaliveInterval)
		case <-s.updateSignal:
			updated := s.latestOptions()
			if len(state.replayPackets) == 0 && !state.awaitingCommit && state.packetizer.pendingLen() == 0 {
				return sttSessionResult{reconnect: true, nextOpts: &updated}
			}
			state.pendingOptions = &updated
		case input := <-inputCh:
			if input.err != nil {
				if !errors.Is(input.err, io.EOF) && !errors.Is(input.err, context.Canceled) {
					return sttSessionResult{err: input.err}
				}
				state.ending = true
				if !state.awaitingCommit {
					if err := s.flushAndCommit(sessionCtx, opts, state, writePacket); err != nil {
						return sttSessionResult{err: err}
					}
				}
				_ = conn.SetReadDeadline(time.Now().Add(max(opts.readWriteTimeout, time.Second)))
				continue
			}
			if input.input.Frame != nil {
				frame := *input.input.Frame
				if frame.SampleRate != int(opts.sampleRate) || frame.Channels != 1 {
					return sttSessionResult{err: fmt.Errorf("%w: got %d Hz/%d channels, want %d Hz/mono", agents.ErrInvalidAudioFormat, frame.SampleRate, frame.Channels, opts.sampleRate)}
				}
				data, err := pcmBytes(frame)
				if err != nil {
					return sttSessionResult{err: err}
				}
				for _, packet := range state.packetizer.write(data) {
					s.rememberSTTPacket(state, packet, opts.maxReplayBytes)
					if err := writePacket(packet, false); err != nil {
						return sttSessionResult{err: normalizeNetworkError(err)}
					}
				}
				state.usage += frame.Duration()
				s.emitPeriodicUsage(sessionCtx, state)
			}
			if input.input.Flush {
				if err := s.flushAndCommit(sessionCtx, opts, state, writePacket); err != nil {
					return sttSessionResult{err: err}
				}
			}
		case result := <-events:
			if result.err != nil {
				return sttSessionResult{err: result.err}
			}
			acceptedFinal, err := s.processRealtimeSTTEvent(sessionCtx, opts, state, result.event)
			if err != nil {
				return sttSessionResult{err: err}
			}
			if acceptedFinal {
				state.replayPackets = nil
				state.replayBytes = 0
				state.replayable = true
				state.awaitingCommit = false
				state.packetizer.reset()
				state.finalCount++
				if state.ending {
					return sttSessionResult{ended: true}
				}
				if state.pendingOptions != nil {
					next := cloneResolvedSTTOptions(*state.pendingOptions)
					state.pendingOptions = nil
					return sttSessionResult{reconnect: true, nextOpts: &next}
				}
			}
		}
	}
}

func (s *SpeechStream) rememberSTTPacket(state *realtimeSTTState, packet []byte, limit int) {
	if len(packet) == 0 || !state.replayable {
		return
	}
	if state.replayBytes+len(packet) > limit {
		state.replayPackets = nil
		state.replayBytes = 0
		state.replayable = false
		return
	}
	copyPacket := append([]byte(nil), packet...)
	state.replayPackets = append(state.replayPackets, copyPacket)
	state.replayBytes += len(copyPacket)
}

func (s *SpeechStream) flushAndCommit(ctx context.Context, opts resolvedSTTOptions, state *realtimeSTTState, writePacket func([]byte, bool) error) error {
	if packet := state.packetizer.flush(); len(packet) != 0 {
		s.rememberSTTPacket(state, packet, opts.maxReplayBytes)
		if err := writePacket(packet, false); err != nil {
			return normalizeNetworkError(err)
		}
	}
	// Record the commit intent before the write. If the socket fails during the
	// write, the next connection replays the turn and its commit atomically.
	state.awaitingCommit = true
	if err := writePacket(nil, true); err != nil {
		return normalizeNetworkError(err)
	}
	s.flushUsage(ctx, state)
	return nil
}

func (s *SpeechStream) emitPeriodicUsage(ctx context.Context, state *realtimeSTTState) {
	for state.usage >= 5*time.Second {
		_ = s.Emit(ctx, stt.SpeechEvent{Type: stt.RecognitionUsageEvent, RecognitionUsage: &stt.RecognitionUsage{AudioDuration: 5 * time.Second}})
		s.owner.EmitMetrics(metrics.STT{
			Label: "elevenlabs.SpeechStream", Timestamp: time.Now(), AudioDuration: 5 * time.Second, Streamed: true,
			Metadata: metrics.Metadata{ModelProvider: s.owner.Provider(), ModelName: s.owner.Model()},
		})
		state.usage -= 5 * time.Second
	}
}

func (s *SpeechStream) flushUsage(ctx context.Context, state *realtimeSTTState) {
	if state.usage <= 0 {
		return
	}
	duration := state.usage
	state.usage = 0
	_ = s.Emit(ctx, stt.SpeechEvent{Type: stt.RecognitionUsageEvent, RecognitionUsage: &stt.RecognitionUsage{AudioDuration: duration}})
	s.owner.EmitMetrics(metrics.STT{
		Label: "elevenlabs.SpeechStream", Timestamp: time.Now(), AudioDuration: duration, Streamed: true,
		Metadata: metrics.Metadata{ModelProvider: s.owner.Provider(), ModelName: s.owner.Model()},
	})
}

func (s *SpeechStream) processRealtimeSTTEvent(ctx context.Context, opts resolvedSTTOptions, state *realtimeSTTState, event realtimeSTTEvent) (bool, error) {
	switch event.MessageType {
	case "session_started":
		return false, nil
	case "warning":
		// The provider uses this for non-fatal notices such as a requested
		// zero-retention mode not being available for the account.
		return false, nil
	case "partial_transcript":
		if event.Text == "" {
			return false, nil
		}
		if !state.speaking {
			if err := s.Emit(ctx, stt.SpeechEvent{Type: stt.StartOfSpeech}); err != nil {
				return false, err
			}
			state.speaking = true
		}
		return false, s.Emit(ctx, stt.SpeechEvent{Type: stt.InterimTranscript, Alternatives: []stt.SpeechData{realtimeSpeechData(event, s.language, state.offset)}})
	case "committed_transcript":
		if opts.includeTimestamps || s.language == "" {
			return false, nil
		}
	case "committed_transcript_with_timestamps":
		if !opts.includeTimestamps && s.language != "" {
			return false, nil
		}
	default:
		if providerErrorType(event.MessageType) {
			return false, newSTTProviderError(event)
		}
		return false, &ProtocolError{Message: "unknown realtime STT message type " + event.MessageType}
	}
	if event.Text != "" {
		if !state.speaking {
			if err := s.Emit(ctx, stt.SpeechEvent{Type: stt.StartOfSpeech}); err != nil {
				return false, err
			}
			state.speaking = true
		}
		if err := s.Emit(ctx, stt.SpeechEvent{Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{realtimeSpeechData(event, s.language, state.offset)}}); err != nil {
			return false, err
		}
	}
	if opts.serverVAD != nil || event.Text == "" {
		if state.speaking {
			if err := s.Emit(ctx, stt.SpeechEvent{Type: stt.EndOfSpeech}); err != nil {
				return false, err
			}
			state.speaking = false
		}
	}
	return true, nil
}

type sttAudioPacket struct {
	MessageType string `json:"message_type"`
	AudioBase64 string `json:"audio_base_64"`
	Commit      bool   `json:"commit"`
	SampleRate  int    `json:"sample_rate"`
}

type realtimeSTTEvent struct {
	MessageType  string
	Text         string
	Words        []batchSTTWord
	LanguageCode string
	SessionID    string
	Message      string
	Details      string
}

type sttReadResult struct {
	event realtimeSTTEvent
	err   error
}

func readSTTMessages(ctx context.Context, conn *websocket.Conn, out chan<- sttReadResult, done chan<- struct{}) {
	defer close(done)
	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			err = websocketReadError(err, "ElevenLabs STT websocket closed")
			select {
			case out <- sttReadResult{err: err}:
			case <-ctx.Done():
			}
			return
		}
		if messageType != websocket.TextMessage {
			select {
			case out <- sttReadResult{err: &ProtocolError{Message: "realtime STT returned a non-text websocket frame"}}:
			case <-ctx.Done():
			}
			return
		}
		event, err := decodeRealtimeSTTEvent(payload)
		select {
		case out <- sttReadResult{event: event, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func decodeRealtimeSTTEvent(payload []byte) (realtimeSTTEvent, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return realtimeSTTEvent{}, &ProtocolError{Message: "invalid realtime STT JSON", Cause: err}
	}
	var event realtimeSTTEvent
	decodeStringAliases(raw, &event.MessageType, "message_type", "messageType", "type")
	decodeStringAliases(raw, &event.Text, "text")
	decodeStringAliases(raw, &event.LanguageCode, "language_code", "languageCode")
	decodeStringAliases(raw, &event.SessionID, "session_id", "sessionId")
	decodeStringAliases(raw, &event.Message, "message", "error")
	decodeStringAliases(raw, &event.Details, "details")
	if value, ok := firstRaw(raw, "words"); ok && len(value) != 0 && string(value) != "null" {
		if err := json.Unmarshal(value, &event.Words); err != nil {
			return realtimeSTTEvent{}, &ProtocolError{Message: "invalid realtime STT words", Cause: err}
		}
	}
	if event.MessageType == "" {
		return realtimeSTTEvent{}, &ProtocolError{Message: "realtime STT message has no type"}
	}
	return event, nil
}

func realtimeSpeechData(event realtimeSTTEvent, requested agents.LanguageCode, offset time.Duration) stt.SpeechData {
	language := agents.NormalizeLanguage(event.LanguageCode)
	if language == "" {
		language = requested
	}
	if language == "" {
		language = agents.AsLanguageCode("en")
	}
	data := stt.SpeechData{Language: language, Text: event.Text, Confidence: speechConfidence(event.Words)}
	if len(event.Words) != 0 {
		data.StartTime = secondsDuration(numberOrZero(event.Words[0].Start)) + offset
		data.EndTime = secondsDuration(numberOrZero(event.Words[len(event.Words)-1].End)) + offset
		data.Words = make([]agents.TimedString, len(event.Words))
		for i, word := range event.Words {
			start := secondsDuration(numberOrZero(word.Start)) + offset
			end := secondsDuration(numberOrZero(word.End)) + offset
			offsetCopy := offset
			data.Words[i] = agents.CreateTimedString(agents.TimedStringOptions{
				Text: word.Text, StartTime: &start, EndTime: &end, StartTimeOffset: &offsetCopy,
			})
		}
	}
	return data
}

func providerErrorType(messageType string) bool {
	switch messageType {
	case "auth_error", "quota_exceeded", "transcriber_error", "input_error", "error",
		"throttled", "commit_throttled", "rate_limited", "queue_overflow", "resource_exhausted",
		"session_time_limit", "session_time_limit_exceeded", "invalid_request", "chunk_too_large",
		"chunk_size_exceeded", "unaccepted_terms", "insufficient_audio_activity":
		return true
	default:
		return false
	}
}

func newSTTProviderError(event realtimeSTTEvent) error {
	retryable := false
	switch event.MessageType {
	case "transcriber_error", "error", "throttled", "commit_throttled", "rate_limited", "queue_overflow", "resource_exhausted", "session_time_limit", "session_time_limit_exceeded", "insufficient_audio_activity":
		retryable = true
	}
	return &ProviderError{Type: event.MessageType, Message: event.Message, Details: event.Details, RetryableFlag: retryable, Body: event}
}

func realtimeSTTURL(opts resolvedSTTOptions, language agents.LanguageCode) string {
	u := websocketURL(opts.baseURL, "speech-to-text", "realtime")
	query := u.Query()
	query.Set("model_id", string(opts.model))
	query.Set("audio_format", fmt.Sprintf("pcm_%d", opts.sampleRate))
	if opts.serverVAD == nil {
		query.Set("commit_strategy", "manual")
	} else {
		query.Set("commit_strategy", "vad")
	}
	query.Set("enable_logging", boolString(opts.enableLogging))
	if language == "" {
		query.Set("include_language_detection", "true")
	} else {
		query.Set("language_code", string(language))
	}
	if opts.includeTimestamps {
		query.Set("include_timestamps", "true")
	}
	if opts.noVerbatim {
		query.Set("no_verbatim", "true")
	}
	if opts.serverVAD != nil {
		if value := opts.serverVAD.VADSilenceThreshold; value != nil {
			query.Set("vad_silence_threshold_secs", formatSeconds(*value))
		}
		if value := opts.serverVAD.VADThreshold; value != nil {
			query.Set("vad_threshold", fmt.Sprintf("%g", *value))
		}
		if value := opts.serverVAD.MinSpeechDuration; value != nil {
			query.Set("min_speech_duration_ms", fmt.Sprintf("%d", value.Milliseconds()))
		}
		if value := opts.serverVAD.MinSilenceDuration; value != nil {
			query.Set("min_silence_duration_ms", fmt.Sprintf("%d", value.Milliseconds()))
		}
	}
	for _, keyterm := range mergedKeyterms(opts.userKeyterms, opts.sessionKeyterms) {
		query.Add("keyterms", keyterm)
	}
	u.RawQuery = query.Encode()
	return u.String()
}

type pcmPacketizer struct {
	frameBytes int
	pending    []byte
}

func newPCMPacketizer(samplesPerChannel, channels int) *pcmPacketizer {
	return &pcmPacketizer{frameBytes: max(1, samplesPerChannel*channels*2)}
}

func (p *pcmPacketizer) setFrameSamples(samples int) { p.frameBytes = max(2, samples*2) }
func (p *pcmPacketizer) pendingLen() int             { return len(p.pending) }
func (p *pcmPacketizer) reset()                      { p.pending = nil }

func (p *pcmPacketizer) write(data []byte) [][]byte {
	if len(data) == 0 {
		return nil
	}
	p.pending = append(p.pending, data...)
	count := len(p.pending) / p.frameBytes
	packets := make([][]byte, 0, count)
	for len(p.pending) >= p.frameBytes {
		packet := make([]byte, p.frameBytes)
		copy(packet, p.pending[:p.frameBytes])
		packets = append(packets, packet)
		p.pending = p.pending[p.frameBytes:]
	}
	if len(p.pending) == 0 {
		p.pending = nil
	} else {
		p.pending = append([]byte(nil), p.pending...)
	}
	return packets
}

func (p *pcmPacketizer) flush() []byte {
	if len(p.pending) == 0 {
		return nil
	}
	packet := append([]byte(nil), p.pending...)
	p.pending = nil
	return packet
}

func dialWebSocket(ctx context.Context, dialer *websocket.Dialer, target string, header http.Header) (*websocket.Conn, *http.Response, error) {
	clean := make(http.Header, len(header))
	for key, values := range header {
		for _, value := range values {
			if value != "" {
				clean.Add(key, value)
			}
		}
	}
	return dialer.DialContext(ctx, target, clean)
}

func writeJSON(ctx context.Context, conn *websocket.Conn, timeout time.Duration, value any) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
	}
	return conn.WriteJSON(value)
}

func websocketReadError(err error, message string) error {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		retryable := closeErr.Code != websocket.CloseNormalClosure && closeErr.Code != websocket.CloseGoingAway
		return agents.NewAPIStatusError(message, closeErr.Code, "", map[string]any{"reason": closeErr.Text}, retryable, err)
	}
	return agents.NewAPIConnectionError(message, true, err)
}

func optionalHeader(value string) []string {
	if value == "" {
		return nil
	}
	return []string{value}
}

func firstRaw(values map[string]json.RawMessage, keys ...string) (json.RawMessage, bool) {
	for _, key := range keys {
		if value, ok := values[key]; ok {
			return value, true
		}
	}
	return nil, false
}

func decodeStringAliases(values map[string]json.RawMessage, destination *string, keys ...string) {
	value, ok := firstRaw(values, keys...)
	if !ok || len(value) == 0 || string(value) == "null" {
		return
	}
	_ = json.Unmarshal(value, destination)
}

func formatSeconds(duration time.Duration) string {
	return fmt.Sprintf("%g", float64(duration)/float64(time.Second))
}

var _ stt.SpeechStream = (*SpeechStream)(nil)
