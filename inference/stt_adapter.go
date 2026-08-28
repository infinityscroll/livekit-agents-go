// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stt"
)

const (
	defaultInferenceSampleRate = 16_000
	defaultInferenceEncoding   = "pcm_s16le"
	defaultInferenceCapacity   = 32
)

// STTOptions configures the LiveKit Cloud Inference streaming STT adapter.
// Model may be "auto" or empty to let the gateway select a provider.
type STTOptions struct {
	Model           string
	Language        agents.LanguageCode
	Encoding        string
	SampleRate      int
	BaseURL         string
	Credentials     Credentials
	ModelOptions    ModelOptions
	Fallback        []STTFallbackModel
	ConnectOptions  agents.APIConnectOptions
	Metadata        func(context.Context) RequestMetadata
	WebSocketDialer *websocket.Dialer
	InputCapacity   int
	OutputCapacity  int
	MaxMessageBytes int64
	SessionKeyterms []string
}

// STTUpdateOptions is a sparse live update. Pointer fields distinguish an
// omitted value from an explicit empty value; ModelOptions are shallow-merged.
type STTUpdateOptions struct {
	Model        *string
	Language     *agents.LanguageCode
	ModelOptions ModelOptions
}

type resolvedSTTOptions struct {
	model           string
	language        agents.LanguageCode
	encoding        string
	sampleRate      int
	baseURL         string
	credentials     Credentials
	modelOptions    ModelOptions
	fallback        []STTFallbackModel
	connect         agents.APIConnectOptions
	metadata        func(context.Context) RequestMetadata
	dialer          *websocket.Dialer
	inputCapacity   int
	outputCapacity  int
	maxMessageBytes int64
	sessionKeyterms []string
}

// STT implements stt.STT using the LiveKit Inference WebSocket protocol.
type STT struct {
	*stt.Base

	mu        sync.RWMutex
	opts      resolvedSTTOptions
	streams   map[*SpeechStream]struct{}
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func NewSTT(options STTOptions) (*STT, error) {
	resolved, err := resolveSTTOptions(options)
	if err != nil {
		return nil, err
	}
	models := make([]string, 0, len(resolved.fallback)+1)
	models = append(models, resolved.model)
	for _, fallback := range resolved.fallback {
		models = append(models, fallback.Model)
	}
	client := &STT{
		Base: stt.NewBase("inference.STT", "livekit", displaySTTModel(resolved.model), stt.Capabilities{
			Streaming:         true,
			InterimResults:    true,
			AlignedTranscript: STTAlignedTranscript(models...),
			Diarization:       STTDiarizationEnabled(resolved.modelOptions),
			Keyterms:          STTSupportsKeyterms(resolved.model),
		}),
		opts: resolved, streams: make(map[*SpeechStream]struct{}), closeDone: make(chan struct{}),
	}
	return client, nil
}

// STTFromModelString parses provider/model:language and constructs an STT.
func STTFromModelString(model string) (*STT, error) {
	parsed, language := ParseSTTModelString(model)
	return NewSTT(STTOptions{Model: parsed, Language: language})
}

func resolveSTTOptions(options STTOptions) (resolvedSTTOptions, error) {
	credentials, err := options.Credentials.Resolve()
	if err != nil {
		return resolvedSTTOptions{}, err
	}
	model, modelLanguage := ParseSTTModelString(strings.TrimSpace(options.Model))
	if options.Language == "" {
		options.Language = modelLanguage
	}
	if model == "auto" {
		model = ""
	}
	if options.Encoding == "" {
		options.Encoding = defaultInferenceEncoding
	}
	if options.Encoding != defaultInferenceEncoding {
		return resolvedSTTOptions{}, fmt.Errorf("inference STT: unsupported encoding %q", options.Encoding)
	}
	if options.SampleRate <= 0 {
		options.SampleRate = defaultInferenceSampleRate
	}
	if options.BaseURL == "" {
		options.BaseURL = DefaultURLFromEnvironment()
	}
	if options.InputCapacity <= 0 {
		options.InputCapacity = defaultInferenceCapacity
	}
	if options.OutputCapacity <= 0 {
		options.OutputCapacity = defaultInferenceCapacity
	}
	if options.MaxMessageBytes <= 0 {
		options.MaxMessageBytes = MaxControlMessageBytes
	}
	return resolvedSTTOptions{
		model: model, language: agents.NormalizeLanguage(string(options.Language)),
		encoding: options.Encoding, sampleRate: options.SampleRate,
		baseURL: strings.TrimRight(options.BaseURL, "/"), credentials: credentials,
		modelOptions: cloneModelOptions(options.ModelOptions), fallback: cloneSTTFallback(options.Fallback),
		connect: options.ConnectOptions.Resolve(), metadata: options.Metadata, dialer: options.WebSocketDialer,
		inputCapacity: options.InputCapacity, outputCapacity: options.OutputCapacity,
		maxMessageBytes: options.MaxMessageBytes, sessionKeyterms: append([]string(nil), options.SessionKeyterms...),
	}, nil
}

func displaySTTModel(model string) string {
	if model == "" {
		return "auto"
	}
	return model
}

func cloneSTTFallback(values []STTFallbackModel) []STTFallbackModel {
	result := make([]STTFallbackModel, len(values))
	for i, value := range values {
		result[i] = value
		result[i].Extra = cloneModelOptions(value.Extra)
	}
	return result
}

// MarshalJSON keeps the gateway's fallback shape exact: extra is always an
// object, even when the provider has no fallback-specific options.
func (model STTFallbackModel) MarshalJSON() ([]byte, error) {
	type wire struct {
		Model string       `json:"model"`
		Extra ModelOptions `json:"extra"`
	}
	extra := model.Extra
	if extra == nil {
		extra = ModelOptions{}
	}
	return json.Marshal(wire{Model: model.Model, Extra: extra})
}

func cloneResolvedSTT(value resolvedSTTOptions) resolvedSTTOptions {
	value.modelOptions = cloneModelOptions(value.modelOptions)
	value.fallback = cloneSTTFallback(value.fallback)
	value.sessionKeyterms = append([]string(nil), value.sessionKeyterms...)
	return value
}

func (s *STT) Recognize(context.Context, []agents.AudioFrame, stt.RecognizeOptions) (stt.SpeechEvent, error) {
	return stt.SpeechEvent{}, agents.NewAPIError(
		"LiveKit Inference STT does not support batch recognition; use Stream", nil, false, nil,
	)
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
	resolved := cloneResolvedSTT(s.opts)
	if options.Language != "" {
		resolved.language = agents.NormalizeLanguage(string(options.Language))
	}
	if options.ConnectOptions != (agents.APIConnectOptions{}) {
		resolved.connect = options.ConnectOptions.Resolve()
	}
	stream := newInferenceSpeechStream(ctx, s, resolved)
	s.streams[stream] = struct{}{}
	s.mu.Unlock()
	stream.start()
	return stream, nil
}

// UpdateOptions shallow-merges provider options and propagates a session.update
// to every active stream without reconnecting it.
func (s *STT) UpdateOptions(update STTUpdateOptions) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return io.ErrClosedPipe
	}
	var wire sttStreamUpdate
	if update.Model != nil {
		model, suffixLanguage := ParseSTTModelString(*update.Model)
		if model == "auto" {
			model = ""
		}
		s.opts.model = model
		wireModel := model
		if wireModel == "" {
			wireModel = "auto"
		}
		wire.model = &wireModel
		if update.Language == nil && suffixLanguage != "" {
			language := suffixLanguage
			s.opts.language = language
			wire.language = &language
		}
	}
	if update.Language != nil {
		language := agents.NormalizeLanguage(string(*update.Language))
		s.opts.language = language
		wire.language = &language
	}
	if update.ModelOptions != nil {
		for key, value := range update.ModelOptions {
			s.opts.modelOptions[key] = value
		}
		wire.extra = cloneModelOptions(update.ModelOptions)
	}
	if update.Model != nil || update.ModelOptions != nil {
		if overlay, ok := MergeSTTKeyterms(s.opts.model, s.opts.modelOptions, s.opts.sessionKeyterms); ok {
			if wire.extra == nil {
				wire.extra = make(ModelOptions)
			}
			for key, value := range overlay {
				wire.extra[key] = value
			}
		}
	}
	if wire.empty() {
		s.mu.Unlock()
		return nil
	}
	snapshot := cloneResolvedSTT(s.opts)
	streams := make([]*SpeechStream, 0, len(s.streams))
	for stream := range s.streams {
		streams = append(streams, stream)
	}
	s.mu.Unlock()
	s.SetModel(displaySTTModel(snapshot.model))
	s.UpdateCapabilities(func(capabilities *stt.Capabilities) {
		models := make([]string, 0, len(snapshot.fallback)+1)
		models = append(models, snapshot.model)
		for _, fallback := range snapshot.fallback {
			models = append(models, fallback.Model)
		}
		capabilities.AlignedTranscript = STTAlignedTranscript(models...)
		capabilities.Diarization = STTDiarizationEnabled(snapshot.modelOptions)
		capabilities.Keyterms = STTSupportsKeyterms(snapshot.model)
	})
	for _, stream := range streams {
		stream.requestUpdate(wire)
	}
	return nil
}

// UpdateSessionKeyterms overlays framework-managed terms on user options. For
// an active utterance the update is deferred until EndOfSpeech.
func (s *STT) UpdateSessionKeyterms(keyterms []string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return io.ErrClosedPipe
	}
	extra, supported := MergeSTTKeyterms(s.opts.model, s.opts.modelOptions, keyterms)
	if !supported {
		s.mu.Unlock()
		return nil
	}
	s.opts.sessionKeyterms = append(s.opts.sessionKeyterms[:0], keyterms...)
	streams := make([]*SpeechStream, 0, len(s.streams))
	for stream := range s.streams {
		streams = append(streams, stream)
	}
	s.mu.Unlock()
	for _, stream := range streams {
		stream.requestUpdate(sttStreamUpdate{extra: cloneModelOptions(extra), deferWhileSpeaking: true})
	}
	return nil
}

func (s *STT) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		streams := make([]*SpeechStream, 0, len(s.streams))
		for stream := range s.streams {
			streams = append(streams, stream)
		}
		s.mu.Unlock()
		for _, stream := range streams {
			_ = stream.Close()
		}
		go func() {
			var closeErr error
			for _, stream := range streams {
				if err := stream.Wait(context.Background()); err != nil {
					closeErr = errors.Join(closeErr, err)
				}
			}
			s.mu.Lock()
			s.closeErr = closeErr
			s.mu.Unlock()
			close(s.closeDone)
		}()
	})
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.closeDone:
		s.mu.RLock()
		err := s.closeErr
		s.mu.RUnlock()
		return err
	}
}

func (s *STT) removeStream(stream *SpeechStream) {
	s.mu.Lock()
	delete(s.streams, stream)
	s.mu.Unlock()
}

type sttStreamUpdate struct {
	model              *string
	language           *agents.LanguageCode
	extra              ModelOptions
	deferWhileSpeaking bool
}

func (u sttStreamUpdate) empty() bool { return u.model == nil && u.language == nil && u.extra == nil }

func mergeSTTStreamUpdate(target *sttStreamUpdate, update sttStreamUpdate) {
	if update.model != nil {
		value := *update.model
		target.model = &value
	}
	if update.language != nil {
		value := *update.language
		target.language = &value
	}
	if update.extra != nil {
		if target.extra == nil {
			target.extra = make(ModelOptions)
		}
		for key, value := range update.extra {
			target.extra[key] = value
		}
	}
	target.deferWhileSpeaking = target.deferWhileSpeaking || update.deferWhileSpeaking
}

type SpeechStream struct {
	*stt.BaseStream
	owner *STT
	opts  resolvedSTTOptions

	updateMu        sync.Mutex
	pending         sttStreamUpdate
	pendingDeferred sttStreamUpdate
	updateSignal    chan struct{}
	done            chan struct{}
}

type sttInputResult struct {
	input stt.StreamInput
	err   error
}

func newInferenceSpeechStream(ctx context.Context, owner *STT, options resolvedSTTOptions) *SpeechStream {
	return &SpeechStream{
		BaseStream: stt.NewBaseStream(ctx, max(options.inputCapacity, options.outputCapacity)),
		owner:      owner, opts: options, updateSignal: make(chan struct{}, 1), done: make(chan struct{}),
	}
}

func (s *SpeechStream) start() {
	go func() {
		defer close(s.done)
		defer s.owner.removeStream(s)
		err := s.run()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			s.owner.EmitError(stt.ErrorEvent{Timestamp: time.Now(), Label: "inference.SpeechStream", Err: err})
		}
		if context.Cause(s.Context()) != nil {
			err = nil
		}
		s.Finish(err)
	}()
}

func (s *SpeechStream) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.done:
		return nil
	}
}

func (s *SpeechStream) requestUpdate(update sttStreamUpdate) {
	s.updateMu.Lock()
	if update.deferWhileSpeaking {
		update.deferWhileSpeaking = false
		mergeSTTStreamUpdate(&s.pendingDeferred, update)
	} else {
		mergeSTTStreamUpdate(&s.pending, update)
	}
	s.updateMu.Unlock()
	select {
	case s.updateSignal <- struct{}{}:
	default:
	}
}

func (s *SpeechStream) takeUpdate() (sttStreamUpdate, sttStreamUpdate) {
	s.updateMu.Lock()
	update, deferred := s.pending, s.pendingDeferred
	s.pending = sttStreamUpdate{}
	s.pendingDeferred = sttStreamUpdate{}
	s.updateMu.Unlock()
	return update, deferred
}

func (s *SpeechStream) run() error {
	ctx, cancelRun := context.WithCancel(s.Context())
	input := make(chan sttInputResult, 1)
	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		for {
			value, err := s.Inputs().Recv(ctx)
			select {
			case input <- sttInputResult{input: value, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		cancelRun()
		<-inputDone
	}()

	conn, err := s.connect(ctx)
	if err != nil {
		return err
	}
	reader := startWSReader(conn)
	defer func() {
		reader.Stop()
		_ = conn.Close()
		_ = reader.Wait(context.Background())
	}()

	packetizer := newInferencePCMPacketizer(s.opts.sampleRate / 20)
	requestID := agents.ShortUUID("stt_request_")
	var speechDuration time.Duration
	var speaking, ending, closed bool
	var deferred sttStreamUpdate
	var idle <-chan time.Time
	var idleTimer *time.Timer
	defer func() {
		if idleTimer != nil {
			idleTimer.Stop()
		}
	}()

	for !closed {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-idle:
			return agents.NewAPITimeoutError("LiveKit Inference STT timed out waiting for session closure", true, nil)
		case <-s.updateSignal:
			update, calmUpdate := s.takeUpdate()
			if !update.empty() {
				if err := s.applyUpdate(ctx, conn, update); err != nil {
					return err
				}
			}
			if !calmUpdate.empty() {
				if speaking {
					mergeSTTStreamUpdate(&deferred, calmUpdate)
				} else if err := s.applyUpdate(ctx, conn, calmUpdate); err != nil {
					return err
				}
			}
		case received := <-input:
			if received.err != nil {
				if !errors.Is(received.err, io.EOF) && !errors.Is(received.err, context.Canceled) {
					return received.err
				}
				if err := packetizer.flush(func(packet []byte) error {
					return writeSTTAudio(ctx, conn, packet)
				}); err != nil {
					return err
				}
				if !ending {
					if err := conn.WriteJSON(ctx, struct {
						Type string `json:"type"`
					}{Type: "session.finalize"}); err != nil {
						return err
					}
					ending = true
					idleTimer = time.NewTimer(s.opts.connect.Timeout)
					idle = idleTimer.C
				}
				continue
			}
			if received.input.Frame != nil {
				frame := *received.input.Frame
				if frame.SampleRate != s.opts.sampleRate || frame.Channels != 1 {
					return fmt.Errorf("%w: got %d Hz/%d channels, want %d Hz/mono", agents.ErrInvalidAudioFormat, frame.SampleRate, frame.Channels, s.opts.sampleRate)
				}
				speechDuration = saturatingDurationAdd(speechDuration, frame.Duration())
				if err := packetizer.writeFrame(frame, func(packet []byte) error {
					return writeSTTAudio(ctx, conn, packet)
				}); err != nil {
					return err
				}
			}
			if received.input.Flush {
				if err := packetizer.flush(func(packet []byte) error {
					return writeSTTAudio(ctx, conn, packet)
				}); err != nil {
					return err
				}
			}
		case result, ok := <-reader.events:
			if !ok {
				if closed {
					return nil
				}
				return agents.NewAPIConnectionError("LiveKit Inference STT connection closed unexpectedly", true, nil)
			}
			if result.Err != nil {
				return agents.NewAPIConnectionError("LiveKit Inference STT connection closed unexpectedly", true, result.Err)
			}
			event, decodeErr := DecodeSTTServerEvent(result.Data)
			result.Resume()
			if decodeErr != nil {
				if errors.Is(decodeErr, ErrInvalidEvent) {
					continue
				}
				return decodeErr
			}
			if !event.Known() {
				continue
			}
			if event.SessionID != "" {
				requestID = event.SessionID
			}
			switch event.Type {
			case "session.created", "session.finalized":
			case "session.closed":
				closed = true
			case "start_of_speech":
				if !speaking {
					if err := s.Emit(ctx, stt.SpeechEvent{Type: stt.StartOfSpeech}); err != nil {
						return err
					}
					speaking = true
				}
			case "interim_transcript", "final_transcript", "preflight_transcript":
				typeOf := stt.InterimTranscript
				if event.Type == "final_transcript" {
					typeOf = stt.FinalTranscript
				} else if event.Type == "preflight_transcript" {
					typeOf = stt.PreflightTranscript
				}
				if event.Transcript == "" && typeOf != stt.FinalTranscript {
					continue
				}
				if !speaking {
					if err := s.Emit(ctx, stt.SpeechEvent{Type: stt.StartOfSpeech}); err != nil {
						return err
					}
					speaking = true
				}
				if typeOf == stt.FinalTranscript && speechDuration > 0 {
					usage := speechDuration
					speechDuration = 0
					if err := s.Emit(ctx, stt.SpeechEvent{Type: stt.RecognitionUsageEvent, RequestID: requestID, RecognitionUsage: &stt.RecognitionUsage{AudioDuration: usage}}); err != nil {
						return err
					}
					s.owner.EmitMetrics(metrics.STT{Label: "inference.SpeechStream", RequestID: requestID, Timestamp: time.Now(), AudioDuration: usage, Streamed: true, Metadata: metrics.Metadata{ModelProvider: s.owner.Provider(), ModelName: s.owner.Model()}})
				}
				if err := s.Emit(ctx, stt.SpeechEvent{Type: typeOf, RequestID: requestID, Alternatives: []stt.SpeechData{s.speechData(event)}}); err != nil {
					return err
				}
				if typeOf == stt.FinalTranscript && speaking {
					speaking = false
					if err := s.Emit(ctx, stt.SpeechEvent{Type: stt.EndOfSpeech}); err != nil {
						return err
					}
					if !deferred.empty() {
						if err := s.applyUpdate(ctx, conn, deferred); err != nil {
							return err
						}
						deferred = sttStreamUpdate{}
					}
				}
			case "error":
				return agents.NewAPIError("LiveKit Inference STT returned an error: "+event.Message, event, false, nil)
			}
		}
	}
	return nil
}

func (s *SpeechStream) connect(ctx context.Context) (*gatewayWS, error) {
	var lastErr error
	for attempt := 0; attempt <= s.opts.connect.MaxRetries; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, s.opts.connect.Timeout)
		conn, err := dialGatewayWS(attemptCtx, gatewayWSDialOptions{
			BaseURL: s.opts.baseURL, Endpoint: "stt", Credentials: s.opts.credentials,
			Metadata: s.opts.metadata, Dialer: s.opts.dialer, ReadLimit: s.opts.maxMessageBytes,
			WriteTimeout: s.opts.connect.Timeout,
		})
		if err == nil {
			err = conn.WriteJSON(attemptCtx, s.sessionCreate())
		}
		cancel()
		if err == nil {
			return conn, nil
		}
		if conn != nil {
			_ = conn.Close()
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if attempt == s.opts.connect.MaxRetries || !inferenceRetryable(err) {
			break
		}
		timer := time.NewTimer(s.opts.connect.RetryDelay(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, context.Cause(ctx)
		case <-timer.C:
		}
	}
	return nil, lastErr
}

func (s *SpeechStream) sessionCreate() STTSessionCreate {
	extra := cloneModelOptions(s.opts.modelOptions)
	if overlay, ok := MergeSTTKeyterms(s.opts.model, s.opts.modelOptions, s.opts.sessionKeyterms); ok {
		for key, value := range overlay {
			extra[key] = value
		}
	}
	event := STTSessionCreate{
		Model:      s.opts.model,
		Settings:   STTSettings{SampleRate: fmt.Sprint(s.opts.sampleRate), Encoding: s.opts.encoding, Language: string(s.opts.language), Extra: extra},
		Connection: &ConnectionSettings{TimeoutSeconds: s.opts.connect.Timeout.Seconds(), Retries: s.opts.connect.MaxRetries},
	}
	if len(s.opts.fallback) != 0 {
		event.Fallback = &STTFallback{Models: cloneSTTFallback(s.opts.fallback)}
	}
	return event
}

func (s *SpeechStream) applyUpdate(ctx context.Context, conn *gatewayWS, update sttStreamUpdate) error {
	settings := STTUpdateSettings{}
	if update.model != nil {
		if *update.model == "auto" {
			s.opts.model = ""
		} else {
			s.opts.model = *update.model
		}
		settings.Model = *update.model
	}
	if update.language != nil {
		s.opts.language = *update.language
		settings.Language = string(*update.language)
	}
	if update.extra != nil {
		for key, value := range update.extra {
			s.opts.modelOptions[key] = value
		}
		settings.Extra = cloneModelOptions(update.extra)
	}
	return conn.WriteJSON(ctx, STTSessionUpdate{Settings: settings})
}

func (s *SpeechStream) speechData(event STTServerEvent) stt.SpeechData {
	language := agents.NormalizeLanguage(event.Language)
	if language == "" {
		language = s.opts.language
	}
	if language == "" {
		language = agents.AsLanguageCode("en")
	}
	start := inferenceSeconds(event.Start)
	end := saturatingDurationAdd(start, inferenceSeconds(event.Duration))
	data := stt.SpeechData{
		Language: language, Text: event.Transcript, StartTime: start, EndTime: end,
		Confidence: event.Confidence, Words: make([]agents.TimedString, len(event.Words)),
	}
	if event.SpeakerID != nil {
		data.SpeakerID = *event.SpeakerID
	}
	if len(event.Extra) != 0 && string(event.Extra) != "null" {
		var metadata map[string]any
		if json.Unmarshal(event.Extra, &metadata) == nil && len(metadata) != 0 {
			data.Metadata = metadata
		}
	}
	for i, word := range event.Words {
		wordStart, wordEnd := inferenceSeconds(word.Start), inferenceSeconds(word.End)
		confidence := word.Confidence
		offset := time.Duration(0)
		data.Words[i] = agents.CreateTimedString(agents.TimedStringOptions{
			Text: word.Word, StartTime: &wordStart, EndTime: &wordEnd,
			StartTimeOffset: &offset, Confidence: &confidence, SpeakerID: word.SpeakerID,
		})
	}
	return data
}

func inferenceSeconds(value float64) time.Duration {
	if value <= 0 || math.IsNaN(value) {
		return 0
	}
	if value >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(value * float64(time.Second))
}

func saturatingDurationAdd(left, right time.Duration) time.Duration {
	if right > 0 && left > time.Duration(math.MaxInt64)-right {
		return time.Duration(math.MaxInt64)
	}
	return left + right
}

func inferenceRetryable(err error) bool {
	type retryable interface{ Retryable() bool }
	var value retryable
	return errors.As(err, &value) && value.Retryable()
}

type inferencePCMPacketizer struct {
	frameSamples int
	pending      []byte
}

func newInferencePCMPacketizer(frameSamples int) *inferencePCMPacketizer {
	return &inferencePCMPacketizer{frameSamples: max(frameSamples, 1), pending: make([]byte, 0, max(frameSamples, 1)*2)}
}

func (p *inferencePCMPacketizer) writeFrame(frame agents.AudioFrame, emit func([]byte) error) error {
	packetBytes := p.frameSamples * 2
	for _, sample := range frame.Data {
		value := uint16(sample)
		p.pending = append(p.pending, byte(value), byte(value>>8))
		if len(p.pending) != packetBytes {
			continue
		}
		if err := emit(p.pending); err != nil {
			return err
		}
		p.pending = p.pending[:0]
	}
	return nil
}

func (p *inferencePCMPacketizer) flush(emit func([]byte) error) error {
	if len(p.pending) == 0 {
		return nil
	}
	if err := emit(p.pending); err != nil {
		return err
	}
	p.pending = p.pending[:0]
	return nil
}

func writeSTTAudio(ctx context.Context, conn *gatewayWS, packet []byte) error {
	event := struct {
		Type  string `json:"type"`
		Audio string `json:"audio"`
	}{Type: "input_audio", Audio: base64.StdEncoding.EncodeToString(packet)}
	return conn.WriteJSON(ctx, event)
}

var _ stt.STT = (*STT)(nil)
var _ stt.SpeechStream = (*SpeechStream)(nil)
