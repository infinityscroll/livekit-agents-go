// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/tokenize"
	"github.com/infinityscroll/livekit-agents-go/tts"
)

const (
	defaultInferenceTTSSessionDuration = 5 * time.Minute
	defaultMaxPendingAlignmentTokens   = 16 << 10
)

// ErrAlignmentBufferLimit reports a server stream that sends unbounded
// timestamp metadata without enough audio to drain it.
var ErrAlignmentBufferLimit = errors.New("inference TTS: pending alignment buffer limit exceeded")

// TTSOptions configures the LiveKit Cloud Inference streaming TTS adapter.
type TTSOptions struct {
	Model              string
	Voice              string
	Language           agents.LanguageCode
	Encoding           string
	SampleRate         int
	BaseURL            string
	Credentials        Credentials
	ModelOptions       ModelOptions
	Fallback           []TTSFallbackModel
	ConnectOptions     agents.APIConnectOptions
	Metadata           func(context.Context) RequestMetadata
	WebSocketDialer    *websocket.Dialer
	InputCapacity      int
	OutputCapacity     int
	MaxMessageBytes    int64
	MaxSessionDuration time.Duration
	// AlignmentDecoder customizes provider timestamp decoding. Nil uses the
	// cross-SDK Cloud Inference word/character decoder.
	AlignmentDecoder TTSAlignmentDecoder
	// MaxPendingAlignmentTokens bounds timestamp messages received before an
	// audio frame. Zero selects a production default.
	MaxPendingAlignmentTokens int
}

// TTSUpdateOptions is a sparse update. Pointer fields distinguish omission
// from an explicit empty value; ModelOptions are shallow-merged.
type TTSUpdateOptions struct {
	Model        *string
	Voice        *string
	Language     *agents.LanguageCode
	ModelOptions ModelOptions
}

type resolvedTTSOptions struct {
	generation                uint64
	model                     string
	voice                     string
	language                  agents.LanguageCode
	encoding                  string
	sampleRate                int
	baseURL                   string
	credentials               Credentials
	modelOptions              ModelOptions
	fallback                  []TTSFallbackModel
	connect                   agents.APIConnectOptions
	metadata                  func(context.Context) RequestMetadata
	dialer                    *websocket.Dialer
	inputCapacity             int
	outputCapacity            int
	maxMessageBytes           int64
	maxSessionDuration        time.Duration
	alignmentDecoder          TTSAlignmentDecoder
	maxPendingAlignmentTokens int
}

// TTS implements tts.TTS using pooled LiveKit Inference WebSockets.
type TTS struct {
	*tts.Base

	mu        sync.RWMutex
	opts      resolvedTTSOptions
	streams   map[*SynthesizeStream]struct{}
	closed    bool
	pool      *agents.ConnectionPool[*gatewayWS]
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func NewTTS(options TTSOptions) (*TTS, error) {
	resolved, err := resolveTTSOptions(options)
	if err != nil {
		return nil, err
	}
	client := &TTS{
		Base: tts.NewBase("inference.TTS", "livekit", resolved.model, resolved.sampleRate, 1, tts.Capabilities{
			Streaming: true, AlignedTranscript: TTSHasAlignedTranscript(resolved.model, resolved.modelOptions),
		}),
		opts: resolved, streams: make(map[*SynthesizeStream]struct{}), closeDone: make(chan struct{}),
	}
	client.SetMarkupProviderKey(inferenceMarkupProvider(resolved.model))
	pool, err := agents.NewConnectionPool(agents.ConnectionPoolOptions[*gatewayWS]{
		MaxSessionDuration: resolved.maxSessionDuration,
		MarkRefreshedOnGet: true,
		ConnectTimeout:     resolved.connect.Timeout,
		Connect:            client.connectTTS,
		Close: func(_ context.Context, conn *gatewayWS) error {
			return conn.Close()
		},
	})
	if err != nil {
		return nil, err
	}
	client.pool = pool
	return client, nil
}

// TTSFromModelString parses provider/model:voice and constructs a TTS.
func TTSFromModelString(model string) (*TTS, error) {
	parsed, voice := ParseTTSModelString(model)
	return NewTTS(TTSOptions{Model: parsed, Voice: voice})
}

func resolveTTSOptions(options TTSOptions) (resolvedTTSOptions, error) {
	credentials, err := options.Credentials.Resolve()
	if err != nil {
		return resolvedTTSOptions{}, err
	}
	model, modelVoice := ParseTTSModelString(strings.TrimSpace(options.Model))
	if model == "" {
		return resolvedTTSOptions{}, errors.New("inference TTS: model is required")
	}
	if options.Voice == "" {
		options.Voice = modelVoice
	}
	if options.Language == "" {
		options.Language = agents.AsLanguageCode("en")
	}
	if options.Encoding == "" {
		options.Encoding = defaultInferenceEncoding
	}
	if options.Encoding != defaultInferenceEncoding {
		return resolvedTTSOptions{}, fmt.Errorf("inference TTS: unsupported encoding %q", options.Encoding)
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
	if options.MaxSessionDuration <= 0 {
		options.MaxSessionDuration = defaultInferenceTTSSessionDuration
	}
	if options.MaxPendingAlignmentTokens <= 0 {
		options.MaxPendingAlignmentTokens = defaultMaxPendingAlignmentTokens
	}
	if options.AlignmentDecoder == nil {
		options.AlignmentDecoder = DefaultTTSAlignmentDecoder()
	}
	return resolvedTTSOptions{
		generation: 1,
		model:      model, voice: options.Voice, language: agents.NormalizeLanguage(string(options.Language)),
		encoding: options.Encoding, sampleRate: options.SampleRate,
		baseURL: strings.TrimRight(options.BaseURL, "/"), credentials: credentials,
		modelOptions: cloneModelOptions(options.ModelOptions), fallback: cloneTTSFallback(options.Fallback),
		connect: options.ConnectOptions.Resolve(), metadata: options.Metadata, dialer: options.WebSocketDialer,
		inputCapacity: options.InputCapacity, outputCapacity: options.OutputCapacity,
		maxMessageBytes: options.MaxMessageBytes, maxSessionDuration: options.MaxSessionDuration,
		alignmentDecoder:          options.AlignmentDecoder,
		maxPendingAlignmentTokens: options.MaxPendingAlignmentTokens,
	}, nil
}

func cloneTTSFallback(values []TTSFallbackModel) []TTSFallbackModel {
	result := make([]TTSFallbackModel, len(values))
	for i, value := range values {
		result[i] = value
		result[i].Extra = cloneModelOptions(value.Extra)
	}
	return result
}

// MarshalJSON keeps the gateway's fallback shape exact: extra is always an
// object, even when the provider has no fallback-specific options.
func (model TTSFallbackModel) MarshalJSON() ([]byte, error) {
	type wire struct {
		Model string       `json:"model"`
		Voice string       `json:"voice"`
		Extra ModelOptions `json:"extra"`
	}
	extra := model.Extra
	if extra == nil {
		extra = ModelOptions{}
	}
	return json.Marshal(wire{Model: model.Model, Voice: model.Voice, Extra: extra})
}

func cloneResolvedTTS(value resolvedTTSOptions) resolvedTTSOptions {
	value.modelOptions = cloneModelOptions(value.modelOptions)
	value.fallback = cloneTTSFallback(value.fallback)
	return value
}

func inferenceMarkupProvider(model string) string {
	provider := inferenceProvider(model)
	if provider == tts.ProviderInworld && !strings.Contains(model, "tts-2") {
		return ""
	}
	return provider
}

func inferenceProvider(model string) string {
	provider, _, _ := strings.Cut(model, "/")
	return provider
}

func (t *TTS) Synthesize(context.Context, string, tts.SynthesizeOptions) (tts.ChunkedStream, error) {
	return nil, agents.NewAPIError(
		"LiveKit Inference TTS does not support chunked synthesis; use Stream", nil, false, nil,
	)
}

func (t *TTS) Stream(ctx context.Context, options tts.StreamOptions) (tts.SynthesizeStream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	resolved := cloneResolvedTTS(t.opts)
	if options.ConnectOptions != (agents.APIConnectOptions{}) {
		resolved.connect = options.ConnectOptions.Resolve()
	}
	stream := newInferenceSynthesizeStream(ctx, t, resolved, t.Expressive())
	t.streams[stream] = struct{}{}
	t.mu.Unlock()
	stream.start()
	return stream, nil
}

// UpdateOptions updates future pooled sessions and live generation settings.
// Stale idle connections are rejected by generation on their next checkout;
// checked-out streams remain usable and receive generation_config updates.
func (t *TTS) UpdateOptions(update TTSUpdateOptions) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return io.ErrClosedPipe
	}
	changed := false
	if update.Model != nil {
		model, suffixVoice := ParseTTSModelString(*update.Model)
		if model == "" {
			t.mu.Unlock()
			return errors.New("inference TTS: model cannot be empty")
		}
		t.opts.model = model
		if update.Voice == nil && suffixVoice != "" {
			t.opts.voice = suffixVoice
		}
		changed = true
	}
	if update.Voice != nil {
		t.opts.voice = *update.Voice
		changed = true
	}
	if update.Language != nil {
		t.opts.language = agents.NormalizeLanguage(string(*update.Language))
		changed = true
	}
	if update.ModelOptions != nil {
		for key, value := range update.ModelOptions {
			t.opts.modelOptions[key] = value
		}
		changed = true
	}
	if !changed {
		t.mu.Unlock()
		return nil
	}
	t.opts.generation++
	snapshot := cloneResolvedTTS(t.opts)
	streams := make([]*SynthesizeStream, 0, len(t.streams))
	for stream := range t.streams {
		streams = append(streams, stream)
	}
	t.mu.Unlock()

	t.SetModel(snapshot.model)
	t.SetMarkupProviderKey(inferenceMarkupProvider(snapshot.model))
	for _, stream := range streams {
		stream.requestOptions(snapshot)
	}
	return nil
}

// Capabilities reflects live option updates, including provider flags which
// enable alignment after construction.
func (t *TTS) Capabilities() tts.Capabilities {
	t.mu.RLock()
	aligned := TTSHasAlignedTranscript(t.opts.model, t.opts.modelOptions)
	t.mu.RUnlock()
	return tts.Capabilities{Streaming: true, AlignedTranscript: aligned}
}

// Prewarm opens one authenticated gateway connection in the background.
func (t *TTS) Prewarm(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	t.pool.Prewarm(ctx)
}

func (t *TTS) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		streams := make([]*SynthesizeStream, 0, len(t.streams))
		for stream := range t.streams {
			streams = append(streams, stream)
		}
		t.mu.Unlock()
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
			closeErr = errors.Join(closeErr, t.pool.Close(context.Background()))
			t.mu.Lock()
			t.closeErr = closeErr
			t.mu.Unlock()
			close(t.closeDone)
		}()
	})
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.closeDone:
		t.mu.RLock()
		err := t.closeErr
		t.mu.RUnlock()
		return err
	}
}

func (t *TTS) connectTTS(ctx context.Context) (*gatewayWS, error) {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil, io.ErrClosedPipe
	}
	options := cloneResolvedTTS(t.opts)
	t.mu.RUnlock()
	conn, err := dialGatewayWS(ctx, gatewayWSDialOptions{
		BaseURL: options.baseURL, Endpoint: "tts", Credentials: options.credentials,
		Metadata: options.metadata, Dialer: options.dialer, ReadLimit: options.maxMessageBytes,
		WriteTimeout: options.connect.Timeout,
	})
	if err != nil {
		return nil, err
	}
	event := TTSSessionCreate{
		SampleRate: fmt.Sprint(options.sampleRate), Encoding: options.encoding,
		Model: options.model, Voice: options.voice, Language: string(options.language),
		Extra:      cloneModelOptions(options.modelOptions),
		Connection: &ConnectionSettings{TimeoutSeconds: options.connect.Timeout.Seconds(), Retries: options.connect.MaxRetries},
	}
	if len(options.fallback) != 0 {
		event.Fallback = &TTSFallback{Models: cloneTTSFallback(options.fallback)}
	}
	if err := conn.WriteJSON(ctx, event); err != nil {
		_ = conn.Close()
		return nil, err
	}
	conn.generation = options.generation
	return conn, nil
}

func (t *TTS) removeStream(stream *SynthesizeStream) {
	t.mu.Lock()
	delete(t.streams, stream)
	t.mu.Unlock()
}

func (t *TTS) generation() uint64 {
	t.mu.RLock()
	generation := t.opts.generation
	t.mu.RUnlock()
	return generation
}

type SynthesizeStream struct {
	*tts.BaseSynthesizeStream
	owner      *TTS
	opts       resolvedTTSOptions
	expressive bool

	updateMu     sync.Mutex
	pendingOpts  resolvedTTSOptions
	updateSignal chan struct{}
	done         chan struct{}
}

type ttsTokenResult struct {
	token tokenize.TokenData
	err   error
}

func newInferenceSynthesizeStream(ctx context.Context, owner *TTS, options resolvedTTSOptions, expressive bool) *SynthesizeStream {
	return &SynthesizeStream{
		BaseSynthesizeStream: tts.NewBaseSynthesizeStream(ctx, max(options.inputCapacity, options.outputCapacity)),
		owner:                owner, opts: options, expressive: expressive,
		pendingOpts: cloneResolvedTTS(options), updateSignal: make(chan struct{}, 1), done: make(chan struct{}),
	}
}

func (s *SynthesizeStream) start() {
	go func() {
		defer close(s.done)
		defer s.owner.removeStream(s)
		err := s.run()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			s.owner.EmitError(tts.ErrorEvent{Timestamp: time.Now(), Label: "inference.SynthesizeStream", Err: err})
		}
		if context.Cause(s.Context()) != nil {
			err = nil
		}
		s.Finish(err)
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

func (s *SynthesizeStream) requestOptions(options resolvedTTSOptions) {
	s.updateMu.Lock()
	s.pendingOpts = cloneResolvedTTS(options)
	s.updateMu.Unlock()
	select {
	case s.updateSignal <- struct{}{}:
	default:
	}
}

func (s *SynthesizeStream) takeOptions() resolvedTTSOptions {
	s.updateMu.Lock()
	options := cloneResolvedTTS(s.pendingOpts)
	s.updateMu.Unlock()
	return options
}

func (s *SynthesizeStream) run() (runErr error) {
	ctx, cancelRun := context.WithCancel(s.Context())
	alignmentProvider := inferenceProvider(s.opts.model)
	provider := inferenceMarkupProvider(s.opts.model)
	tokenizer := tts.SentenceTokenizer(alignmentProvider, s.expressive).Stream()
	tokens := make(chan ttsTokenResult, 1)
	inputDone := make(chan struct{})
	tokenDone := make(chan struct{})

	go s.pumpText(ctx, tokenizer, provider, inputDone)
	go func() {
		defer close(tokenDone)
		for {
			token, err := tokenizer.Recv(ctx)
			select {
			case tokens <- ttsTokenResult{token: token, err: err}:
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
		_ = tokenizer.Abort(context.Canceled)
		<-inputDone
		<-tokenDone
	}()

	var (
		conn *gatewayWS
		err  error
	)
	for {
		conn, err = s.owner.pool.Get(ctx)
		if err != nil {
			return err
		}
		if conn.generation == s.owner.generation() {
			break
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = s.owner.pool.Remove(cleanupCtx, conn)
		cleanupCancel()
		if err != nil {
			return err
		}
	}
	healthy := false
	defer func() {
		if healthy && runErr == nil && s.owner.pool.Put(conn) {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if removeErr := s.owner.pool.Remove(cleanupCtx, conn); removeErr != nil && runErr == nil {
			runErr = removeErr
		}
	}()

	reader := startWSReader(conn)
	readerStopped := false
	defer func() {
		if !readerStopped {
			reader.Stop()
			_ = conn.Close()
		}
		_ = reader.Wait(context.Background())
	}()

	audio, err := agents.NewAudioByteStream(s.opts.sampleRate, 1, 0)
	if err != nil {
		return err
	}
	requestID := agents.ShortUUID("tts_request_")
	sessionID := requestID
	var lastFrame *agents.AudioFrame
	var pendingTimed []agents.TimedString
	var cueStripper tts.BracketCueStripper
	var idleTimer *time.Timer
	var idle <-chan time.Time
	var started, firstByte time.Time
	var audioDuration time.Duration
	var characters int64
	flushed := false
	defer func() {
		if idleTimer != nil {
			idleTimer.Stop()
		}
		if started.IsZero() {
			started = time.Now()
		}
		s.owner.EmitMetrics(metrics.TTS{
			Label: "inference.SynthesizeStream", RequestID: requestID, SegmentID: sessionID,
			Timestamp: time.Now(), TimeToFirstByte: durationSince(started, firstByte),
			Duration: time.Since(started), AudioDuration: audioDuration,
			Cancelled: context.Cause(ctx) != nil, CharactersCount: characters, Streamed: true,
			Metadata: metrics.Metadata{ModelProvider: s.owner.Provider(), ModelName: s.opts.model},
		})
	}()

	resetIdle := func() {
		if idleTimer == nil {
			idleTimer = time.NewTimer(s.opts.connect.Timeout)
			idle = idleTimer.C
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(s.opts.connect.Timeout)
	}
	emitLast := func(final bool) error {
		if lastFrame == nil {
			return nil
		}
		frame := *lastFrame
		lastFrame = nil
		if firstByte.IsZero() {
			firstByte = time.Now()
		}
		audioDuration += frame.Duration()
		result := tts.SynthesizedAudio{
			RequestID: requestID, SegmentID: sessionID, Frame: frame,
			Final: final, TimedTranscripts: pendingTimed,
		}
		pendingTimed = nil
		return s.Emit(ctx, result)
	}

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-idle:
			return agents.NewAPITimeoutError("LiveKit Inference TTS receive idle timeout", true, nil)
		case <-s.updateSignal:
			s.opts = s.takeOptions()
		case token := <-tokens:
			if token.err != nil {
				if !errors.Is(token.err, io.EOF) {
					return token.err
				}
				if !flushed {
					if started.IsZero() {
						started = time.Now()
					}
					if err := conn.WriteJSON(ctx, struct {
						Type string `json:"type"`
					}{Type: "session.flush"}); err != nil {
						return err
					}
					flushed = true
					resetIdle()
				}
				continue
			}
			if started.IsZero() {
				started = time.Now()
			}
			s.opts = s.takeOptions()
			text := token.token.Token
			provider = inferenceMarkupProvider(s.opts.model)
			if s.expressive {
				text = tts.ConvertMarkup(provider, tts.NormalizeMarkup(provider, text))
			}
			characters += int64(utf8.RuneCountInString(text))
			config := &TTSGenerationConfig{Voice: s.opts.voice, Language: string(s.opts.language), Model: s.opts.model}
			event := struct {
				Type             string               `json:"type"`
				Transcript       string               `json:"transcript"`
				GenerationConfig *TTSGenerationConfig `json:"generation_config"`
				Extra            ModelOptions         `json:"extra"`
			}{
				Type: "input_transcript", Transcript: text + " ", GenerationConfig: config,
				Extra: cloneModelOptions(s.opts.modelOptions),
			}
			if err := conn.WriteJSON(ctx, event); err != nil {
				return err
			}
			resetIdle()
		case result, ok := <-reader.events:
			if !ok {
				return agents.NewAPIConnectionError("LiveKit Inference TTS connection closed unexpectedly", true, nil)
			}
			if result.Err != nil {
				return agents.NewAPIConnectionError("LiveKit Inference TTS connection closed unexpectedly", true, result.Err)
			}
			if idleTimer != nil {
				resetIdle()
			}
			event, decodeErr := DecodeTTSServerEvent(result.Data)
			if decodeErr != nil {
				result.Resume()
				if errors.Is(decodeErr, ErrInvalidEvent) {
					continue
				}
				return decodeErr
			}
			if !event.Known() {
				result.Resume()
				continue
			}
			if event.SessionID != "" {
				sessionID = event.SessionID
				requestID = event.SessionID
			}
			switch event.Type {
			case "session.created":
				result.Resume()
			case "output_audio":
				decoded, err := base64.StdEncoding.DecodeString(event.Audio)
				if err != nil {
					result.Resume()
					return agents.NewAPIError("LiveKit Inference TTS returned invalid base64 audio", nil, false, err)
				}
				for _, frame := range audio.Write(decoded) {
					if err := emitLast(false); err != nil {
						result.Resume()
						return err
					}
					frameCopy := frame
					lastFrame = &frameCopy
				}
				result.Resume()
			case "output_alignment":
				aligned, err := s.opts.alignmentDecoder.DecodeTTSAlignment(ctx, event, alignmentProvider)
				if err != nil {
					result.Resume()
					return fmt.Errorf("decode inference TTS alignment: %w", err)
				}
				if s.expressive {
					aligned = cueStripper.Push(aligned)
				}
				if len(aligned) > s.opts.maxPendingAlignmentTokens-len(pendingTimed) {
					result.Resume()
					return ErrAlignmentBufferLimit
				}
				pendingTimed = append(pendingTimed, aligned...)
				result.Resume()
			case "done":
				result.Stop()
				reader.Stop()
				readerStopped = true
				if err := reader.Wait(ctx); err != nil {
					return err
				}
				if s.expressive {
					pendingTimed = append(pendingTimed, cueStripper.Flush()...)
				}
				frames, flushErr := audio.Flush()
				if flushErr != nil {
					return flushErr
				}
				for _, frame := range frames {
					if err := emitLast(false); err != nil {
						return err
					}
					frameCopy := frame
					lastFrame = &frameCopy
				}
				if err := emitLast(true); err != nil {
					return err
				}
				healthy = true
				return nil
			case "session.closed":
				result.Stop()
				reader.Stop()
				readerStopped = true
				_ = reader.Wait(ctx)
				return nil
			case "error":
				result.Stop()
				reader.Stop()
				readerStopped = true
				_ = reader.Wait(ctx)
				return agents.NewAPIError("LiveKit Inference TTS returned an error: "+event.Message, event, false, nil)
			}
		}
	}
}

func (s *SynthesizeStream) pumpText(ctx context.Context, tokenizer *tokenize.SentenceStream, provider string, done chan<- struct{}) {
	defer close(done)
	for {
		input, err := s.Inputs().Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				_ = tokenizer.EndInput(ctx)
			} else if context.Cause(ctx) == nil {
				_ = tokenizer.Abort(err)
			}
			return
		}
		if input.Text != "" {
			text := input.Text
			if s.expressive {
				text = tts.NormalizeMarkup(provider, text)
			}
			if err := tokenizer.PushText(ctx, text); err != nil {
				_ = tokenizer.Abort(err)
				return
			}
		}
		if input.Flush {
			if err := tokenizer.Flush(ctx); err != nil {
				_ = tokenizer.Abort(err)
				return
			}
		}
	}
}

func durationSince(start, end time.Time) time.Duration {
	if start.IsZero() || end.IsZero() {
		return 0
	}
	return end.Sub(start)
}

var _ tts.TTS = (*TTS)(nil)
var _ tts.SynthesizeStream = (*SynthesizeStream)(nil)
