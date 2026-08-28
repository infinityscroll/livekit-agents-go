// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/tts"
)

type TTS struct {
	*tts.Base

	optsMu sync.RWMutex
	opts   resolvedTTSOptions

	gate        chan struct{}
	connMu      sync.Mutex
	current     *ttsConnection
	connections map[*ttsConnection]struct{}
	streams     map[*SynthesizeStream]struct{}
	chunks      map[*ChunkedStream]struct{}
	closed      atomic.Bool
	closeDone   chan struct{}
}

func NewTTS(options TTSOptions) (*TTS, error) {
	resolved, err := resolveTTSOptions(options)
	if err != nil {
		return nil, err
	}
	client := &TTS{
		Base: tts.NewBase("elevenlabs.TTS", "ElevenLabs", string(resolved.model), resolved.sampleRate, 1, tts.Capabilities{
			Streaming: true, AlignedTranscript: resolved.syncAlignment,
		}),
		opts: resolved, gate: make(chan struct{}, 1),
		connections: make(map[*ttsConnection]struct{}),
		streams:     make(map[*SynthesizeStream]struct{}), chunks: make(map[*ChunkedStream]struct{}),
		closeDone: make(chan struct{}),
	}
	client.gate <- struct{}{}
	return client, nil
}

func (t *TTS) Synthesize(ctx context.Context, text string, options tts.SynthesizeOptions) (tts.ChunkedStream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts, err := t.snapshotOptions()
	if err != nil {
		return nil, err
	}
	if !isPCMEncoding(opts.encoding) {
		return nil, &UnsupportedEncodingError{Encoding: opts.encoding, Reason: "the agents audio contract requires decoded PCM; no MP3 decoder is installed"}
	}
	stream := newChunkedStream(ctx, t, text, opts, options.ConnectOptions.Resolve())
	t.connMu.Lock()
	if t.closed.Load() {
		t.connMu.Unlock()
		return nil, io.ErrClosedPipe
	}
	t.chunks[stream] = struct{}{}
	t.connMu.Unlock()
	stream.start()
	return stream, nil
}

func (t *TTS) Stream(ctx context.Context, options tts.StreamOptions) (tts.SynthesizeStream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts, err := t.snapshotOptions()
	if err != nil {
		return nil, err
	}
	if !isPCMEncoding(opts.encoding) {
		return nil, &UnsupportedEncodingError{Encoding: opts.encoding, Reason: "the agents audio contract requires decoded PCM; no MP3 decoder is installed"}
	}
	if opts.model == ElevenV3 {
		return nil, &UnsupportedTransportError{Model: opts.model, Transport: "TTS multi-context websocket"}
	}
	stream := newSynthesizeStream(ctx, t, opts, options.ConnectOptions.Resolve())
	t.connMu.Lock()
	if t.closed.Load() {
		t.connMu.Unlock()
		return nil, io.ErrClosedPipe
	}
	t.streams[stream] = struct{}{}
	t.connMu.Unlock()
	stream.start()
	return stream, nil
}

func (t *TTS) UpdateOptions(ctx context.Context, update TTSUpdateOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := t.lockGate(ctx); err != nil {
		return err
	}
	defer t.unlockGate()

	t.optsMu.Lock()
	if t.closed.Load() {
		t.optsMu.Unlock()
		return io.ErrClosedPipe
	}
	next := cloneResolvedTTSOptions(t.opts)
	changed := false
	if update.VoiceID != nil && *update.VoiceID != next.voiceID {
		if *update.VoiceID == "" {
			t.optsMu.Unlock()
			return &ProtocolError{Message: "voice ID cannot be empty"}
		}
		next.voiceID = *update.VoiceID
		changed = true
	}
	if value, ok := update.VoiceSettings.Value(); ok {
		if err := validateVoiceSettings(&value); err != nil {
			t.optsMu.Unlock()
			return err
		}
		next.voiceSettings = cloneVoiceSettings(&value)
		changed = true
	} else if update.VoiceSettings.IsDisabled() {
		next.voiceSettings = nil
		changed = true
	}
	if update.Model != nil && *update.Model != next.model {
		if *update.Model == "" {
			t.optsMu.Unlock()
			return &ProtocolError{Message: "model cannot be empty"}
		}
		next.model = *update.Model
		changed = true
	}
	if value, ok := update.Language.Value(); ok {
		normalized := agents.NormalizeLanguage(string(value))
		if normalized != next.language {
			next.language = normalized
			changed = true
		}
	} else if update.Language.IsDisabled() && next.language != "" {
		next.language = ""
		changed = true
	}
	if value, ok := update.PronunciationDictionaryLocators.Value(); ok {
		if err := validateDictionaries(value); err != nil {
			t.optsMu.Unlock()
			return err
		}
		next.pronunciationDictionaries = cloneDictionaries(value)
		changed = true
	} else if update.PronunciationDictionaryLocators.IsDisabled() {
		next.pronunciationDictionaries = nil
		changed = true
	}
	if changed {
		next.generation++
		t.opts = next
		t.SetModel(string(next.model))
	}
	t.optsMu.Unlock()
	if !changed {
		return nil
	}

	t.connMu.Lock()
	old := t.current
	t.current = nil
	t.connMu.Unlock()
	if old != nil {
		old.markRetired()
	}
	return nil
}

func (t *TTS) ListVoices(ctx context.Context) ([]Voice, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts, err := t.snapshotOptions()
	if err != nil {
		return nil, err
	}
	requestCtx, cancel := withAttemptTimeout(ctx, agents.DefaultAPIConnectOptions.Timeout)
	defer cancel()
	u := endpointURL(opts.baseURL, "voices")
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set(AuthorizationHeader, opts.apiKey)
	if opts.userAgent != "" {
		request.Header.Set("User-Agent", opts.userAgent)
	}
	response, err := opts.httpClient.Do(request)
	if err != nil {
		return nil, normalizeNetworkError(err)
	}
	defer response.Body.Close()
	body, err := readBounded(response.Body, opts.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, apiStatusError(response, body)
	}
	if err := requireJSONContentType(response.Header.Get("Content-Type")); err != nil {
		return nil, err
	}
	var payload struct {
		Voices []Voice `json:"voices"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, &ProtocolError{Message: "invalid voices JSON", Cause: err}
	}
	return payload.Voices, nil
}

func (t *TTS) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := t.lockGate(ctx); err != nil {
		return err
	}
	t.optsMu.Lock()
	if t.closed.Load() {
		t.optsMu.Unlock()
		t.unlockGate()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-t.closeDone:
			return nil
		}
	}
	t.closed.Store(true)
	t.optsMu.Unlock()
	defer close(t.closeDone)

	t.connMu.Lock()
	streams := make([]*SynthesizeStream, 0, len(t.streams))
	for stream := range t.streams {
		streams = append(streams, stream)
	}
	chunks := make([]*ChunkedStream, 0, len(t.chunks))
	for chunk := range t.chunks {
		chunks = append(chunks, chunk)
	}
	connections := make([]*ttsConnection, 0, len(t.connections))
	for connection := range t.connections {
		connections = append(connections, connection)
	}
	t.current = nil
	t.connMu.Unlock()
	t.unlockGate()

	for _, stream := range streams {
		_ = stream.Close()
	}
	for _, chunk := range chunks {
		_ = chunk.Close()
	}
	for _, connection := range connections {
		connection.requestClose()
	}
	for _, stream := range streams {
		if err := stream.Wait(ctx); err != nil {
			return err
		}
	}
	for _, chunk := range chunks {
		if err := chunk.Wait(ctx); err != nil {
			return err
		}
	}
	for _, connection := range connections {
		if err := connection.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *TTS) snapshotOptions() (resolvedTTSOptions, error) {
	t.optsMu.RLock()
	defer t.optsMu.RUnlock()
	if t.closed.Load() {
		return resolvedTTSOptions{}, io.ErrClosedPipe
	}
	return cloneResolvedTTSOptions(t.opts), nil
}

func (t *TTS) lockGate(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.gate:
		return nil
	}
}

func (t *TTS) unlockGate() { t.gate <- struct{}{} }

func (t *TTS) removeStream(stream *SynthesizeStream) {
	t.connMu.Lock()
	delete(t.streams, stream)
	t.connMu.Unlock()
}

func (t *TTS) removeChunk(stream *ChunkedStream) {
	t.connMu.Lock()
	delete(t.chunks, stream)
	t.connMu.Unlock()
}

func (t *TTS) removeConnection(connection *ttsConnection) {
	t.connMu.Lock()
	delete(t.connections, connection)
	if t.current == connection {
		t.current = nil
	}
	t.connMu.Unlock()
}

func cloneResolvedTTSOptions(value resolvedTTSOptions) resolvedTTSOptions {
	value.baseURL = cloneURL(value.baseURL)
	value.voiceSettings = cloneVoiceSettings(value.voiceSettings)
	value.streamingLatency = cloneInt(value.streamingLatency)
	value.chunkLengthSchedule = cloneInts(value.chunkLengthSchedule)
	value.applyLanguageTextNormalization = cloneBool(value.applyLanguageTextNormalization)
	value.pronunciationDictionaries = cloneDictionaries(value.pronunciationDictionaries)
	return value
}

var _ tts.TTS = (*TTS)(nil)
