// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/tokenize"
)

const (
	defaultStreamCapacity     = 32
	defaultWriterCapacity     = 64
	defaultProviderEventCap   = 16
	defaultMaxResponseBytes   = 1 << 20
	defaultMaxWSMessageBytes  = 4 << 20
	defaultSTTReplayDuration  = 30 * time.Second
	defaultWSInactivity       = 180 * time.Second
	defaultReadWriteTimeout   = 10 * time.Second
	defaultMaxActiveContexts  = 5
	maxProviderContextCount   = 5
	maxProviderInactivity     = 180 * time.Second
	maxBatchKeyterms          = 1000
	maxRealtimeKeyterms       = 50
	maxBatchKeytermRunes      = 50
	maxRealtimeKeytermRunes   = 20
	maxKeytermNormalizedWords = 5
)

// VADOptions configures ElevenLabs realtime server-side VAD. Durations are
// serialized in the units required by the provider.
type VADOptions struct {
	VADSilenceThreshold *time.Duration
	VADThreshold        *float64
	MinSpeechDuration   *time.Duration
	MinSilenceDuration  *time.Duration
}

type STTOptions struct {
	APIKey            string
	BaseURL           string
	Language          agents.LanguageCode
	LanguageCode      agents.LanguageCode // Deprecated: use Language.
	TagAudioEvents    *bool
	UseRealtime       *bool // Deprecated: select ScribeV2Realtime with Model.
	SampleRate        STTRealtimeSampleRate
	ServerVAD         *VADOptions
	IncludeTimestamps bool
	Model             STTModel
	ModelID           STTModel // Deprecated: use Model.
	Keyterms          []string
	NoVerbatim        bool
	EnableLogging     *bool

	HTTPClient        *http.Client
	WebSocketDialer   *websocket.Dialer
	InputCapacity     int
	OutputCapacity    int
	ProviderEventCap  int
	MaxResponseBytes  int64
	MaxWSMessageBytes int64
	MaxReplayDuration time.Duration
	ReadWriteTimeout  time.Duration
	UserAgent         string
}

type STTUpdateOptions struct {
	TagAudioEvents *bool
	ServerVAD      agents.Override[VADOptions]
	Keyterms       agents.Override[[]string]
	NoVerbatim     *bool
}

type resolvedSTTOptions struct {
	apiKey            string
	baseURL           *url.URL
	language          agents.LanguageCode
	tagAudioEvents    bool
	sampleRate        STTRealtimeSampleRate
	serverVAD         *VADOptions
	includeTimestamps bool
	model             STTModel
	userKeyterms      []string
	sessionKeyterms   []string
	noVerbatim        bool
	enableLogging     bool
	httpClient        *http.Client
	wsDialer          *websocket.Dialer
	inputCapacity     int
	outputCapacity    int
	providerEventCap  int
	maxResponseBytes  int64
	maxWSMessageBytes int64
	maxReplayBytes    int
	readWriteTimeout  time.Duration
	userAgent         string
}

type VoiceSettings struct {
	Stability       float64  `json:"stability"`
	SimilarityBoost float64  `json:"similarity_boost"`
	Style           *float64 `json:"style,omitempty"`
	Speed           *float64 `json:"speed,omitempty"`
	UseSpeakerBoost *bool    `json:"use_speaker_boost,omitempty"`
}

type Voice struct {
	ID       string         `json:"voice_id"`
	Name     string         `json:"name"`
	Category string         `json:"category"`
	Settings *VoiceSettings `json:"settings,omitempty"`
}

type PronunciationDictionaryLocator struct {
	PronunciationDictionaryID string `json:"pronunciation_dictionary_id"`
	VersionID                 string `json:"version_id"`
}

// TextTokenizer is implemented by the core WordTokenizer and
// SentenceTokenizer. Spans allow the provider stream to retain an incomplete
// suffix across arbitrary caller chunk boundaries.
type TextTokenizer interface {
	TokenizeSpans(string) []tokenize.Span
}

type TTSOptions struct {
	APIKey        string
	VoiceID       string
	VoiceSettings *VoiceSettings
	Model         TTSModel
	Language      agents.LanguageCode

	Voice        *Voice              // Deprecated: use VoiceID and VoiceSettings.
	ModelID      TTSModel            // Deprecated: use Model.
	LanguageCode agents.LanguageCode // Deprecated: use Language.

	BaseURL                         string
	Encoding                        TTSEncoding
	StreamingLatency                *int
	Tokenizer                       TextTokenizer
	WordTokenizer                   TextTokenizer // Deprecated: use Tokenizer.
	ChunkLengthSchedule             []int
	EnableSSMLParsing               bool
	EnableLogging                   *bool
	InactivityTimeout               time.Duration
	SyncAlignment                   *bool
	ApplyTextNormalization          TextNormalization
	ApplyLanguageTextNormalization  *bool
	PreferredAlignment              PreferredAlignment
	AutoMode                        *bool
	PronunciationDictionaryLocators []PronunciationDictionaryLocator

	HTTPClient        *http.Client
	WebSocketDialer   *websocket.Dialer
	InputCapacity     int
	OutputCapacity    int
	WriterCapacity    int
	ProviderEventCap  int
	MaxResponseBytes  int64
	MaxWSMessageBytes int64
	ReadWriteTimeout  time.Duration
	MaxActiveContexts int
	UserAgent         string
}

type TTSUpdateOptions struct {
	VoiceID                         *string
	VoiceSettings                   agents.Override[VoiceSettings]
	Model                           *TTSModel
	Language                        agents.Override[agents.LanguageCode]
	PronunciationDictionaryLocators agents.Override[[]PronunciationDictionaryLocator]
}

type resolvedTTSOptions struct {
	generation                     uint64
	apiKey                         string
	voiceID                        string
	voiceSettings                  *VoiceSettings
	model                          TTSModel
	language                       agents.LanguageCode
	baseURL                        *url.URL
	encoding                       TTSEncoding
	sampleRate                     int
	streamingLatency               *int
	tokenizer                      TextTokenizer
	chunkLengthSchedule            []int
	enableSSMLParsing              bool
	enableLogging                  bool
	inactivityTimeout              time.Duration
	syncAlignment                  bool
	applyTextNormalization         TextNormalization
	applyLanguageTextNormalization *bool
	preferredAlignment             PreferredAlignment
	autoMode                       bool
	pronunciationDictionaries      []PronunciationDictionaryLocator
	httpClient                     *http.Client
	wsDialer                       *websocket.Dialer
	inputCapacity                  int
	outputCapacity                 int
	writerCapacity                 int
	providerEventCap               int
	maxResponseBytes               int64
	maxWSMessageBytes              int64
	readWriteTimeout               time.Duration
	maxActiveContexts              int
	userAgent                      string
}

func resolveSTTOptions(options STTOptions) (resolvedSTTOptions, error) {
	apiKey, err := resolveAPIKey(options.APIKey)
	if err != nil {
		return resolvedSTTOptions{}, err
	}
	baseURL, err := resolveBaseURL(options.BaseURL)
	if err != nil {
		return resolvedSTTOptions{}, err
	}

	model := options.Model
	if model == "" {
		model = options.ModelID
	}
	if model == "" && options.UseRealtime != nil {
		if *options.UseRealtime {
			model = ScribeV2Realtime
		} else {
			model = ScribeV1
		}
	}
	if model == "" {
		model = DefaultSTTModel
	}
	language := options.Language
	if language == "" {
		language = options.LanguageCode
	}
	language = agents.NormalizeLanguage(string(language))

	tagAudioEvents := true
	if options.TagAudioEvents != nil {
		tagAudioEvents = *options.TagAudioEvents
	}
	enableLogging := true
	if options.EnableLogging != nil {
		enableLogging = *options.EnableLogging
	}
	sampleRate := options.SampleRate
	if sampleRate == 0 {
		sampleRate = SampleRate16000
	}
	if !validRealtimeSampleRate(sampleRate) {
		return resolvedSTTOptions{}, fmt.Errorf("elevenlabs: unsupported realtime sample rate %d", sampleRate)
	}
	if err := validateVADOptions(options.ServerVAD); err != nil {
		return resolvedSTTOptions{}, err
	}
	if err := validateKeyterms(options.Keyterms, model == ScribeV2Realtime); err != nil {
		return resolvedSTTOptions{}, err
	}

	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	wsDialer := options.WebSocketDialer
	if wsDialer == nil {
		wsDialer = websocket.DefaultDialer
	}
	inputCapacity := positiveOr(options.InputCapacity, defaultStreamCapacity)
	outputCapacity := positiveOr(options.OutputCapacity, defaultStreamCapacity)
	eventCapacity := positiveOr(options.ProviderEventCap, defaultProviderEventCap)
	maxResponse := positiveInt64Or(options.MaxResponseBytes, defaultMaxResponseBytes)
	maxWSMessage := positiveInt64Or(options.MaxWSMessageBytes, defaultMaxWSMessageBytes)
	replayDuration := options.MaxReplayDuration
	if replayDuration <= 0 {
		replayDuration = defaultSTTReplayDuration
	}
	readWriteTimeout := options.ReadWriteTimeout
	if readWriteTimeout <= 0 {
		readWriteTimeout = defaultReadWriteTimeout
	}
	bytesPerSecond := int64(sampleRate) * 2
	maxReplayBytes64 := int64(replayDuration/time.Second)*bytesPerSecond + int64(replayDuration%time.Second)*bytesPerSecond/int64(time.Second)
	maxInt := int64(^uint(0) >> 1)
	if maxReplayBytes64 > maxInt {
		return resolvedSTTOptions{}, fmt.Errorf("elevenlabs: replay duration is too large")
	}
	maxReplayBytes := int(maxReplayBytes64)

	return resolvedSTTOptions{
		apiKey: apiKey, baseURL: baseURL, language: language,
		tagAudioEvents: tagAudioEvents, sampleRate: sampleRate,
		serverVAD: cloneVADOptions(options.ServerVAD), includeTimestamps: options.IncludeTimestamps,
		model: model, userKeyterms: cloneStrings(options.Keyterms), noVerbatim: options.NoVerbatim,
		enableLogging: enableLogging, httpClient: httpClient, wsDialer: wsDialer,
		inputCapacity: inputCapacity, outputCapacity: outputCapacity, providerEventCap: eventCapacity,
		maxResponseBytes: maxResponse, maxWSMessageBytes: maxWSMessage,
		maxReplayBytes: maxReplayBytes, readWriteTimeout: readWriteTimeout,
		userAgent: options.UserAgent,
	}, nil
}

func resolveTTSOptions(options TTSOptions) (resolvedTTSOptions, error) {
	apiKey, err := resolveAPIKey(options.APIKey)
	if err != nil {
		return resolvedTTSOptions{}, err
	}
	baseURL, err := resolveBaseURL(options.BaseURL)
	if err != nil {
		return resolvedTTSOptions{}, err
	}
	voiceID := options.VoiceID
	if voiceID == "" && options.Voice != nil {
		voiceID = options.Voice.ID
	}
	if voiceID == "" {
		voiceID = DefaultVoiceID
	}
	voiceSettings := cloneVoiceSettings(options.VoiceSettings)
	if voiceSettings == nil && options.Voice != nil {
		voiceSettings = cloneVoiceSettings(options.Voice.Settings)
	}
	if err := validateVoiceSettings(voiceSettings); err != nil {
		return resolvedTTSOptions{}, err
	}
	model := options.Model
	if model == "" {
		model = options.ModelID
	}
	if model == "" {
		model = DefaultTTSModel
	}
	language := options.Language
	if language == "" {
		language = options.LanguageCode
	}
	language = agents.NormalizeLanguage(string(language))
	encoding := options.Encoding
	if encoding == "" {
		encoding = DefaultEncoding
	}
	sampleRate, err := sampleRateFromEncoding(encoding)
	if err != nil {
		return resolvedTTSOptions{}, err
	}
	if options.StreamingLatency != nil && (*options.StreamingLatency < 0 || *options.StreamingLatency > 4) {
		return resolvedTTSOptions{}, fmt.Errorf("elevenlabs: streaming latency must be between 0 and 4")
	}
	autoMode := len(options.ChunkLengthSchedule) == 0
	if options.AutoMode != nil {
		autoMode = *options.AutoMode
	}
	tokenizer := options.Tokenizer
	if tokenizer == nil {
		tokenizer = options.WordTokenizer
	}
	if tokenizer == nil {
		if autoMode {
			tokenizer = tokenize.NewSentenceTokenizer()
		} else {
			tokenizer = tokenize.NewWordTokenizer(false)
		}
	}
	enableLogging := true
	if options.EnableLogging != nil {
		enableLogging = *options.EnableLogging
	}
	syncAlignment := true
	if options.SyncAlignment != nil {
		syncAlignment = *options.SyncAlignment
	}
	inactivityTimeout := options.InactivityTimeout
	if inactivityTimeout <= 0 {
		inactivityTimeout = defaultWSInactivity
	}
	if inactivityTimeout > maxProviderInactivity {
		return resolvedTTSOptions{}, fmt.Errorf("elevenlabs: inactivity timeout exceeds provider maximum %s", maxProviderInactivity)
	}
	normalization := options.ApplyTextNormalization
	if normalization == "" {
		normalization = TextNormalizationAuto
	}
	if normalization != TextNormalizationAuto && normalization != TextNormalizationOn && normalization != TextNormalizationOff {
		return resolvedTTSOptions{}, fmt.Errorf("elevenlabs: invalid text normalization %q", normalization)
	}
	preferredAlignment := options.PreferredAlignment
	if preferredAlignment == "" {
		preferredAlignment = NormalizedAlignment
	}
	if preferredAlignment != NormalizedAlignment && preferredAlignment != OriginalAlignment {
		return resolvedTTSOptions{}, fmt.Errorf("elevenlabs: invalid preferred alignment %q", preferredAlignment)
	}
	if err := validateDictionaries(options.PronunciationDictionaryLocators); err != nil {
		return resolvedTTSOptions{}, err
	}

	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	wsDialer := options.WebSocketDialer
	if wsDialer == nil {
		wsDialer = websocket.DefaultDialer
	}
	maxContexts := positiveOr(options.MaxActiveContexts, defaultMaxActiveContexts)
	if maxContexts > maxProviderContextCount {
		return resolvedTTSOptions{}, fmt.Errorf("elevenlabs: max active contexts cannot exceed %d", maxProviderContextCount)
	}
	readWriteTimeout := options.ReadWriteTimeout
	if readWriteTimeout <= 0 {
		readWriteTimeout = defaultReadWriteTimeout
	}

	return resolvedTTSOptions{
		generation: 1, apiKey: apiKey, voiceID: voiceID, voiceSettings: voiceSettings, model: model,
		language: language, baseURL: baseURL, encoding: encoding, sampleRate: sampleRate,
		streamingLatency: cloneInt(options.StreamingLatency), tokenizer: tokenizer,
		chunkLengthSchedule: cloneInts(options.ChunkLengthSchedule), enableSSMLParsing: options.EnableSSMLParsing,
		enableLogging: enableLogging, inactivityTimeout: inactivityTimeout, syncAlignment: syncAlignment,
		applyTextNormalization:         normalization,
		applyLanguageTextNormalization: cloneBool(options.ApplyLanguageTextNormalization),
		preferredAlignment:             preferredAlignment, autoMode: autoMode,
		pronunciationDictionaries: cloneDictionaries(options.PronunciationDictionaryLocators),
		httpClient:                httpClient, wsDialer: wsDialer,
		inputCapacity:     positiveOr(options.InputCapacity, defaultStreamCapacity),
		outputCapacity:    positiveOr(options.OutputCapacity, defaultStreamCapacity),
		writerCapacity:    positiveOr(options.WriterCapacity, defaultWriterCapacity),
		providerEventCap:  positiveOr(options.ProviderEventCap, defaultProviderEventCap),
		maxResponseBytes:  positiveInt64Or(options.MaxResponseBytes, defaultMaxResponseBytes),
		maxWSMessageBytes: positiveInt64Or(options.MaxWSMessageBytes, defaultMaxWSMessageBytes),
		readWriteTimeout:  readWriteTimeout, maxActiveContexts: maxContexts,
		userAgent: options.UserAgent,
	}, nil
}

func resolveAPIKey(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if key := os.Getenv("ELEVEN_API_KEY"); key != "" {
		return key, nil
	}
	if key := os.Getenv("ELEVENLABS_API_KEY"); key != "" {
		return key, nil
	}
	return "", &agents.MissingCredentialsError{Name: "ELEVEN_API_KEY"}
}

func resolveBaseURL(raw string) (*url.URL, error) {
	if raw == "" {
		raw = DefaultBaseURL
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("elevenlabs: invalid base URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("elevenlabs: base URL must use http or https")
	}
	if u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("elevenlabs: base URL must contain a host and no query or fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return u, nil
}

func validateVADOptions(options *VADOptions) error {
	if options == nil {
		return nil
	}
	if options.VADSilenceThreshold != nil && *options.VADSilenceThreshold < 0 {
		return fmt.Errorf("elevenlabs: VAD silence threshold cannot be negative")
	}
	if options.VADThreshold != nil && (!finite(*options.VADThreshold) || *options.VADThreshold < 0 || *options.VADThreshold > 1) {
		return fmt.Errorf("elevenlabs: VAD threshold must be between 0 and 1")
	}
	if options.MinSpeechDuration != nil && *options.MinSpeechDuration < 0 {
		return fmt.Errorf("elevenlabs: minimum speech duration cannot be negative")
	}
	if options.MinSilenceDuration != nil && *options.MinSilenceDuration < 0 {
		return fmt.Errorf("elevenlabs: minimum silence duration cannot be negative")
	}
	return nil
}

func validateKeyterms(terms []string, realtime bool) error {
	limit := maxBatchKeyterms
	characterLimit := maxBatchKeytermRunes
	if realtime {
		limit = maxRealtimeKeyterms
		characterLimit = maxRealtimeKeytermRunes
	}
	if len(terms) > limit {
		return fmt.Errorf("elevenlabs: keyterms count %d exceeds limit %d", len(terms), limit)
	}
	for i, term := range terms {
		trimmed := strings.TrimSpace(term)
		if trimmed == "" {
			return fmt.Errorf("elevenlabs: keyterm %d is empty", i)
		}
		if utf8.RuneCountInString(trimmed) > characterLimit {
			return fmt.Errorf("elevenlabs: keyterm %d exceeds %d characters", i, characterLimit)
		}
		if len(strings.Fields(trimmed)) > maxKeytermNormalizedWords {
			return fmt.Errorf("elevenlabs: keyterm %d exceeds %d words", i, maxKeytermNormalizedWords)
		}
		if !realtime && strings.ContainsAny(trimmed, "<>{}[]\\") {
			return fmt.Errorf("elevenlabs: keyterm %d contains a character unsupported by batch Scribe", i)
		}
	}
	return nil
}

func validateVoiceSettings(settings *VoiceSettings) error {
	if settings == nil {
		return nil
	}
	if !finite(settings.Stability) || !finite(settings.SimilarityBoost) || settings.Stability < 0 || settings.Stability > 1 || settings.SimilarityBoost < 0 || settings.SimilarityBoost > 1 {
		return fmt.Errorf("elevenlabs: stability and similarity boost must be between 0 and 1")
	}
	if settings.Style != nil && (!finite(*settings.Style) || *settings.Style < 0 || *settings.Style > 1) {
		return fmt.Errorf("elevenlabs: style must be between 0 and 1")
	}
	if settings.Speed != nil && (!finite(*settings.Speed) || *settings.Speed < 0.8 || *settings.Speed > 1.2) {
		return fmt.Errorf("elevenlabs: speed must be between 0.8 and 1.2")
	}
	return nil
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func validateDictionaries(locators []PronunciationDictionaryLocator) error {
	for i, locator := range locators {
		if locator.PronunciationDictionaryID == "" || locator.VersionID == "" {
			return fmt.Errorf("elevenlabs: pronunciation dictionary %d requires id and version", i)
		}
	}
	return nil
}

func cloneVADOptions(value *VADOptions) *VADOptions {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneVoiceSettings(value *VoiceSettings) *VoiceSettings {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Style = cloneFloat(value.Style)
	clone.Speed = cloneFloat(value.Speed)
	clone.UseSpeakerBoost = cloneBool(value.UseSpeakerBoost)
	return &clone
}

func cloneDictionaries(value []PronunciationDictionaryLocator) []PronunciationDictionaryLocator {
	return append([]PronunciationDictionaryLocator(nil), value...)
}

func cloneStrings(value []string) []string { return append([]string(nil), value...) }
func cloneInts(value []int) []int          { return append([]int(nil), value...) }
func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
func positiveOr(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
func positiveInt64Or(value, fallback int64) int64 {
	if value > 0 {
		return value
	}
	return fallback
}
