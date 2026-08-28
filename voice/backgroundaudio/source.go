// SPDX-License-Identifier: Apache-2.0

package backgroundaudio

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/livekit/agents-go"
)

const (
	// The pinned agents-js 1.7.1 built-in clip names are part of the public API.
	BuiltinHoldMusic       BuiltinAudioClip = "hold_music.ogg"
	BuiltinOfficeAmbience  BuiltinAudioClip = "office-ambience.ogg"
	BuiltinKeyboardTyping  BuiltinAudioClip = "keyboard-typing.ogg"
	BuiltinKeyboardTyping2 BuiltinAudioClip = "keyboard-typing2.ogg"

	// TypeScript-compatible aliases.
	HOLD_MUSIC       = BuiltinHoldMusic
	OFFICE_AMBIENCE  = BuiltinOfficeAmbience
	KEYBOARD_TYPING  = BuiltinKeyboardTyping
	KEYBOARD_TYPING2 = BuiltinKeyboardTyping2
)

var (
	ErrInvalidSource  = errors.New("backgroundaudio: invalid audio source")
	ErrSourceConsumed = errors.New("backgroundaudio: one-shot stream source was already opened")
)

// BuiltinAudioClip identifies a package-provided Ogg/Vorbis clip.
type BuiltinAudioClip string

func IsBuiltinAudioClip(clip BuiltinAudioClip) bool {
	switch clip {
	case BuiltinHoldMusic, BuiltinOfficeAmbience, BuiltinKeyboardTyping, BuiltinKeyboardTyping2:
		return true
	default:
		return false
	}
}

//go:embed resources/*.ogg
var embeddedAudio embed.FS

// BuiltinResources exposes the embedded, read-only files without extraction.
// Paths are of the form "resources/office-ambience.ogg".
func BuiltinResources() fs.FS { return embeddedAudio }

// OpenBuiltinAudio opens an embedded clip. The caller must close the file.
func OpenBuiltinAudio(clip BuiltinAudioClip) (fs.File, error) {
	if !IsBuiltinAudioClip(clip) {
		return nil, fmt.Errorf("%w: unknown built-in clip %q", ErrInvalidSource, clip)
	}
	return embeddedAudio.Open("resources/" + string(clip))
}

// ExtractBuiltinAudio writes one clip to a private temporary directory. This
// is useful for decoders that require a path. Cleanup is idempotent and should
// be deferred by the caller. BackgroundAudioPlayer performs the same extraction
// lazily and removes every extracted file from Close.
func ExtractBuiltinAudio(ctx context.Context, clip BuiltinAudioClip) (path string, cleanup func() error, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return "", nil, err
	}
	if !IsBuiltinAudioClip(clip) {
		return "", nil, fmt.Errorf("%w: unknown built-in clip %q", ErrInvalidSource, clip)
	}
	directory, err := os.MkdirTemp("", "livekit-agents-backgroundaudio-")
	if err != nil {
		return "", nil, err
	}
	var once sync.Once
	cleanup = func() (removeErr error) {
		once.Do(func() { removeErr = os.RemoveAll(directory) })
		return removeErr
	}
	path = filepath.Join(directory, filepath.Base(string(clip)))
	if err := extractEmbedded(ctx, clip, path); err != nil {
		_ = cleanup()
		return "", nil, err
	}
	return path, cleanup, nil
}

// GetBuiltinAudioPath is the agents-js-compatible name for
// ExtractBuiltinAudio. Unlike JavaScript package resources, Go's embedded files
// do not inherently have an OS path, so the returned cleanup is explicit.
func GetBuiltinAudioPath(ctx context.Context, clip BuiltinAudioClip) (path string, cleanup func() error, err error) {
	return ExtractBuiltinAudio(ctx, clip)
}

func extractEmbedded(ctx context.Context, clip BuiltinAudioClip, path string) error {
	source, err := OpenBuiltinAudio(clip)
	if err != nil {
		return err
	}
	defer source.Close()
	destination, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	buffer := make([]byte, 32<<10)
	for {
		if err := context.Cause(ctx); err != nil {
			_ = destination.Close()
			return err
		}
		n, readErr := source.Read(buffer)
		if n != 0 {
			if _, err := destination.Write(buffer[:n]); err != nil {
				_ = destination.Close()
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return destination.Close()
			}
			_ = destination.Close()
			return readErr
		}
	}
}

// BuiltinResolver may override where built-in clips are obtained. Resolve must
// honor ctx. The returned cleanup is called when the player closes; it may be
// nil. It can replace temporary extraction with an application-managed path or
// asset store; the embedded fallback remains part of the package binary.
type BuiltinResolver func(context.Context, BuiltinAudioClip) (path string, cleanup func() error, err error)

type sourceKind uint8

const (
	sourceInvalid sourceKind = iota
	sourceFile
	sourceBuiltin
	sourceReader
	sourceFactory
)

// FrameReader is the context-aware async audio source contract. Recv must
// unblock when ctx is canceled. Implementations return io.EOF at normal end.
type FrameReader interface {
	Recv(context.Context) (agents.AudioFrame, error)
}

// FrameStreamFactory constructs a fresh async stream for a play request.
type FrameStreamFactory func(context.Context) (FrameReader, error)

// AudioSource is the Go representation of agents-js's string | BuiltinAudioClip
// | AsyncIterable<AudioFrame> union. Use File, Builtin, Stream, OwnedStream, or
// StreamFactorySource to construct one.
type AudioSource struct {
	kind    sourceKind
	path    string
	clip    BuiltinAudioClip
	reader  *oneShotReader
	factory FrameStreamFactory
}

// AudioSourceType is the agents-js compatibility name.
type AudioSourceType = AudioSource

type oneShotReader struct {
	value FrameReader
	owned bool
	used  atomic.Bool
}

func File(path string) AudioSource {
	return AudioSource{kind: sourceFile, path: path}
}

func Builtin(clip BuiltinAudioClip) AudioSource {
	return AudioSource{kind: sourceBuiltin, clip: clip}
}

// Stream wraps a caller-owned, one-shot async stream. The player never closes it.
func Stream(reader FrameReader) AudioSource {
	return AudioSource{kind: sourceReader, reader: &oneShotReader{value: reader}}
}

// OwnedStream wraps a one-shot async stream that the player closes when it
// implements io.Closer.
func OwnedStream(reader FrameReader) AudioSource {
	return AudioSource{kind: sourceReader, reader: &oneShotReader{value: reader, owned: true}}
}

// StreamFactorySource creates a fresh async stream for every play request.
// Like AsyncIterable sources in agents-js, factory streams are not automatically
// looped; return an infinite stream when looping is desired.
func StreamFactorySource(factory FrameStreamFactory) AudioSource {
	return AudioSource{kind: sourceFactory, factory: factory}
}

func (s AudioSource) Valid() bool {
	switch s.kind {
	case sourceFile:
		return strings.TrimSpace(s.path) != ""
	case sourceBuiltin:
		return IsBuiltinAudioClip(s.clip)
	case sourceReader:
		return s.reader != nil && !isNilInterface(s.reader.value)
	case sourceFactory:
		return s.factory != nil
	default:
		return false
	}
}

func (s AudioSource) fileLike() bool { return s.kind == sourceFile || s.kind == sourceBuiltin }

type openedSource struct {
	reader FrameReader
	close  func() error
}

type sourceOpenOptions struct {
	loop           bool
	blockDuration  time.Duration
	bufferDuration time.Duration
	ffmpegPath     string
	resolveBuiltin func(context.Context, BuiltinAudioClip) (string, error)
}

func (s AudioSource) open(ctx context.Context, options sourceOpenOptions) (openedSource, error) {
	if !s.Valid() {
		return openedSource{}, ErrInvalidSource
	}
	switch s.kind {
	case sourceFile, sourceBuiltin:
		path := s.path
		if s.kind == sourceBuiltin {
			if options.resolveBuiltin == nil {
				return openedSource{}, errors.New("backgroundaudio: built-in resolver is required")
			}
			var err error
			path, err = options.resolveBuiltin(ctx, s.clip)
			if err != nil {
				return openedSource{}, err
			}
		}
		decode := agents.AudioDecodeOptions{
			SampleRate: 48_000, Channels: 1, FrameDuration: options.blockDuration,
			// The player's 400 ms ingress queue owns decoded buffering. Keep the
			// decoder handoff at one block to avoid stacking two full queues.
			StreamCapacity: 1, FFmpegPath: options.ffmpegPath,
		}
		var value agents.AudioFrameStream
		var err error
		if options.loop {
			value, err = agents.LoopAudioFramesFromFile(ctx, path, decode)
		} else {
			value, err = agents.AudioFramesFromFile(ctx, path, decode)
		}
		if err != nil {
			return openedSource{}, err
		}
		return openedSource{reader: value, close: value.Close}, nil
	case sourceReader:
		if !s.reader.used.CompareAndSwap(false, true) {
			return openedSource{}, ErrSourceConsumed
		}
		var closeFn func() error
		if s.reader.owned {
			if closer, ok := s.reader.value.(io.Closer); ok {
				closeFn = closer.Close
			}
		}
		return openedSource{reader: s.reader.value, close: closeFn}, nil
	case sourceFactory:
		value, err := s.factory(ctx)
		if err != nil {
			return openedSource{}, err
		}
		if isNilInterface(value) {
			return openedSource{}, ErrInvalidSource
		}
		var closeFn func() error
		if closer, ok := value.(io.Closer); ok {
			closeFn = closer.Close
		}
		return openedSource{reader: value, close: closeFn}, nil
	default:
		return openedSource{}, ErrInvalidSource
	}
}

// AudioConfig configures one selectable source. Nil Volume and Probability
// fields have the TypeScript defaults of 1.0. Pointers preserve the distinction
// between an omitted value and an explicit zero.
type AudioConfig struct {
	Source      AudioSource
	Volume      *float64
	Probability *float64
}

func Config(source AudioSource) AudioConfig { return AudioConfig{Source: source} }

func Float64(value float64) *float64 { return &value }

func (c AudioConfig) WithVolume(value float64) AudioConfig {
	c.Volume = Float64(value)
	return c
}

func (c AudioConfig) WithProbability(value float64) AudioConfig {
	c.Probability = Float64(value)
	return c
}

func (c AudioConfig) resolved() (resolvedConfig, error) {
	if !c.Source.Valid() {
		return resolvedConfig{}, ErrInvalidSource
	}
	volume, probability := 1.0, 1.0
	if c.Volume != nil {
		volume = *c.Volume
	}
	if c.Probability != nil {
		probability = *c.Probability
	}
	if math.IsNaN(volume) || math.IsInf(volume, 0) || volume < 0 {
		return resolvedConfig{}, fmt.Errorf("backgroundaudio: volume must be a finite non-negative number: %v", volume)
	}
	if math.IsNaN(probability) || math.IsInf(probability, 0) {
		return resolvedConfig{}, fmt.Errorf("backgroundaudio: probability must be finite: %v", probability)
	}
	return resolvedConfig{source: c.Source, volume: volume, probability: probability}, nil
}

type resolvedConfig struct {
	source      AudioSource
	volume      float64
	probability float64
}

// Sound is either one configured source or a probability-weighted list. Its
// zero value means no sound, which is useful for optional player settings.
type Sound struct {
	Configs []AudioConfig
	// Weighted applies agents-js list selection semantics. Choose sets it.
	// Multiple Configs are also treated as weighted for struct-literal callers.
	Weighted bool
}

func SourceSound(source AudioSource) Sound     { return Sound{Configs: []AudioConfig{Config(source)}} }
func ConfiguredSound(config AudioConfig) Sound { return Sound{Configs: []AudioConfig{config}} }
func Choose(configs ...AudioConfig) Sound {
	return Sound{Configs: append([]AudioConfig(nil), configs...), Weighted: true}
}
func (s Sound) Empty() bool { return len(s.Configs) == 0 }

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}

type builtinStore struct {
	resolver BuiltinResolver
	mu       sync.Mutex
	dir      string
	paths    map[BuiltinAudioClip]string
	cleanups []func() error
	closed   bool
}

func (s *builtinStore) resolve(ctx context.Context, clip BuiltinAudioClip) (string, error) {
	if !IsBuiltinAudioClip(clip) {
		return "", fmt.Errorf("%w: unknown built-in clip %q", ErrInvalidSource, clip)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return "", ErrClosed
	}
	if path := s.paths[clip]; path != "" {
		s.mu.Unlock()
		return path, nil
	}
	resolver := s.resolver
	s.mu.Unlock()
	if resolver != nil {
		path, cleanup, err := resolver(ctx, clip)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(path) == "" {
			if cleanup != nil {
				_ = cleanup()
			}
			return "", ErrInvalidSource
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			if cleanup != nil {
				_ = cleanup()
			}
			return "", ErrClosed
		}
		if existing := s.paths[clip]; existing != "" {
			s.mu.Unlock()
			if cleanup != nil {
				_ = cleanup()
			}
			return existing, nil
		}
		if s.paths == nil {
			s.paths = make(map[BuiltinAudioClip]string)
		}
		s.paths[clip] = path
		if cleanup != nil {
			s.cleanups = append(s.cleanups, cleanup)
		}
		s.mu.Unlock()
		return path, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", ErrClosed
	}
	if path := s.paths[clip]; path != "" {
		return path, nil
	}
	if s.dir == "" {
		directory, err := os.MkdirTemp("", "livekit-agents-backgroundaudio-")
		if err != nil {
			return "", err
		}
		s.dir = directory
	}
	path := filepath.Join(s.dir, filepath.Base(string(clip)))
	if err := extractEmbedded(ctx, clip, path); err != nil {
		return "", err
	}
	if s.paths == nil {
		s.paths = make(map[BuiltinAudioClip]string)
	}
	s.paths[clip] = path
	return path, nil
}

func (s *builtinStore) close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	directory := s.dir
	cleanups := append([]func() error(nil), s.cleanups...)
	s.dir = ""
	s.paths = nil
	s.cleanups = nil
	s.mu.Unlock()
	var errs []error
	for i := len(cleanups) - 1; i >= 0; i-- {
		if err := cleanups[i](); err != nil {
			errs = append(errs, err)
		}
	}
	if directory != "" {
		if err := os.RemoveAll(directory); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
