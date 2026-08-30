// SPDX-License-Identifier: Apache-2.0

// Package transcription contains the legacy public text/audio synchronizer.
// The implementation is event-driven and bounded: it creates one worker, has
// no idle ticker, and applies backpressure through context-aware methods.
package transcription

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/tokenize"
	livekit "github.com/livekit/protocol/livekit"
)

const (
	StandardSpeechRate          = 3.83 // syllables per second
	DefaultSegmentQueueCapacity = 32
)

var ErrClosed = errors.New("transcription: TextAudioSynchronizer is closed")

type TextSyncOptions struct {
	Language             string
	Speed                float64
	NewSentenceDelay     time.Duration
	SentenceTokenizer    *tokenize.SentenceTokenizer
	HyphenateWord        func(string) []string
	SplitWords           func(string) []tokenize.Span
	SegmentQueueCapacity int
}

func DefaultTextSyncOptions() TextSyncOptions {
	return TextSyncOptions{
		Speed: 1, NewSentenceDelay: 400 * time.Millisecond,
		SentenceTokenizer:    tokenize.NewSentenceTokenizer(),
		HyphenateWord:        tokenize.HyphenateWord,
		SplitWords:           func(text string) []tokenize.Span { return tokenize.SplitWords(text, false) },
		SegmentQueueCapacity: DefaultSegmentQueueCapacity,
	}
}

func resolveTextSyncOptions(options TextSyncOptions) (TextSyncOptions, error) {
	defaults := DefaultTextSyncOptions()
	if options.Speed == 0 {
		options.Speed = defaults.Speed
	}
	if options.NewSentenceDelay == 0 {
		options.NewSentenceDelay = defaults.NewSentenceDelay
	}
	if options.SentenceTokenizer == nil {
		options.SentenceTokenizer = defaults.SentenceTokenizer
	}
	if options.HyphenateWord == nil {
		options.HyphenateWord = defaults.HyphenateWord
	}
	if options.SplitWords == nil {
		options.SplitWords = defaults.SplitWords
	}
	if options.SegmentQueueCapacity == 0 {
		options.SegmentQueueCapacity = defaults.SegmentQueueCapacity
	}
	if options.Speed <= 0 {
		return TextSyncOptions{}, errors.New("transcription: speed must be positive")
	}
	if options.NewSentenceDelay < 0 {
		return TextSyncOptions{}, errors.New("transcription: new-sentence delay must not be negative")
	}
	if options.SegmentQueueCapacity < 1 {
		return TextSyncOptions{}, errors.New("transcription: segment queue capacity must be positive")
	}
	return options, nil
}

type audioSegment struct {
	duration atomic.Int64
	done     atomic.Bool
}

func (s *audioSegment) add(duration time.Duration) { s.duration.Add(int64(duration)) }
func (s *audioSegment) pushedDuration() time.Duration {
	return time.Duration(s.duration.Load())
}

type textSegment struct {
	stream *tokenize.SentenceStream

	mu                 sync.RWMutex
	pushedText         string
	done               bool
	forwardedHyphens   int
	forwardedSentences int
}

func (s *textSegment) appendText(text string) {
	s.mu.Lock()
	s.pushedText += text
	s.mu.Unlock()
}

func (s *textSegment) snapshot() (string, bool) {
	s.mu.RLock()
	text, done := s.pushedText, s.done
	s.mu.RUnlock()
	return text, done
}

func (s *textSegment) markDone() {
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()
}

// TextAudioSynchronizer paces partial transcript segments against actual audio
// duration and playout state. Push/mark methods are serialized and safe to call
// concurrently with the output callback.
type TextAudioSynchronizer struct {
	options TextSyncOptions
	speed   float64

	ctx    context.Context
	cancel context.CancelCauseFunc
	textQ  *stream.Channel[*textSegment]
	audioQ *stream.Channel[*audioSegment]

	opMu         sync.Mutex
	currentText  *textSegment
	currentAudio *audioSegment

	stateMu       sync.RWMutex
	playingIndex  int
	finishedIndex int
	playedText    string
	interrupted   bool
	stateChanged  chan struct{}

	updates   agents.EventEmitter[*livekit.TranscriptionSegment]
	closed    atomic.Bool
	closeOnce sync.Once
	done      chan struct{}
}

func NewTextAudioSynchronizer(
	parent context.Context,
	options TextSyncOptions,
) (*TextAudioSynchronizer, error) {
	resolved, err := resolveTextSyncOptions(options)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	synchronizer := &TextAudioSynchronizer{
		options: resolved, speed: resolved.Speed * StandardSpeechRate,
		ctx: ctx, cancel: cancel,
		textQ:        stream.NewChannel[*textSegment](resolved.SegmentQueueCapacity),
		audioQ:       stream.NewChannel[*audioSegment](resolved.SegmentQueueCapacity),
		playingIndex: -1, finishedIndex: -1,
		stateChanged: make(chan struct{}, 1), done: make(chan struct{}),
	}
	go synchronizer.run()
	return synchronizer, nil
}

func (s *TextAudioSynchronizer) OnTextUpdated(
	callback func(*livekit.TranscriptionSegment),
) func() {
	return s.updates.Subscribe(callback)
}

func (s *TextAudioSynchronizer) PushAudio(ctx context.Context, frame agents.AudioFrame) error {
	if frame.SampleRate <= 0 || frame.Channels <= 0 {
		return errors.New("transcription: audio frame sample rate and channels must be positive")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	if s.currentAudio == nil {
		segment := &audioSegment{}
		if err := s.audioQ.Send(ctx, segment); err != nil {
			return err
		}
		s.currentAudio = segment
	}
	s.currentAudio.add(frame.Duration())
	s.signalState()
	return nil
}

func (s *TextAudioSynchronizer) PushText(ctx context.Context, text string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	if s.currentText == nil {
		segment := &textSegment{stream: s.options.SentenceTokenizer.Stream()}
		if err := s.textQ.Send(ctx, segment); err != nil {
			_ = segment.stream.Close()
			return err
		}
		s.currentText = segment
	}
	if text == "" {
		return nil
	}
	if err := s.currentText.stream.PushText(ctx, text); err != nil {
		return err
	}
	s.currentText.appendText(text)
	return nil
}

func (s *TextAudioSynchronizer) MarkAudioSegmentEnd(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	if s.currentAudio == nil {
		segment := &audioSegment{}
		if err := s.audioQ.Send(ctx, segment); err != nil {
			return err
		}
		s.currentAudio = segment
	}
	s.currentAudio.done.Store(true)
	s.currentAudio = nil
	s.signalState()
	return nil
}

func (s *TextAudioSynchronizer) MarkTextSegmentEnd(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	if s.currentText == nil {
		segment := &textSegment{stream: s.options.SentenceTokenizer.Stream()}
		if err := s.textQ.Send(ctx, segment); err != nil {
			_ = segment.stream.Close()
			return err
		}
		s.currentText = segment
	}
	segment := s.currentText
	segment.markDone()
	s.currentText = nil
	if err := segment.stream.EndInput(ctx); err != nil {
		return err
	}
	return nil
}

func (s *TextAudioSynchronizer) SegmentPlayoutStarted() error {
	if s.closed.Load() {
		return ErrClosed
	}
	s.stateMu.Lock()
	s.playingIndex++
	s.stateMu.Unlock()
	s.signalState()
	return nil
}

func (s *TextAudioSynchronizer) SegmentPlayoutFinished() error {
	if s.closed.Load() {
		return ErrClosed
	}
	s.stateMu.Lock()
	s.finishedIndex++
	s.stateMu.Unlock()
	s.signalState()
	return nil
}

func (s *TextAudioSynchronizer) PlayedText() string {
	s.stateMu.RLock()
	text := s.playedText
	s.stateMu.RUnlock()
	return text
}

func (s *TextAudioSynchronizer) Closed() bool { return s.closed.Load() }

// Close is idempotent. A non-interrupted close drains already-paired text with
// delays removed; an interrupted close suppresses remaining partial/final text.
func (s *TextAudioSynchronizer) Close(ctx context.Context, interrupt bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.stateMu.Lock()
		s.interrupted = interrupt
		s.stateMu.Unlock()
		s.opMu.Lock()
		if s.currentText != nil {
			s.currentText.markDone()
			_ = s.currentText.stream.EndInput(context.Background())
			s.currentText = nil
		}
		if s.currentAudio != nil {
			s.currentAudio.done.Store(true)
			s.currentAudio = nil
		}
		_ = s.textQ.Close()
		_ = s.audioQ.Close()
		s.opMu.Unlock()
		s.signalState()
	})
	select {
	case <-s.done:
		s.cancel(ErrClosed)
		return nil
	case <-ctx.Done():
		s.cancel(context.Cause(ctx))
		return context.Cause(ctx)
	}
}

func (s *TextAudioSynchronizer) run() {
	defer close(s.done)
	for index := 0; ; index++ {
		textSegment, err := s.textQ.Recv(s.ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, stream.ErrClosed) {
				return
			}
			return
		}
		audioSegment, err := s.audioQ.Recv(s.ctx)
		if err != nil {
			return
		}
		if textSegment == nil || audioSegment == nil {
			return
		}
		if err := s.waitForPlayout(index); err != nil {
			return
		}
		segmentStart := time.Now()
		for {
			token, err := textSegment.stream.Recv(s.ctx)
			if errors.Is(err, io.EOF) || errors.Is(err, stream.ErrClosed) {
				break
			}
			if err != nil {
				return
			}
			if err := s.syncSentence(index, segmentStart, textSegment, audioSegment, token.Token); err != nil {
				if errors.Is(err, errInterrupted) {
					break
				}
				return
			}
		}
	}
}

var errInterrupted = errors.New("transcription: interrupted")

func (s *TextAudioSynchronizer) waitForPlayout(index int) error {
	for {
		s.stateMu.RLock()
		ready := s.playingIndex >= index || s.closed.Load()
		interrupted := s.interrupted
		s.stateMu.RUnlock()
		if interrupted {
			return errInterrupted
		}
		if ready {
			return nil
		}
		select {
		case <-s.stateChanged:
		case <-s.ctx.Done():
			return context.Cause(s.ctx)
		}
	}
}

func (s *TextAudioSynchronizer) syncSentence(
	segmentIndex int,
	segmentStart time.Time,
	textData *textSegment,
	audioData *audioSegment,
	sentence string,
) error {
	pushedText, textDone := textData.snapshot()
	var realSpeed float64
	audioDuration := audioData.pushedDuration()
	if audioDuration > 0 && audioData.done.Load() && textDone {
		hyphens := s.calculateHyphens(pushedText)
		realSpeed = float64(len(hyphens)) / audioDuration.Seconds()
	}
	segmentID := agents.ShortUUID("SG_")
	words := s.options.SplitWords(sentence)
	s.stateMu.RLock()
	originalPlayedText := s.playedText
	s.stateMu.RUnlock()
	for _, word := range words {
		if s.segmentFinished(segmentIndex) {
			break
		}
		if s.isInterrupted() {
			return errInterrupted
		}
		wordHyphens := len(s.options.HyphenateWord(word.Text))
		end := min(max(word.End, 0), len(sentence))
		text := strings.TrimRight(sentence[:end], ".,!?;:-—")
		speed := s.speed
		var delay time.Duration
		if realSpeed > 0 {
			speed = realSpeed
			estimatedPauses := time.Duration(textData.forwardedSentences) * s.options.NewSentenceDelay
			pauseHyphens := estimatedPauses.Seconds() * speed
			targetHyphens := speed * time.Since(segmentStart).Seconds()
			delta := targetHyphens - float64(textData.forwardedHyphens) - pauseHyphens
			toWait := max(0, float64(wordHyphens)-delta)
			delay = time.Duration(toWait / speed * float64(time.Second))
		} else {
			delay = time.Duration(float64(wordHyphens) / speed * float64(time.Second))
		}
		firstDelay := min(delay/2, time.Duration(2/speed*float64(time.Second)))
		if err := s.waitDelay(firstDelay); err != nil {
			return err
		}
		s.updates.Emit(&livekit.TranscriptionSegment{
			Id: segmentID, Text: text, Final: false, Language: s.options.Language,
		})
		s.stateMu.Lock()
		s.playedText = originalPlayedText + " " + text
		s.stateMu.Unlock()
		if err := s.waitDelay(delay - firstDelay); err != nil {
			return err
		}
		textData.forwardedHyphens += wordHyphens
	}
	if s.isInterrupted() {
		return errInterrupted
	}
	s.updates.Emit(&livekit.TranscriptionSegment{
		Id: segmentID, Text: sentence, Final: true, Language: s.options.Language,
	})
	s.stateMu.Lock()
	s.playedText = originalPlayedText + " " + sentence
	s.stateMu.Unlock()
	if err := s.waitDelay(s.options.NewSentenceDelay); err != nil {
		return err
	}
	textData.forwardedSentences++
	return nil
}

func (s *TextAudioSynchronizer) waitDelay(delay time.Duration) error {
	if delay <= 0 || s.closed.Load() {
		if s.isInterrupted() {
			return errInterrupted
		}
		return nil
	}
	deadline := time.Now().Add(delay)
	for {
		if s.closed.Load() {
			if s.isInterrupted() {
				return errInterrupted
			}
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		timer := time.NewTimer(remaining)
		select {
		case <-timer.C:
			return nil
		case <-s.stateChanged:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if s.isInterrupted() {
				return errInterrupted
			}
		case <-s.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return context.Cause(s.ctx)
		}
	}
}

func (s *TextAudioSynchronizer) calculateHyphens(text string) []string {
	words := s.options.SplitWords(text)
	result := make([]string, 0, len(words)*2)
	for _, word := range words {
		result = append(result, s.options.HyphenateWord(word.Text)...)
	}
	return result
}

func (s *TextAudioSynchronizer) segmentFinished(index int) bool {
	s.stateMu.RLock()
	finished := index <= s.finishedIndex
	s.stateMu.RUnlock()
	return finished
}

func (s *TextAudioSynchronizer) isInterrupted() bool {
	s.stateMu.RLock()
	interrupted := s.interrupted
	s.stateMu.RUnlock()
	return interrupted
}

func (s *TextAudioSynchronizer) signalState() {
	select {
	case s.stateChanged <- struct{}{}:
	default:
	}
}

func (s *TextAudioSynchronizer) String() string {
	return fmt.Sprintf("TextAudioSynchronizer(language=%q, speed=%g)", s.options.Language, s.options.Speed)
}
