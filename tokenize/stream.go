// SPDX-License-Identifier: Apache-2.0

package tokenize

import (
	"context"
	"errors"
	"io"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stream"
)

// DefaultMaxBufferedBytes bounds text retained by an incremental tokenizer
// when hostile or malformed input never produces a complete token.
const DefaultMaxBufferedBytes = 1 << 20

// ErrBufferLimit means PushText rejected a chunk without consuming it because
// retaining it would exceed MaxBufferedBytes.
var ErrBufferLimit = errors.New("tokenize: buffered text limit exceeded")

// StreamOptions controls incremental buffering. Lengths are Unicode code-point
// counts; source Span offsets remain UTF-8 byte offsets.
type StreamOptions struct {
	MinTokenLength   int
	MinContextLength int
	MaxTokenLength   int
	FirstTokenLength int
	XMLAware         bool
	OutputCapacity   int
	// MaxBufferedBytes caps in-progress input plus a batched output token. Zero
	// selects DefaultMaxBufferedBytes.
	MaxBufferedBytes int
}

// TokenStream incrementally applies a tokenizer with bounded output buffering.
// PushText and Flush may block on downstream backpressure and therefore accept
// a context. One sender and one receiver may operate concurrently.
type TokenStream struct {
	fn               func(string) []Span
	minTokenLength   int
	minContextLength int
	maxTokenLength   int
	firstTokenLength int
	xmlAware         bool
	maxBufferedBytes int

	opMu       sync.Mutex
	inBuf      string
	outBuf     string
	segmentID  string
	emitted    int
	pending    []TokenData
	pendingPos int

	output    *stream.Channel[TokenData]
	closed    atomic.Bool
	closeOnce sync.Once
}

// BufferedTokenStream is the cross-language compatibility name.
type BufferedTokenStream = TokenStream

// SentenceStream is a TokenStream produced by SentenceTokenizer.
type SentenceStream struct{ *TokenStream }

// WordStream is a TokenStream produced by WordTokenizer.
type WordStream struct{ *TokenStream }

// NewBufferedTokenStream builds a stream around a custom span tokenizer.
func NewBufferedTokenStream(fn func(string) []Span, opts StreamOptions) *TokenStream {
	return newTokenStream(fn, opts)
}

func newTokenStream(fn func(string) []Span, opts StreamOptions) *TokenStream {
	if fn == nil {
		panic("tokenize: nil tokenizer")
	}
	if opts.MinTokenLength <= 0 {
		opts.MinTokenLength = 1
	}
	if opts.MinContextLength <= 0 {
		opts.MinContextLength = 1
	}
	if opts.OutputCapacity <= 0 {
		opts.OutputCapacity = 32
	}
	if opts.MaxBufferedBytes <= 0 {
		opts.MaxBufferedBytes = DefaultMaxBufferedBytes
	}
	if opts.XMLAware {
		fn = wrapXMLTokenizer(fn)
	}
	return &TokenStream{
		fn:               fn,
		minTokenLength:   opts.MinTokenLength,
		minContextLength: opts.MinContextLength,
		maxTokenLength:   opts.MaxTokenLength,
		firstTokenLength: opts.FirstTokenLength,
		xmlAware:         opts.XMLAware,
		maxBufferedBytes: opts.MaxBufferedBytes,
		segmentID:        agents.ShortUUID(""),
		output:           stream.NewChannel[TokenData](opts.OutputCapacity),
	}
}

// PushText adds text and emits every token that is provably complete.
func (s *TokenStream) PushText(ctx context.Context, text string) error {
	if text == "" {
		return s.closedError()
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.closed.Load() {
		return stream.ErrClosed
	}
	if err := s.sendPending(ctx); err != nil {
		return err
	}
	buffered := len(s.inBuf) + len(s.outBuf)
	if len(text) > s.maxBufferedBytes-buffered {
		return ErrBufferLimit
	}
	s.inBuf += text
	if utf8.RuneCountInString(s.inBuf) < s.minContextLength {
		return nil
	}

	for {
		tokens := s.fn(s.inBuf)
		if len(tokens) <= 1 {
			break
		}
		token := tokens[0]
		if s.xmlAware && HasUnclosedXMLTags(token.Text) {
			break
		}
		s.appendToken(token.Text)
		if s.tokenLength() >= s.emitThreshold() {
			s.emit()
		}
		if err := s.sendPending(ctx); err != nil {
			return err
		}
		if token.End <= 0 || token.End > len(s.inBuf) {
			break
		}
		s.inBuf = s.inBuf[token.End:]
	}
	return s.sendPending(ctx)
}

// Flush emits all buffered text and starts a new segment.
func (s *TokenStream) Flush(ctx context.Context) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.closed.Load() {
		return stream.ErrClosed
	}
	if err := s.sendPending(ctx); err != nil {
		return err
	}
	hadContent := s.inBuf != "" || s.outBuf != ""
	if hadContent {
		for _, token := range s.fn(s.inBuf) {
			s.appendToken(token.Text)
			if err := s.sendPending(ctx); err != nil {
				return err
			}
		}
		if s.outBuf != "" {
			s.emit()
		}
		s.segmentID = agents.ShortUUID("")
		s.emitted = 0
	}
	s.inBuf = ""
	s.outBuf = ""
	return s.sendPending(ctx)
}

// EndInput flushes pending text and closes the receive side with io.EOF.
func (s *TokenStream) EndInput(ctx context.Context) error {
	if err := s.Flush(ctx); err != nil {
		return err
	}
	return s.Close()
}

// Recv receives the next token. io.EOF is returned after a clean close.
func (s *TokenStream) Recv(ctx context.Context) (TokenData, error) {
	return s.output.Recv(ctx)
}

// Range adapts Recv to Go's range-over-function iteration.
func (s *TokenStream) Range(ctx context.Context) iter.Seq2[TokenData, error] {
	return func(yield func(TokenData, error) bool) {
		for {
			token, err := s.Recv(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					yield(TokenData{}, err)
				}
				return
			}
			if !yield(token, nil) {
				return
			}
		}
	}
}

// Closed reports whether Close or Abort has run.
func (s *TokenStream) Closed() bool { return s.closed.Load() }

// Close closes the stream without flushing, matching the JS/Python close
// contract. Call EndInput to retain pending text.
func (s *TokenStream) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		_ = s.output.Close()
		s.opMu.Lock()
		s.pending = nil
		s.inBuf = ""
		s.outBuf = ""
		s.opMu.Unlock()
	})
	return nil
}

// Abort closes the stream with a terminal receive error.
func (s *TokenStream) Abort(err error) error {
	if err == nil {
		err = stream.ErrClosed
	}
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		_ = s.output.Abort(err)
		s.opMu.Lock()
		s.pending = nil
		s.inBuf = ""
		s.outBuf = ""
		s.opMu.Unlock()
	})
	return nil
}

func (s *TokenStream) closedError() error {
	if s.closed.Load() {
		return stream.ErrClosed
	}
	return nil
}

func (s *TokenStream) emitThreshold() int {
	if s.emitted == 0 && s.firstTokenLength > 0 {
		return s.firstTokenLength
	}
	return s.minTokenLength
}

func (s *TokenStream) tokenLength() int { return utf8.RuneCountInString(s.outBuf) }

func (s *TokenStream) appendToken(text string) {
	if s.maxTokenLength > 0 && s.outBuf != "" &&
		s.tokenLength()+1+utf8.RuneCountInString(text) > s.maxTokenLength {
		s.emit()
	}
	if s.outBuf != "" {
		s.outBuf += " "
	}
	s.outBuf += text
}

func (s *TokenStream) emit() {
	if s.outBuf == "" {
		return
	}
	s.pending = append(s.pending, TokenData{SegmentID: s.segmentID, Token: s.outBuf})
	s.outBuf = ""
	s.emitted++
}

func (s *TokenStream) sendPending(ctx context.Context) error {
	for s.pendingPos < len(s.pending) {
		if err := s.output.Send(ctx, s.pending[s.pendingPos]); err != nil {
			return err
		}
		s.pendingPos++
	}
	if s.pendingPos != 0 {
		s.pending = nil
		s.pendingPos = 0
	}
	return nil
}

// JoinTokens is a small convenience for tests and adapters that need to drain a
// finite stream while preserving the tokenizer's inter-token spacing.
func JoinTokens(tokens []TokenData) string {
	parts := make([]string, len(tokens))
	for i := range tokens {
		parts[i] = tokens[i].Token
	}
	return strings.Join(parts, " ")
}
