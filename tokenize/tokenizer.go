// SPDX-License-Identifier: Apache-2.0

package tokenize

import "strings"

// TokenData is one streamed token. SegmentID changes after each Flush, allowing
// downstream consumers to retain segment boundaries without sentinel values.
type TokenData struct {
	SegmentID string
	Token     string
}

// Span is a token and its half-open UTF-8 byte range in the source text.
type Span struct {
	Text       string
	Start, End int
}

// Punctuations is the punctuation vocabulary removed by the default word
// tokenizer. It intentionally matches livekit-agents 1.7.1.
const Punctuations = `!"#$%&'()*+,-./:;<=>?@[\]^_` + "`" + `{|}~±—‘’“”…`

// SentenceOptions configures SentenceTokenizer. Zero values select the same
// defaults as the Python and TypeScript SDKs.
type SentenceOptions struct {
	Language            string
	MinSentenceLength   int
	StreamContextLength int
	RetainFormat        bool
	MaxTokenLength      int
	MinTokenLength      int
	FirstTokenLength    int
	XMLAware            bool
	OutputCapacity      int
	// MaxBufferedBytes bounds un-emitted streamed input. Zero selects the
	// production default; it does not affect batch Tokenize calls.
	MaxBufferedBytes int
}

// SentenceTokenizerOptions is retained as a discoverable compatibility name.
type SentenceTokenizerOptions = SentenceOptions

// SentenceTokenizer implements the LiveKit basic English sentence tokenizer.
// It is immutable and safe for concurrent Tokenize and Stream calls.
type SentenceTokenizer struct {
	opts SentenceOptions
}

// NewSentenceTokenizer constructs a tokenizer. At most one options value is
// accepted; omitting it selects the cross-language defaults.
func NewSentenceTokenizer(options ...SentenceOptions) *SentenceTokenizer {
	var opts SentenceOptions
	if len(options) != 0 {
		opts = options[0]
	}
	if opts.Language == "" {
		opts.Language = "en-US"
	}
	if opts.MinSentenceLength <= 0 {
		opts.MinSentenceLength = 20
	}
	if opts.StreamContextLength <= 0 {
		opts.StreamContextLength = 10
	}
	if opts.OutputCapacity <= 0 {
		opts.OutputCapacity = 32
	}
	if opts.MaxBufferedBytes <= 0 {
		opts.MaxBufferedBytes = DefaultMaxBufferedBytes
	}
	return &SentenceTokenizer{opts: opts}
}

// Options returns the fully resolved configuration.
func (t *SentenceTokenizer) Options() SentenceOptions { return t.opts }

// Tokenize splits text into sentence strings. The optional language is accepted
// for parity; the built-in tokenizer is deliberately English-only.
func (t *SentenceTokenizer) Tokenize(text string, _ ...string) []string {
	spans := t.TokenizeSpans(text)
	out := make([]string, len(spans))
	for i := range spans {
		out[i] = spans[i].Text
	}
	return out
}

// TokenizeSpans is Tokenize with source offsets.
func (t *SentenceTokenizer) TokenizeSpans(text string) []Span {
	fn := func(input string) []Span {
		return SplitSentences(input, t.opts.MinSentenceLength, t.opts.RetainFormat)
	}
	if t.opts.XMLAware {
		return wrapXMLTokenizer(fn)(text)
	}
	return fn(text)
}

// Stream creates an independent incremental sentence stream.
func (t *SentenceTokenizer) Stream(_ ...string) *SentenceStream {
	minTokenLength := t.opts.MinTokenLength
	if minTokenLength <= 0 {
		minTokenLength = t.opts.MinSentenceLength
	}
	fn := func(text string) []Span {
		return SplitSentences(text, t.opts.MinSentenceLength, t.opts.RetainFormat)
	}
	return &SentenceStream{TokenStream: newTokenStream(fn, StreamOptions{
		MinTokenLength:   minTokenLength,
		MinContextLength: t.opts.StreamContextLength,
		MaxTokenLength:   t.opts.MaxTokenLength,
		FirstTokenLength: t.opts.FirstTokenLength,
		XMLAware:         t.opts.XMLAware,
		OutputCapacity:   t.opts.OutputCapacity,
		MaxBufferedBytes: t.opts.MaxBufferedBytes,
	})}
}

// WordOptions configures WordTokenizer.
type WordOptions struct {
	// KeepPunctuation opts out of the default punctuation stripping.
	KeepPunctuation bool
	// SplitCharacter emits CJK, Japanese, and Thai code points separately,
	// matching the Python tokenizer's non-spaced-language mode.
	SplitCharacter bool
	// RetainFormat attaches original inter-word whitespace to the next token.
	RetainFormat bool
	// DropEmptyTokens selects Python's behavior for punctuation-only spans. The
	// JS tokenizer retains those spans as empty tokens by default.
	DropEmptyTokens  bool
	OutputCapacity   int
	MaxBufferedBytes int
}

// WordTokenizer splits on Unicode whitespace and optionally removes the shared
// punctuation vocabulary from each word.
type WordTokenizer struct {
	ignorePunctuation bool
	splitCharacter    bool
	retainFormat      bool
	dropEmptyTokens   bool
	outputCapacity    int
	maxBufferedBytes  int
}

// NewWordTokenizer constructs a word tokenizer. Punctuation is ignored by
// default; pass false to retain it, matching new WordTokenizer(false) in JS.
func NewWordTokenizer(ignorePunctuation ...bool) *WordTokenizer {
	ignore := true
	if len(ignorePunctuation) != 0 {
		ignore = ignorePunctuation[0]
	}
	return &WordTokenizer{
		ignorePunctuation: ignore,
		dropEmptyTokens:   false,
		outputCapacity:    32,
		maxBufferedBytes:  DefaultMaxBufferedBytes,
	}
}

// NewWordTokenizerWithOptions is the struct-options constructor.
func NewWordTokenizerWithOptions(opts WordOptions) *WordTokenizer {
	capacity := opts.OutputCapacity
	if capacity <= 0 {
		capacity = 32
	}
	maxBufferedBytes := opts.MaxBufferedBytes
	if maxBufferedBytes <= 0 {
		maxBufferedBytes = DefaultMaxBufferedBytes
	}
	ignore := !opts.KeepPunctuation
	return &WordTokenizer{
		ignorePunctuation: ignore,
		splitCharacter:    opts.SplitCharacter,
		retainFormat:      opts.RetainFormat,
		dropEmptyTokens:   opts.DropEmptyTokens,
		outputCapacity:    capacity,
		maxBufferedBytes:  maxBufferedBytes,
	}
}

// Tokenize splits text into words. The optional language is accepted for parity.
func (t *WordTokenizer) Tokenize(text string, _ ...string) []string {
	spans := t.tokenizeSpans(text)
	out := make([]string, len(spans))
	for i := range spans {
		out[i] = spans[i].Text
	}
	return out
}

// TokenizeSpans is Tokenize with source offsets.
func (t *WordTokenizer) TokenizeSpans(text string) []Span {
	return t.tokenizeSpans(text)
}

// FormatWords joins already-tokenized words using the cross-language default.
func (t *WordTokenizer) FormatWords(words []string) string { return strings.Join(words, " ") }

// Stream creates an independent incremental word stream.
func (t *WordTokenizer) Stream(_ ...string) *WordStream {
	return &WordStream{TokenStream: newTokenStream(
		func(text string) []Span { return t.tokenizeSpans(text) },
		StreamOptions{
			MinTokenLength:   1,
			MinContextLength: 1,
			OutputCapacity:   t.outputCapacity,
			MaxBufferedBytes: t.maxBufferedBytes,
		},
	)}
}

func (t *WordTokenizer) tokenizeSpans(text string) []Span {
	return splitWordsConfigured(text, t.ignorePunctuation, t.splitCharacter, t.retainFormat, t.dropEmptyTokens)
}

func stripPunctuation(word string) string {
	return strings.Map(func(r rune) rune {
		if strings.ContainsRune(Punctuations, r) {
			return -1
		}
		return r
	}, word)
}
