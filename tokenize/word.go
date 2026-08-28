// SPDX-License-Identifier: Apache-2.0

package tokenize

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// SplitWords splits text on Unicode whitespace. Offsets always refer to the
// unmodified source, even when punctuation is removed from Span.Text.
func SplitWords(text string, ignorePunctuation ...bool) []Span {
	ignore := true
	if len(ignorePunctuation) != 0 {
		ignore = ignorePunctuation[0]
	}
	return splitWordsConfigured(text, ignore, false, false, false)
}

// SplitWordsWithOptions exposes Python-compatible character splitting and
// formatting. Stream-only WordOptions fields are ignored.
func SplitWordsWithOptions(text string, opts WordOptions) []Span {
	return splitWordsConfigured(text, !opts.KeepPunctuation, opts.SplitCharacter, opts.RetainFormat, opts.DropEmptyTokens)
}

func splitWordsConfigured(text string, ignorePunctuation, splitCharacter, retainFormat, dropEmpty bool) []Span {
	if !splitCharacter && !retainFormat {
		return splitWhitespaceWords(text, ignorePunctuation, dropEmpty)
	}
	words := make([]Span, 0, 16)
	wordStart := 0
	appendWord := func(start, end int) {
		if start >= end {
			return
		}
		word := text[start:end]
		if ignorePunctuation {
			word = stripPunctuation(word)
		}
		if dropEmpty && word == "" {
			return
		}
		words = append(words, Span{Text: word, Start: start, End: end})
	}

	for i, r := range text {
		if unicode.IsSpace(r) {
			if retainFormat && strings.TrimSpace(text[wordStart:i]) == "" {
				continue
			}
			appendWord(wordStart, i)
			if retainFormat {
				wordStart = i
			} else {
				wordStart = i + utf8.RuneLen(r)
			}
			continue
		}
		if splitCharacter && isCharacterBased(r) {
			appendWord(wordStart, i)
			end := i + utf8.RuneLen(r)
			appendWord(i, end)
			wordStart = end
		}
	}
	appendWord(wordStart, len(text))
	return words
}

func splitWhitespaceWords(text string, ignorePunctuation, dropEmpty bool) []Span {
	words := make([]Span, 0, 16)
	start := -1
	appendWord := func(end int) {
		word := text[start:end]
		if ignorePunctuation {
			word = stripPunctuation(word)
		}
		if !dropEmpty || word != "" {
			words = append(words, Span{Text: word, Start: start, End: end})
		}
		start = -1
	}
	for i, r := range text {
		if unicode.IsSpace(r) {
			if start >= 0 {
				appendWord(i)
			}
			continue
		}
		if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		appendWord(len(text))
	}
	return words
}

func isCharacterBased(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fff' ||
		r >= '\u3040' && r <= '\u30ff' ||
		r >= '\u3400' && r <= '\u4dbf' ||
		r >= '\u0e00' && r <= '\u0e7f'
}
