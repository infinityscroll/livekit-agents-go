// SPDX-License-Identifier: Apache-2.0

package tokenize

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// SplitParagraphs splits at runs containing two or more newlines with only
// whitespace between them.
func SplitParagraphs(text string) []Span {
	separators := paragraphSeparators(text)
	paragraphs := make([]Span, 0, len(separators)+1)
	start := 0
	for _, sep := range separators {
		appendTrimmedSpan(&paragraphs, text, start, sep.Start)
		start = sep.End
	}
	appendTrimmedSpan(&paragraphs, text, start, len(text))
	return paragraphs
}

// TokenizeParagraphs returns paragraph text without offsets.
func TokenizeParagraphs(text string) []string {
	spans := SplitParagraphs(text)
	out := make([]string, len(spans))
	for i := range spans {
		out[i] = spans[i].Text
	}
	return out
}

func paragraphSeparators(text string) []Span {
	var out []Span
	for i := 0; i < len(text); {
		if text[i] != '\n' {
			_, n := utf8.DecodeRuneInString(text[i:])
			i += n
			continue
		}
		j := i + 1
		lastNewline := -1
		for j < len(text) {
			r, n := utf8.DecodeRuneInString(text[j:])
			if !unicode.IsSpace(r) {
				break
			}
			if r == '\n' {
				lastNewline = j
			}
			j += n
		}
		if lastNewline >= 0 {
			out = append(out, Span{Start: i, End: lastNewline + 1})
			i = lastNewline + 1
			continue
		}
		i++
	}
	return out
}

func appendTrimmedSpan(dst *[]Span, source string, start, end int) {
	if start >= end {
		return
	}
	raw := source[start:end]
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return
	}
	offset := strings.Index(raw, trimmed)
	spanStart := start + offset
	*dst = append(*dst, Span{Text: trimmed, Start: spanStart, End: spanStart + len(trimmed)})
}
