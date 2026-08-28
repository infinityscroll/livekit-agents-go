// SPDX-License-Identifier: Apache-2.0

package tokenize

import (
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	periodMarker  = "<prd>"
	stopMarker    = "<stop>"
	newlineMarker = "<nel>"
)

type sentenceRules struct {
	prefixes       *regexp.Regexp
	websites       *regexp.Regexp
	decimals       *regexp.Regexp
	dots           *regexp.Regexp
	singleInitial  *regexp.Regexp
	acronymStarter *regexp.Regexp
	tripleInitial  *regexp.Regexp
	doubleInitial  *regexp.Regexp
	suffixStarter  *regexp.Regexp
	suffix         *regexp.Regexp
	initial        *regexp.Regexp
}

var (
	rulesOnce sync.Once
	rules     sentenceRules
)

func loadSentenceRules() sentenceRules {
	rulesOnce.Do(func() {
		const (
			alphabet = `([A-Za-z])`
			suffixes = `(Inc|Ltd|Jr|Sr|Co)`
			starters = `(Mr|Mrs|Ms|Dr|Prof|Capt|Cpt|Lt|He\s|She\s|It\s|They\s|Their\s|Our\s|We\s|But\s|However\s|That\s|This\s|Wherever)`
		)
		rules = sentenceRules{
			prefixes:       regexp.MustCompile(`(Mr|St|Mrs|Ms|Dr)[.]`),
			websites:       regexp.MustCompile(`(?:\w+\.)+(?:com|net|org|io|gov|edu|me)`),
			decimals:       regexp.MustCompile(`([0-9])[.]([0-9])`),
			dots:           regexp.MustCompile(`\.{2,}`),
			singleInitial:  regexp.MustCompile(`\s` + alphabet + `[.] `),
			acronymStarter: regexp.MustCompile(`([A-Z][.][A-Z][.](?:[A-Z][.])?) ` + starters),
			tripleInitial:  regexp.MustCompile(alphabet + `[.]` + alphabet + `[.]` + alphabet + `[.]`),
			doubleInitial:  regexp.MustCompile(alphabet + `[.]` + alphabet + `[.]`),
			suffixStarter:  regexp.MustCompile(` ` + suffixes + `[.] ` + starters),
			suffix:         regexp.MustCompile(` ` + suffixes + `[.]`),
			initial:        regexp.MustCompile(` ` + alphabet + `[.]`),
		}
	})
	return rules
}

// SplitSentences implements the basic LiveKit sentence splitter. Short spans
// are merged forward until their combined length is strictly greater than
// minLength, matching the Python/TypeScript semantics.
func SplitSentences(text string, minLength int, retainFormat ...bool) []Span {
	retain := len(retainFormat) != 0 && retainFormat[0]
	r := loadSentenceRules()

	if retain {
		text = strings.ReplaceAll(text, "\n", newlineMarker+stopMarker)
	} else {
		text = strings.ReplaceAll(text, "\n", " ")
	}

	text = r.prefixes.ReplaceAllString(text, `${1}`+periodMarker)
	text = r.websites.ReplaceAllStringFunc(text, func(s string) string {
		return strings.ReplaceAll(s, ".", periodMarker)
	})
	text = r.decimals.ReplaceAllString(text, `${1}`+periodMarker+`${2}`)
	text = r.dots.ReplaceAllStringFunc(text, func(s string) string {
		return strings.Repeat(periodMarker, len(s))
	})
	text = strings.ReplaceAll(text, "Ph.D.", "Ph"+periodMarker+"D"+periodMarker)
	text = r.singleInitial.ReplaceAllString(text, ` ${1}`+periodMarker+` `)
	text = r.acronymStarter.ReplaceAllString(text, `${1}`+stopMarker+` ${2}`)
	text = r.tripleInitial.ReplaceAllString(text, `${1}`+periodMarker+`${2}`+periodMarker+`${3}`+periodMarker)
	text = r.doubleInitial.ReplaceAllString(text, `${1}`+periodMarker+`${2}`+periodMarker)
	text = r.suffixStarter.ReplaceAllString(text, `${1}`+stopMarker+` ${2}`)
	text = r.suffix.ReplaceAllString(text, `${1}`+periodMarker)
	text = r.initial.ReplaceAllString(text, `${1}`+periodMarker)

	text = strings.ReplaceAll(text, ".”", "”.")
	text = strings.ReplaceAll(text, `."`, `".`)
	text = strings.ReplaceAll(text, `!"`, `"!`)
	text = strings.ReplaceAll(text, `?"`, `"?`)
	text = markSentenceStops(text)
	text = strings.ReplaceAll(text, periodMarker, ".")
	if retain {
		text = strings.ReplaceAll(text, newlineMarker, "\n")
	}

	parts := strings.Split(text, stopMarker)
	textLength := len(text) - (len(parts)-1)*len(stopMarker)
	spans := make([]Span, 0, len(parts))
	var buf strings.Builder
	start, end := 0, 0
	for _, part := range parts {
		sentence := part
		if !retain {
			sentence = strings.TrimSpace(part)
		}
		if sentence == "" {
			// Deliberately do not advance end. The upstream splitter treats empty
			// stop spans this way, and its XML offset remapping relies on it.
			continue
		}
		if !retain && buf.Len() != 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(sentence)
		end += len(part)
		if utf8.RuneCountInString(buf.String()) > minLength {
			value := buf.String()
			spans = append(spans, Span{Text: value, Start: start, End: end})
			start = end
			buf.Reset()
		}
	}
	if buf.Len() != 0 {
		spans = append(spans, Span{Text: buf.String(), Start: start, End: textLength})
	}
	return spans
}

func markSentenceStops(text string) string {
	var b strings.Builder
	b.Grow(len(text) + len(text)/8)
	for i := 0; i < len(text); i++ {
		c := text[i]
		b.WriteByte(c)
		switch c {
		case '?', '!':
			b.WriteString(stopMarker)
		case '.':
			if i+1 == len(text) || isASCIIWhitespace(text[i+1]) {
				b.WriteString(stopMarker)
			}
		}
	}
	return b.String()
}

func isASCIIWhitespace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}
