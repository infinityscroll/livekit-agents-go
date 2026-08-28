// SPDX-License-Identifier: Apache-2.0

package tokenize

import "strings"

type xmlTagSpan struct {
	start, end    int
	name          string
	closing, self bool
}

// HasUnclosedXMLTags reports whether text ends inside a tag-shaped fragment or
// contains more opening than closing letter-named XML tags. Digit pseudo-tags
// and comparison operators are deliberately treated as prose.
func HasUnclosedXMLTags(text string) bool {
	if !strings.Contains(text, "<") {
		return false
	}
	lastOpen := strings.LastIndexByte(text, '<')
	lastClose := strings.LastIndexByte(text, '>')
	if lastOpen > lastClose {
		if lastOpen+1 == len(text) {
			return true
		}
		next := text[lastOpen+1]
		if next == '/' || isASCIILetter(next) {
			return true
		}
	}

	depth := 0
	for _, tag := range scanXMLTags(text) {
		if tag.self {
			continue
		}
		if tag.closing {
			depth--
		} else {
			depth++
		}
	}
	return depth > 0
}

func wrapXMLTokenizer(fn func(string) []Span) func(string) []Span {
	return func(text string) []Span {
		tags := scanXMLTags(text)
		if len(tags) == 0 {
			return fn(text)
		}

		var clean strings.Builder
		clean.Grow(len(text))
		pos := 0
		for _, tag := range tags {
			clean.WriteString(text[pos:tag.start])
			pos = tag.end
		}
		clean.WriteString(text[pos:])
		cleanText := clean.String()
		if strings.TrimSpace(cleanText) == "" {
			if strings.TrimSpace(text) == "" {
				return nil
			}
			return []Span{{Text: text, Start: 0, End: len(text)}}
		}

		raw := fn(cleanText)
		if len(raw) == 0 {
			return nil
		}
		result := make([]Span, 0, len(raw)+1)
		start := 0
		for _, token := range raw {
			origEnd := cleanToOriginal(token.End, tags)
			if origEnd < start {
				origEnd = start
			}
			if origEnd > len(text) {
				origEnd = len(text)
			}
			sentence := strings.TrimSpace(text[start:origEnd])
			if sentence != "" {
				result = append(result, Span{Text: sentence, Start: start, End: origEnd})
			}
			start = origEnd
		}
		if start < len(text) {
			sentence := strings.TrimSpace(text[start:])
			if sentence != "" {
				result = append(result, Span{Text: sentence, Start: start, End: len(text)})
			}
		}

		if len(result) <= 1 {
			return result
		}
		merged := make([]Span, 0, len(result))
		merged = append(merged, result[0])
		for _, current := range result[1:] {
			previous := &merged[len(merged)-1]
			if HasUnclosedXMLTags(previous.Text) || xmlOnly(previous.Text) {
				previous.Text = strings.TrimSpace(text[previous.Start:current.End])
				previous.End = current.End
			} else {
				merged = append(merged, current)
			}
		}
		return merged
	}
}

func cleanToOriginal(cleanPos int, tags []xmlTagSpan) int {
	original := cleanPos
	for _, tag := range tags {
		if tag.start >= original {
			break
		}
		original += tag.end - tag.start
	}
	return original
}

func xmlOnly(text string) bool {
	if !strings.Contains(text, "<") {
		return false
	}
	var clean strings.Builder
	pos := 0
	for _, tag := range scanXMLTags(text) {
		clean.WriteString(text[pos:tag.start])
		pos = tag.end
	}
	clean.WriteString(text[pos:])
	return strings.TrimSpace(clean.String()) == ""
}

func scanXMLTags(text string) []xmlTagSpan {
	var tags []xmlTagSpan
	for pos := 0; pos < len(text); {
		rel := strings.IndexByte(text[pos:], '<')
		if rel < 0 {
			break
		}
		start := pos + rel
		relEnd := strings.IndexByte(text[start+1:], '>')
		if relEnd < 0 {
			break
		}
		end := start + 1 + relEnd + 1
		inside := text[start+1 : end-1]
		i := 0
		closing := false
		if i < len(inside) && inside[i] == '/' {
			closing = true
			i++
		}
		if i >= len(inside) || !isASCIILetter(inside[i]) {
			pos = end
			continue
		}
		nameStart := i
		for i < len(inside) && isASCIIWord(inside[i]) {
			i++
		}
		name := inside[nameStart:i]
		trimmed := strings.TrimSpace(inside)
		self := !closing && strings.HasSuffix(trimmed, "/")
		tags = append(tags, xmlTagSpan{start: start, end: end, name: name, closing: closing, self: self})
		pos = end
	}
	return tags
}

func isASCIILetter(b byte) bool {
	return b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z'
}

func isASCIIWord(b byte) bool {
	return isASCIILetter(b) || b >= '0' && b <= '9' || b == '_'
}
