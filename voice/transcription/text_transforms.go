// SPDX-License-Identifier: Apache-2.0

// Package transcription provides streaming transcript synchronization and
// text transforms for voice sessions.
package transcription

import (
	"context"
	"errors"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/infinityscroll/livekit-agents-go/stream"
)

type TextTransform interface {
	Transform(stream.Reader[string]) stream.Reader[string]
}

type TransformFunc func(stream.Reader[string]) stream.Reader[string]

func (f TransformFunc) Transform(input stream.Reader[string]) stream.Reader[string] {
	return f(input)
}

type BuiltinTextTransform string

const (
	FilterMarkdown BuiltinTextTransform = "filter_markdown"
	FilterEmoji    BuiltinTextTransform = "filter_emoji"
)

func (b BuiltinTextTransform) Transform(input stream.Reader[string]) stream.Reader[string] {
	switch b {
	case FilterMarkdown:
		return NewMarkdownFilter(input)
	case FilterEmoji:
		return NewEmojiFilter(input)
	default:
		return &errorReader[string]{err: errors.New("unknown builtin text transform: " + string(b))}
	}
}

func ApplyTextTransforms(input stream.Reader[string], transforms ...TextTransform) (stream.Reader[string], error) {
	if input == nil {
		return nil, errors.New("text transform input must not be nil")
	}
	current := input
	for _, transform := range transforms {
		if transform == nil {
			return nil, errors.New("text transform must not be nil")
		}
		current = transform.Transform(current)
		if current == nil {
			return nil, errors.New("text transform returned nil")
		}
	}
	return current, nil
}

type errorReader[T any] struct {
	err  error
	once bool
}

func (r *errorReader[T]) Recv(context.Context) (T, error) {
	var zero T
	if r.once {
		return zero, io.EOF
	}
	r.once = true
	return zero, r.err
}

type emojiFilter struct{ source stream.Reader[string] }

func NewEmojiFilter(source stream.Reader[string]) stream.Reader[string] {
	return &emojiFilter{source: source}
}

func (f *emojiFilter) Recv(ctx context.Context) (string, error) {
	chunk, err := f.source.Recv(ctx)
	if err != nil {
		return "", err
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 0x1f000 && r <= 0x1fbff,
			r >= 0x2600 && r <= 0x26ff,
			r >= 0x2700 && r <= 0x27bf,
			r >= 0x2b00 && r <= 0x2bff,
			r >= 0xfe00 && r <= 0xfe0f,
			r == 0x200d, r == 0x20e3:
			return -1
		default:
			return r
		}
	}, chunk), nil
}

type markdownFilter struct {
	source        stream.Reader[string]
	buffer        string
	bufferNewline bool
	pending       []string
	done          bool
}

func NewMarkdownFilter(source stream.Reader[string]) stream.Reader[string] {
	return &markdownFilter{source: source, bufferNewline: true}
}

func (f *markdownFilter) Recv(ctx context.Context) (string, error) {
	for {
		if len(f.pending) != 0 {
			value := f.pending[0]
			copy(f.pending, f.pending[1:])
			f.pending[len(f.pending)-1] = ""
			f.pending = f.pending[:len(f.pending)-1]
			return value, nil
		}
		if f.done {
			return "", io.EOF
		}

		chunk, err := f.source.Recv(ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				f.done = true
				return "", err
			}
			f.done = true
			if f.buffer != "" {
				value := processMarkdown(f.buffer, f.bufferNewline, true)
				f.buffer = ""
				return value, nil
			}
			return "", io.EOF
		}
		f.buffer += chunk

		if strings.Contains(f.buffer, "\n") {
			lines := strings.Split(f.buffer, "\n")
			f.buffer = lines[len(lines)-1]
			for index, line := range lines[:len(lines)-1] {
				isNewline := true
				if index == 0 {
					isNewline = f.bufferNewline
				}
				f.pending = append(f.pending, processMarkdown(line, isNewline, true)+"\n")
			}
			f.bufferNewline = true
			continue
		}

		if split := lastInlineSplit(f.buffer); split >= 1 {
			processable := f.buffer[:split]
			if !hasIncompleteMarkdown(processable) {
				f.pending = append(f.pending, processMarkdown(processable, f.bufferNewline, false))
				f.buffer = f.buffer[split:]
				f.bufferNewline = false
			}
		}
	}
}

func lastInlineSplit(value string) int {
	last := -1
	for _, token := range []string{" ", ",", ".", "?", "!", ";", "，", "。", "？", "！", "；"} {
		if index := strings.LastIndex(value, token); index > last {
			last = index
		}
	}
	return last
}

func hasIncompleteMarkdown(value string) bool {
	if value == "" {
		return false
	}
	last, _ := utf8.DecodeLastRuneInString(value)
	if strings.ContainsRune("#-+*_>!`~ ", last) {
		return true
	}
	if unbalanced(value, "*") || unbalanced(value, "_") || strings.Count(value, "`")%2 == 1 || strings.Count(value, "~~")%2 == 1 {
		return true
	}
	open := strings.Count(value, "[")
	completeLinks := countCompleteLinks(value, false)
	completeImages := countCompleteLinks(value, true)
	return open-completeLinks-completeImages > 0
}

func unbalanced(value, delimiter string) bool {
	doubles := strings.Count(value, delimiter+delimiter)
	return doubles%2 == 1 || (strings.Count(value, delimiter)-doubles*2)%2 == 1
}

func countCompleteLinks(value string, images bool) int {
	count := 0
	for offset := 0; offset < len(value); {
		open := strings.Index(value[offset:], "[")
		if open < 0 {
			break
		}
		open += offset
		isImage := open > 0 && value[open-1] == '!'
		closeLabel := strings.Index(value[open+1:], "](")
		if closeLabel < 0 {
			break
		}
		closeLabel += open + 1
		closeURL := strings.IndexByte(value[closeLabel+2:], ')')
		if closeURL < 0 {
			break
		}
		if isImage == images {
			count++
		}
		offset = closeLabel + 2 + closeURL + 1
	}
	return count
}

func processMarkdown(value string, atLineStart, lineEnd bool) string {
	if atLineStart {
		if lineEnd && horizontalRule(value) {
			return ""
		}
		value = stripLineMarker(value)
	}
	if !strings.ContainsAny(value, "*_`~[") {
		return value
	}
	value = stripLinks(value)
	value = stripEmphasis(value, '*')
	value = stripEmphasis(value, '_')
	value = stripEmphasis(value, '*')
	value = stripCode(value)
	value = stripStrikethrough(value)
	return value
}

func horizontalRule(value string) bool {
	if strings.HasPrefix(value, "\t") || strings.HasPrefix(value, "    ") {
		return false
	}
	trimmed := strings.TrimSpace(value)
	if len(trimmed) < 3 {
		return false
	}
	marker := rune(trimmed[0])
	if marker != '-' && marker != '*' && marker != '_' {
		return false
	}
	count := 0
	for _, r := range trimmed {
		if r == marker {
			count++
			continue
		}
		if r != ' ' && r != '\t' {
			return false
		}
	}
	return count >= 3
}

func stripLineMarker(value string) string {
	if strings.HasPrefix(value, "#") {
		index := 0
		for index < len(value) && value[index] == '#' && index < 6 {
			index++
		}
		if index < len(value) && unicode.IsSpace(rune(value[index])) {
			return strings.TrimLeftFunc(value[index:], unicode.IsSpace)
		}
	}
	index := 0
	for index < len(value) && (value[index] == ' ' || value[index] == '\t') {
		index++
	}
	if index < len(value) && strings.ContainsRune("-+*>", rune(value[index])) {
		next := index + 1
		if next < len(value) && unicode.IsSpace(rune(value[next])) {
			return strings.TrimLeftFunc(value[next:], unicode.IsSpace)
		}
	}
	return value
}

func stripLinks(value string) string {
	var out strings.Builder
	for offset := 0; offset < len(value); {
		open := strings.Index(value[offset:], "[")
		if open < 0 {
			out.WriteString(value[offset:])
			break
		}
		open += offset
		start := open
		if open > 0 && value[open-1] == '!' {
			start--
		}
		closeLabel := strings.Index(value[open+1:], "](")
		if closeLabel < 0 {
			out.WriteString(value[offset:])
			break
		}
		closeLabel += open + 1
		closeURL := strings.IndexByte(value[closeLabel+2:], ')')
		if closeURL < 0 {
			out.WriteString(value[offset:])
			break
		}
		closeURL += closeLabel + 2
		out.WriteString(value[offset:start])
		out.WriteString(value[open+1 : closeLabel])
		offset = closeURL + 1
	}
	return out.String()
}

func stripEmphasis(value string, marker byte) string {
	for offset := 0; offset < len(value); {
		open := strings.IndexByte(value[offset:], marker)
		if open < 0 {
			return value
		}
		open += offset
		count := 1
		for count < 3 && open+count < len(value) && value[open+count] == marker {
			count++
		}
		if open > 0 && value[open-1] == marker || !validEmphasisOpen(value, open, count, marker) {
			offset = open + count
			continue
		}
		token := strings.Repeat(string(marker), count)
		contentStart := open + count
		search := contentStart
		matched := false
		for search < len(value) {
			relative := strings.Index(value[search:], token)
			if relative < 0 {
				break
			}
			closeAt := search + relative
			if strings.ContainsRune(value[contentStart:closeAt], rune(marker)) || strings.ContainsRune(value[contentStart:closeAt], '\n') {
				break
			}
			if validEmphasisClose(value, contentStart, closeAt, count, marker) {
				value = value[:open] + value[contentStart:closeAt] + value[closeAt+count:]
				offset = open
				matched = true
				break
			}
			search = closeAt + 1
		}
		if !matched {
			offset = contentStart
		}
	}
	return value
}

func validEmphasisOpen(value string, open, count int, marker byte) bool {
	if open+count >= len(value) {
		return false
	}
	next, _ := utf8.DecodeRuneInString(value[open+count:])
	if unicode.IsSpace(next) {
		return false
	}
	if open == 0 {
		return true
	}
	previous, _ := utf8.DecodeLastRuneInString(value[:open])
	if marker == '_' {
		return !isUnicodeWord(previous)
	}
	return !isAsteriskIntraword(previous)
}

func validEmphasisClose(value string, contentStart, closeAt, count int, marker byte) bool {
	if closeAt <= contentStart {
		return false
	}
	previous, _ := utf8.DecodeLastRuneInString(value[:closeAt])
	if unicode.IsSpace(previous) {
		return false
	}
	nextAt := closeAt + count
	if nextAt < len(value) && value[nextAt] == marker {
		return false
	}
	if nextAt >= len(value) {
		return true
	}
	next, _ := utf8.DecodeRuneInString(value[nextAt:])
	if marker == '_' {
		return !isUnicodeWord(next)
	}
	return !isAsteriskIntraword(next)
}

func isUnicodeWord(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r) }
func isAsteriskIntraword(r rune) bool {
	return isUnicodeWord(r) && !isFlushEmphasisScript(r)
}
func isFlushEmphasisScript(r rune) bool {
	return r >= 0x0e00 && r <= 0x0e7f || r >= 0x1100 && r <= 0x11ff ||
		r >= 0x3040 && r <= 0x30ff || r >= 0x3130 && r <= 0x318f ||
		r >= 0x3400 && r <= 0x4dbf || r >= 0x4e00 && r <= 0x9fff ||
		r >= 0xac00 && r <= 0xd7af || r >= 0xf900 && r <= 0xfaff ||
		r >= 0xff66 && r <= 0xff9d
}

func stripCode(value string) string {
	for {
		start := strings.Index(value, "```")
		if start < 0 {
			break
		}
		end := start + 3
		for end < len(value) && value[end] == '`' && end < start+4 {
			end++
		}
		for end < len(value) && !unicode.IsSpace(rune(value[end])) {
			end++
		}
		value = value[:start] + value[end:]
	}
	for {
		start := strings.IndexByte(value, '`')
		if start < 0 {
			return value
		}
		end := strings.IndexByte(value[start+1:], '`')
		if end < 0 {
			return value
		}
		end += start + 1
		value = value[:start] + value[start+1:end] + value[end+1:]
	}
}

func stripStrikethrough(value string) string {
	for offset := 0; offset < len(value); {
		relative := strings.Index(value[offset:], "~~")
		if relative < 0 {
			return value
		}
		start := offset + relative
		if start+2 >= len(value) {
			return value
		}
		next, _ := utf8.DecodeRuneInString(value[start+2:])
		if unicode.IsSpace(next) {
			offset = start + 2
			continue
		}
		endRelative := strings.Index(value[start+2:], "~~")
		if endRelative < 0 {
			return value
		}
		end := start + 2 + endRelative
		if strings.ContainsRune(value[start+2:end], '~') {
			offset = start + 2
			continue
		}
		previous, _ := utf8.DecodeLastRuneInString(value[:end])
		if unicode.IsSpace(previous) {
			offset = end + 2
			continue
		}
		value = value[:start] + value[end+2:]
		offset = start
	}
	return value
}

type replacementTransform struct {
	entries       []replacementEntry
	pattern       *regexp.Regexp
	prefixes      []string
	caseSensitive bool
}

type replacementEntry struct{ old, replacement string }

func Replace(replacements map[string]string, caseSensitive bool) TextTransform {
	entries := make([]replacementEntry, 0, len(replacements))
	for old, replacement := range replacements {
		if old != "" {
			entries = append(entries, replacementEntry{old: old, replacement: replacement})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if len(entries[i].old) == len(entries[j].old) {
			return entries[i].old < entries[j].old
		}
		return len(entries[i].old) > len(entries[j].old)
	})
	patterns := make([]string, len(entries))
	prefixSet := make(map[string]struct{})
	for index, entry := range entries {
		patterns[index] = regexp.QuoteMeta(entry.old)
		for _, offset := range rangeRuneBoundaries(entry.old) {
			if offset > 0 && offset < len(entry.old) {
				prefixSet[entry.old[:offset]] = struct{}{}
			}
		}
	}
	prefixes := make([]string, 0, len(prefixSet))
	for prefix := range prefixSet {
		prefixes = append(prefixes, prefix)
	}
	sort.Slice(prefixes, func(i, j int) bool { return len(prefixes[i]) > len(prefixes[j]) })
	var pattern *regexp.Regexp
	if len(patterns) != 0 {
		expression := strings.Join(patterns, "|")
		if !caseSensitive {
			expression = "(?i:" + expression + ")"
		}
		pattern = regexp.MustCompile(expression)
	}
	return &replacementTransform{entries: entries, pattern: pattern, prefixes: prefixes, caseSensitive: caseSensitive}
}

func rangeRuneBoundaries(value string) []int {
	result := make([]int, 0, utf8.RuneCountInString(value)+1)
	for offset := range value {
		result = append(result, offset)
	}
	result = append(result, len(value))
	return result
}

func (t *replacementTransform) Transform(source stream.Reader[string]) stream.Reader[string] {
	return &replacementReader{source: source, transform: t}
}

type replacementReader struct {
	source    stream.Reader[string]
	transform *replacementTransform
	buffer    string
	pending   []string
	done      bool
}

func (r *replacementReader) Recv(ctx context.Context) (string, error) {
	for {
		if len(r.pending) != 0 {
			value := r.pending[0]
			r.pending = r.pending[1:]
			return value, nil
		}
		if r.done {
			return "", io.EOF
		}
		chunk, err := r.source.Recv(ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				r.done = true
				return "", err
			}
			r.done = true
			if r.buffer != "" {
				value := r.buffer
				r.buffer = ""
				return value, nil
			}
			return "", io.EOF
		}
		source := r.buffer + chunk
		output, lastMatch := r.transform.apply(source)
		held := r.transform.holdback(source)
		heldStart := len(source) - held
		retain := held > 0 && heldStart >= lastMatch
		if retain {
			r.buffer = source[heldStart:]
			output = output[:len(output)-held]
		} else {
			r.buffer = ""
		}
		if output != "" {
			return output, nil
		}
	}
}

func (t *replacementTransform) apply(value string) (string, int) {
	if t.pattern == nil {
		return value, 0
	}
	matches := t.pattern.FindAllStringIndex(value, -1)
	if len(matches) == 0 {
		return value, 0
	}
	var out strings.Builder
	last := 0
	for _, match := range matches {
		out.WriteString(value[last:match[0]])
		matched := value[match[0]:match[1]]
		found := false
		for _, entry := range t.entries {
			equal := entry.old == matched
			if !t.caseSensitive {
				equal = strings.EqualFold(entry.old, matched)
			}
			if equal {
				out.WriteString(entry.replacement)
				found = true
				break
			}
		}
		if !found {
			out.WriteString(matched)
		}
		last = match[1]
	}
	out.WriteString(value[last:])
	return out.String(), last
}

func (t *replacementTransform) holdback(value string) int {
	for _, prefix := range t.prefixes {
		if len(prefix) <= len(value) && strings.EqualFold(value[len(value)-len(prefix):], prefix) {
			return len(prefix)
		}
	}
	return 0
}
