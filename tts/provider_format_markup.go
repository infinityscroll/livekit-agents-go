// SPDX-License-Identifier: Apache-2.0

package tts

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	agents "github.com/livekit/agents-go"
)

const maxHeldMarkupChars = 64 << 10

var allMarkupTags = []string{
	"angry", "break", "build-intensity", "calm", "confident", "curious",
	"decrease-intensity", "emotion", "emphasis", "excited", "expression", "fast",
	"happy", "higher-pitch", "laugh-speak", "loud", "lower-pitch", "nervous",
	"playful", "sad", "sarcastic", "sing-song", "singing", "slow", "soft", "sound",
	"spell", "speed", "surprised", "sympathetic", "volume", "whisper",
}

var attributeMarkupTags = []string{"expression", "emotion", "sound", "break", "speed", "volume"}

type markupTag struct {
	start, end    int
	name, attrs   string
	closing, self bool
}

// ExtractAndStrip removes selected XML tags and returns their semantic values.
// Attribute-tag names prefer their first quoted attribute over wrapped text.
func ExtractAndStrip(text string, xmlTags []string, attributeTags ...[]string) (string, []ExpressiveTag) {
	if len(xmlTags) == 0 || !strings.Contains(text, "<") {
		return text, nil
	}
	var attrs []string
	if len(attributeTags) != 0 {
		attrs = attributeTags[0]
	}
	clean := text
	all := make([]ExpressiveTag, 0, 4)
	for {
		next, tags, changed := extractAndStripPass(clean, xmlTags, attrs)
		all = append(all, tags...)
		if !changed || next == clean {
			return next, all
		}
		clean = next
	}
}

func extractAndStripPass(text string, xmlTags, attributeTags []string) (string, []ExpressiveTag, bool) {
	var out strings.Builder
	out.Grow(len(text))
	result := make([]ExpressiveTag, 0, 4)
	cursor, scan := 0, 0
	changed := false
	for {
		tag, ok := nextMarkupTag(text, scan, "")
		if !ok {
			break
		}
		scan = tag.end
		if !containsString(xmlTags, tag.name) {
			continue
		}

		matchEnd := tag.end
		inner := ""
		if !tag.closing && !tag.self {
			if closeTag, found := findClosingTag(text, tag.end, tag.name); found {
				inner = text[tag.end:closeTag.start]
				matchEnd = closeTag.end
				scan = matchEnd
			}
		}

		preStart := horizontalWhitespaceStart(text, cursor, tag.start)
		out.WriteString(text[cursor:preStart])
		pre := text[preStart:tag.start]
		if tag.closing {
			out.WriteString(dedupRemovalSpace(pre, "", text, matchEnd))
			cursor = matchEnd
			changed = true
			continue
		}

		attrValue := firstQuotedAttribute(tag.attrs)
		value := attrValue
		if containsString(attributeTags, tag.name) {
			if value == "" {
				value = strings.TrimSpace(inner)
			}
		} else if strings.TrimSpace(inner) != "" {
			value = strings.TrimSpace(inner)
		}
		result = append(result, ExpressiveTag{Type: tag.name, Value: value})
		kept := ""
		if !tag.self {
			kept = inner
		}
		out.WriteString(dedupRemovalSpace(pre, kept, text, matchEnd))
		cursor = matchEnd
		changed = true
	}
	out.WriteString(text[cursor:])
	return out.String(), result, changed
}

// ConvertExpressionTags converts framework-standard expression and sound XML
// tags to square-bracket provider cues.
func ConvertExpressionTags(text string) string {
	return replaceTagWrappers(text, func(tag markupTag) bool {
		return tag.name == "expression" || tag.name == "sound"
	}, func(tag markupTag, _ string) string {
		value := attributeValue(tag.attrs, "value")
		return "[" + value + "]"
	})
}

// SplitAllMarkup strips expr and every supported provider-native XML tag.
func SplitAllMarkup(text string) (string, []ExpressiveTag) {
	if !strings.Contains(text, "<") {
		return text, nil
	}
	withoutExpr, exprTags := splitExpr(text)
	clean, nativeTags := ExtractAndStrip(withoutExpr, allMarkupTags, attributeMarkupTags)
	return clean, append(exprTags, nativeTags...)
}

// StripAllMarkup is SplitAllMarkup with tags discarded.
func StripAllMarkup(text string) string {
	clean, _ := SplitAllMarkup(text)
	return clean
}

// StripExprMarkup removes only the framework expr dialect.
func StripExprMarkup(text string) string {
	clean, _ := splitExpr(text)
	return clean
}

func splitExpr(text string) (string, []ExpressiveTag) {
	if !strings.Contains(text, "<expr") && !strings.Contains(text, "</expr") {
		return text, nil
	}
	var out strings.Builder
	out.Grow(len(text))
	tags := make([]ExpressiveTag, 0, 4)
	cursor, scan := 0, 0
	for {
		tag, ok := nextMarkupTag(text, scan, "expr")
		if !ok {
			break
		}
		scan = tag.end
		preStart := horizontalWhitespaceStart(text, cursor, tag.start)
		out.WriteString(text[cursor:preStart])
		pre := text[preStart:tag.start]
		if !tag.closing {
			kind, label := exprAttributes(tag.attrs)
			tags = append(tags, ExpressiveTag{Type: kind, Value: label})
		}
		out.WriteString(dedupRemovalSpace(pre, "", text, tag.end))
		cursor = tag.end
	}
	out.WriteString(text[cursor:])
	return out.String(), tags
}

func cartesiaProsodyMarkup(label string) string {
	switch label {
	case "slow":
		return `<speed ratio="0.85"/>`
	case "fast":
		return `<speed ratio="1.2"/>`
	case "soft":
		return `<volume ratio="0.9"/>`
	case "loud":
		return `<volume ratio="1.3"/>`
	default:
		return ""
	}
}

func xaiSoundLabel(label string) string {
	if strings.EqualFold(label, "breathe") {
		return "breath"
	}
	return label
}

func fishSoundLabel(label string) string {
	switch strings.ToLower(label) {
	case "laugh":
		return "laughing"
	case "chuckle":
		return "chuckling"
	case "sigh":
		return "sighing"
	case "gasp":
		return "gasping"
	case "groan":
		return "groaning"
	case "yawn":
		return "yawning"
	case "sob", "cry":
		return "sobbing"
	default:
		return label
	}
}

func convertExpr(provider, text string) string {
	if !strings.Contains(text, "<expr") && !strings.Contains(text, "</expr") {
		return text
	}

	// Wrappers first. A self-closing prosody marker is never allowed to consume
	// a later </expr>; this is the Cartesia spell regression covered upstream.
	out := replaceExprWrappers(text, func(markerType, label, inner string) string {
		label = strings.ToLower(strings.TrimSpace(label))
		if markerType == "spell" {
			if provider == ProviderCartesia {
				return "<spell>" + inner + "</spell>"
			}
			return inner
		}
		switch provider {
		case ProviderXAI:
			native := strings.ReplaceAll(label, " ", "-")
			if containsString(xaiWrapping, native) {
				return "<" + native + ">" + inner + "</" + native + ">"
			}
			return inner
		case ProviderInworld:
			return `<expression value="` + label + `"/>` + inner
		case ProviderCartesia:
			return cartesiaProsodyMarkup(label) + inner
		case ProviderFishAudio:
			if label == "emphasis" {
				return "<emphasis>" + inner + "</emphasis>"
			}
			if containsString(fishTones, label) {
				return "[" + label + "] " + inner
			}
			return inner
		default:
			return inner
		}
	})

	out = replaceExprSelf(out, func(markerType, label string) string {
		switch markerType {
		case "expression":
			switch provider {
			case ProviderCartesia:
				return `<emotion value="` + label + `"/>`
			case ProviderInworld, ProviderFishAudio:
				return `<expression value="` + label + `"/>`
			default:
				return ""
			}
		case "sound":
			if provider == ProviderCartesia {
				return ""
			}
			if provider == ProviderXAI {
				label = xaiSoundLabel(label)
			}
			if provider == ProviderFishAudio {
				label = fishSoundLabel(label)
			}
			return `<sound value="` + label + `"/>`
		case "break":
			return `<break time="` + label + `"/>`
		case "prosody":
			label = strings.ToLower(strings.TrimSpace(label))
			if provider == ProviderCartesia {
				return cartesiaProsodyMarkup(label)
			}
			if provider == ProviderFishAudio && containsString(fishTones, label) {
				return "[" + label + "]"
			}
		}
		return ""
	})
	return dropExprTags(out)
}

func replaceExprWrappers(text string, replace func(string, string, string) string) string {
	var out strings.Builder
	out.Grow(len(text))
	cursor, scan := 0, 0
	for {
		tag, ok := nextMarkupTag(text, scan, "expr")
		if !ok {
			break
		}
		scan = tag.end
		if tag.closing || tag.self {
			continue
		}
		kind, label := exprAttributes(tag.attrs)
		if kind != "prosody" && kind != "spell" {
			continue
		}
		closeTag, found := findClosingTag(text, tag.end, "expr")
		if !found {
			continue
		}
		preStart := horizontalWhitespaceStart(text, cursor, tag.start)
		out.WriteString(text[cursor:preStart])
		kept := replace(kind, label, text[tag.end:closeTag.start])
		out.WriteString(dedupRemovalSpace(text[preStart:tag.start], kept, text, closeTag.end))
		cursor, scan = closeTag.end, closeTag.end
	}
	out.WriteString(text[cursor:])
	return out.String()
}

func replaceExprSelf(text string, replace func(string, string) string) string {
	var out strings.Builder
	out.Grow(len(text))
	cursor, scan := 0, 0
	for {
		tag, ok := nextMarkupTag(text, scan, "expr")
		if !ok {
			break
		}
		scan = tag.end
		if tag.closing || !tag.self {
			continue
		}
		preStart := horizontalWhitespaceStart(text, cursor, tag.start)
		out.WriteString(text[cursor:preStart])
		kind, label := exprAttributes(tag.attrs)
		kept := replace(kind, label)
		out.WriteString(dedupRemovalSpace(text[preStart:tag.start], kept, text, tag.end))
		cursor = tag.end
	}
	out.WriteString(text[cursor:])
	return out.String()
}

func dropExprTags(text string) string {
	clean, _ := splitExpr(text)
	return clean
}

// NormalizeMarkup closes provider self-closing tags commonly left open by LLMs.
func NormalizeMarkup(provider, text string) string {
	if !HasMarkupDialect(provider) || !strings.Contains(text, "<") {
		return text
	}
	native := selfClosingTags(provider)
	var out strings.Builder
	out.Grow(len(text) + 8)
	cursor, scan := 0, 0
	for {
		tag, ok := nextMarkupTag(text, scan, "")
		if !ok {
			break
		}
		scan = tag.end
		if tag.closing || tag.self {
			continue
		}
		closeIt := containsString(native, tag.name)
		if tag.name == "expr" {
			kind := attributeValue(tag.attrs, "type")
			closeIt = kind == "expression" || kind == "break" || kind == "sound"
		}
		if !closeIt {
			continue
		}
		out.WriteString(text[cursor:tag.start])
		raw := strings.TrimRightFunc(text[tag.start:tag.end-1], unicode.IsSpace)
		out.WriteString(raw)
		out.WriteString("/>")
		cursor = tag.end
	}
	if cursor == 0 {
		return text
	}
	out.WriteString(text[cursor:])
	return out.String()
}

func selfClosingTags(provider string) []string {
	switch provider {
	case ProviderCartesia:
		return []string{"emotion", "speed", "volume", "break"}
	case ProviderInworld:
		return []string{"expression", "sound", "break"}
	case ProviderFishAudio:
		return []string{"expression", "sound", "break"}
	default:
		return nil
	}
}

// ConvertMarkup lowers expr markers and framework-standard markup to a
// provider's native dialect.
func ConvertMarkup(provider, text string) string {
	out := text
	if HasMarkupDialect(provider) {
		out = convertExpr(provider, out)
	}
	if provider == ProviderInworld || provider == ProviderXAI {
		out = ConvertExpressionTags(out)
	}
	if provider == ProviderXAI {
		out = replaceSimpleTags(out, "break", func(tag markupTag, _ string) string {
			if parseDurationSeconds(attributeValue(tag.attrs, "time")) >= 1 {
				return "[long-pause]"
			}
			return "[pause]"
		})
	}
	if provider == ProviderFishAudio {
		out = replaceTagWrappers(out, func(tag markupTag) bool { return tag.name == "expression" }, func(tag markupTag, _ string) string {
			label := strings.TrimSpace(attributeValue(tag.attrs, "value"))
			if label != "" && !strings.HasPrefix(strings.ToLower(label), "very ") {
				label = "very " + label
			}
			return "[" + label + "]"
		})
		out = ConvertExpressionTags(out)
		out = replaceSimpleTags(out, "break", func(tag markupTag, _ string) string {
			if parseDurationSeconds(attributeValue(tag.attrs, "time")) >= 1 {
				return "[long-break]"
			}
			return "[break]"
		})
		out = replaceTagWrappers(out, func(tag markupTag) bool { return strings.EqualFold(tag.name, "emphasis") }, func(_ markupTag, inner string) string {
			return "[emphasis] " + strings.TrimSpace(inner)
		})
	}
	return out
}

func parseDurationSeconds(raw string) float64 {
	raw = strings.ToLower(strings.TrimSpace(raw))
	divisor := 1.0
	if strings.HasSuffix(raw, "ms") {
		raw = strings.TrimSpace(strings.TrimSuffix(raw, "ms"))
		divisor = 1000
	} else {
		raw = strings.TrimRight(raw, "s")
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return value / divisor
}

// ExpressionAttribute builds the lk.expression transcription attribute.
func ExpressionAttribute(tags []ExpressiveTag) map[string]string {
	for _, tag := range tags {
		if tag.Type != "expression" && tag.Type != "emotion" {
			continue
		}
		payload, _ := json.Marshal(struct {
			Expression string    `json:"expression"`
			Mood       AgentMood `json:"mood"`
		}{Expression: tag.Value, Mood: MatchMood(tag.Value)})
		return map[string]string{agents.AttributeTranscriptionExpression: string(payload)}
	}
	return nil
}

// TranscriptMarkupStripper incrementally strips provider-agnostic markup. Its
// retained tail is bounded so malformed input cannot grow memory indefinitely.
type TranscriptMarkupStripper struct {
	mu             sync.Mutex
	buf            string
	tags           []ExpressiveTag
	seamAfterStrip bool
	emittedVisible bool
}

// Push consumes one streamed text chunk.
func (s *TranscriptMarkupStripper) Push(text string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf += text
	if start, ok := s.openTagStart(); ok {
		// Emit everything before the partial tag immediately. consume retains only
		// bounded trailing whitespace, so a tiny malformed tail can never pin an
		// arbitrarily large visible prefix in memory.
		if start == 0 {
			return ""
		}
		prefix, tail := s.buf[:start], s.buf[start:]
		emit := s.consume(prefix, false)
		s.buf += tail
		return emit
	}
	return s.consume(s.buf, false)
}

// Flush drains any held fragment at segment end.
func (s *TranscriptMarkupStripper) Flush() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buf == "" {
		return ""
	}
	return s.consume(s.buf, true)
}

// Tags returns a defensive copy of stripped tags in observed order.
func (s *TranscriptMarkupStripper) Tags() []ExpressiveTag {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ExpressiveTag(nil), s.tags...)
}

// ExpressionAttribute returns the current lk.expression attribute.
func (s *TranscriptMarkupStripper) ExpressionAttribute() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ExpressionAttribute(s.tags)
}

func (s *TranscriptMarkupStripper) consume(text string, final bool) string {
	input := text
	if s.seamAfterStrip && len(input) != 0 && (input[0] == ' ' || input[0] == '\t') {
		input = input[:1] + strings.TrimLeft(input[1:], " \t")
	}
	clean, tags := SplitAllMarkup(input)
	s.tags = append(s.tags, tags...)
	held := ""
	if !final {
		trimmed := strings.TrimRight(clean, " \t")
		held = clean[len(trimmed):]
		if len(held) > maxHeldMarkupChars {
			held = held[len(held)-maxHeldMarkupChars:]
		}
	}
	s.buf = held
	s.seamAfterStrip = len(tags) != 0 && held != "" && strings.HasSuffix(strings.TrimRight(input, " \t"), ">")
	emit := clean[:len(clean)-len(held)]
	if !s.emittedVisible {
		emit = strings.TrimLeftFunc(emit, unicode.IsSpace)
	}
	if emit != "" {
		s.emittedVisible = true
	}
	return emit
}

func (s *TranscriptMarkupStripper) openTagStart() (int, bool) {
	last := strings.LastIndexByte(s.buf, '<')
	if last <= strings.LastIndexByte(s.buf, '>') {
		return 0, false
	}
	if len(s.buf)-last > maxHeldMarkupChars {
		return 0, false
	}
	if last+1 == len(s.buf) {
		return last, true
	}
	next := s.buf[last+1]
	return last, next == '/' || isASCIILetter(next)
}

const maxHeldBracketChars = 256

// DropBracketCues removes provider-native square-bracket cues while preserving
// all timing metadata. held carries an unfinished cue between calls.
func DropBracketCues(tokens []agents.TimedString, held *[]agents.TimedString, final bool) []agents.TimedString {
	all := make([]agents.TimedString, 0, len(*held)+len(tokens))
	all = append(all, (*held)...)
	all = append(all, tokens...)
	*held = (*held)[:0]
	var joined strings.Builder
	for _, token := range all {
		joined.WriteString(token.Text)
	}
	text := []rune(joined.String())
	if !containsRune(text, '[') {
		return all
	}
	dropped := make([]bool, len(text))
	for start := 0; start < len(text); {
		rel := indexRune(text[start:], '[')
		if rel < 0 {
			break
		}
		open := start + rel
		closeRel := indexRune(text[open+1:], ']')
		if closeRel < 0 {
			break
		}
		end := open + 1 + closeRel + 1
		dropStart, dropEnd := open, end
		if dropStart > 0 && text[dropStart-1] == ' ' && (dropEnd == len(text) || text[dropEnd] == ' ') {
			dropStart--
		} else if dropStart == 0 && dropEnd < len(text) && text[dropEnd] == ' ' {
			dropEnd++
		}
		for i := dropStart; i < dropEnd; i++ {
			dropped[i] = true
		}
		start = end
	}
	holdFrom := len(text)
	if !final {
		open := lastIndexRune(text, '[')
		if open > lastIndexRune(text, ']') && len(text)-open <= maxHeldBracketChars {
			holdFrom = open
		}
	}

	out := make([]agents.TimedString, 0, len(all))
	pos := 0
	for _, token := range all {
		runes := []rune(token.Text)
		var emit, keep strings.Builder
		for i, r := range runes {
			at := pos + i
			if at >= holdFrom {
				keep.WriteRune(r)
			} else if !dropped[at] {
				emit.WriteRune(r)
			}
		}
		pos += len(runes)
		if emit.Len() != 0 {
			copy := token
			copy.Text = emit.String()
			out = append(out, copy)
		}
		if keep.Len() != 0 {
			copy := token
			copy.Text = keep.String()
			*held = append(*held, copy)
		}
	}
	return out
}

// BracketCueStripper owns the held state needed by DropBracketCues.
type BracketCueStripper struct{ held []agents.TimedString }

func (s *BracketCueStripper) Push(tokens []agents.TimedString) []agents.TimedString {
	return DropBracketCues(tokens, &s.held, false)
}

func (s *BracketCueStripper) Flush() []agents.TimedString {
	return DropBracketCues(nil, &s.held, true)
}

func replaceTagWrappers(text string, match func(markupTag) bool, replace func(markupTag, string) string) string {
	var out strings.Builder
	out.Grow(len(text))
	cursor, scan := 0, 0
	for {
		tag, ok := nextMarkupTag(text, scan, "")
		if !ok {
			break
		}
		scan = tag.end
		if tag.closing || !match(tag) {
			continue
		}
		matchEnd := tag.end
		inner := ""
		if !tag.self {
			closeTag, found := findClosingTagFold(text, tag.end, tag.name)
			if !found {
				continue
			}
			inner, matchEnd, scan = text[tag.end:closeTag.start], closeTag.end, closeTag.end
		}
		out.WriteString(text[cursor:tag.start])
		out.WriteString(replace(tag, inner))
		cursor = matchEnd
	}
	if cursor == 0 {
		return text
	}
	out.WriteString(text[cursor:])
	return out.String()
}

func replaceSimpleTags(text, name string, replace func(markupTag, string) string) string {
	var out strings.Builder
	out.Grow(len(text))
	cursor, scan := 0, 0
	for {
		tag, ok := nextMarkupTag(text, scan, name)
		if !ok {
			break
		}
		scan = tag.end
		if tag.closing {
			continue
		}
		out.WriteString(text[cursor:tag.start])
		out.WriteString(replace(tag, ""))
		cursor = tag.end
	}
	if cursor == 0 {
		return text
	}
	out.WriteString(text[cursor:])
	return out.String()
}

func nextMarkupTag(text string, from int, onlyName string) (markupTag, bool) {
	for from < len(text) {
		rel := strings.IndexByte(text[from:], '<')
		if rel < 0 {
			return markupTag{}, false
		}
		start := from + rel
		tag, ok := parseMarkupTagAt(text, start)
		if ok && (onlyName == "" || tag.name == onlyName) {
			return tag, true
		}
		from = start + 1
	}
	return markupTag{}, false
}

func parseMarkupTagAt(text string, start int) (markupTag, bool) {
	if start < 0 || start >= len(text) || text[start] != '<' {
		return markupTag{}, false
	}
	relEnd := strings.IndexByte(text[start+1:], '>')
	if relEnd < 0 {
		return markupTag{}, false
	}
	end := start + relEnd + 2
	inside := text[start+1 : end-1]
	i := 0
	closing := false
	if i < len(inside) && inside[i] == '/' {
		closing, i = true, i+1
	}
	if i >= len(inside) || !isASCIILetter(inside[i]) {
		return markupTag{}, false
	}
	nameStart := i
	for i < len(inside) && (isASCIIWord(inside[i]) || inside[i] == '-') {
		i++
	}
	name := inside[nameStart:i]
	trimmed := strings.TrimSpace(inside)
	self := !closing && strings.HasSuffix(trimmed, "/")
	attrs := inside[i:]
	if self {
		attrs = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(attrs), "/"))
	}
	return markupTag{start: start, end: end, name: name, attrs: attrs, closing: closing, self: self}, true
}

func findClosingTag(text string, from int, name string) (markupTag, bool) {
	for {
		tag, ok := nextMarkupTag(text, from, name)
		if !ok {
			return markupTag{}, false
		}
		if tag.closing {
			return tag, true
		}
		from = tag.end
	}
}

func findClosingTagFold(text string, from int, name string) (markupTag, bool) {
	for {
		tag, ok := nextMarkupTag(text, from, "")
		if !ok {
			return markupTag{}, false
		}
		if tag.closing && strings.EqualFold(tag.name, name) {
			return tag, true
		}
		from = tag.end
	}
}

func firstQuotedAttribute(attrs string) string {
	_, value, _, ok := nextAttribute(attrs, 0)
	if !ok {
		return ""
	}
	return value
}

func exprAttributes(attrs string) (kind, label string) {
	for pos := 0; pos < len(attrs); {
		name, value, next, ok := nextAttribute(attrs, pos)
		if !ok {
			break
		}
		pos = next
		switch name {
		case "type":
			kind = value
		case "label":
			label = value
		}
		if kind != "" && label != "" {
			break
		}
	}
	return kind, label
}

func attributeValue(attrs, key string) string {
	for pos := 0; pos < len(attrs); {
		name, value, next, ok := nextAttribute(attrs, pos)
		if !ok {
			return ""
		}
		if name == key {
			return value
		}
		pos = next
	}
	return ""
}

// nextAttribute scans the small XML attribute subset emitted by the agents
// SDKs without allocating a map. Both quote styles are accepted for resilience.
func nextAttribute(attrs string, from int) (name, value string, next int, ok bool) {
	for pos := from; pos < len(attrs); {
		for pos < len(attrs) && isMarkupSpace(attrs[pos]) {
			pos++
		}
		nameStart := pos
		for pos < len(attrs) && (isASCIIWord(attrs[pos]) || attrs[pos] == '-') {
			pos++
		}
		if nameStart == pos {
			pos++
			continue
		}
		name = attrs[nameStart:pos]
		for pos < len(attrs) && isMarkupSpace(attrs[pos]) {
			pos++
		}
		if pos >= len(attrs) || attrs[pos] != '=' {
			continue
		}
		pos++
		for pos < len(attrs) && isMarkupSpace(attrs[pos]) {
			pos++
		}
		if pos >= len(attrs) || (attrs[pos] != '"' && attrs[pos] != '\'') {
			continue
		}
		quote := attrs[pos]
		pos++
		valueStart := pos
		for pos < len(attrs) && attrs[pos] != quote {
			pos++
		}
		if pos >= len(attrs) {
			return "", "", len(attrs), false
		}
		return name, attrs[valueStart:pos], pos + 1, true
	}
	return "", "", len(attrs), false
}

func dedupRemovalSpace(pre, kept, source string, matchEnd int) string {
	if kept != "" {
		return pre + kept
	}
	if pre == "" {
		return ""
	}
	if matchEnd < len(source) {
		next, _ := utf8.DecodeRuneInString(source[matchEnd:])
		if unicode.IsSpace(next) {
			return ""
		}
	}
	return pre
}

func horizontalWhitespaceStart(text string, floor, end int) int {
	for end > floor && (text[end-1] == ' ' || text[end-1] == '\t' || text[end-1] == '\v' || text[end-1] == '\f') {
		end--
	}
	return end
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func isASCIILetter(b byte) bool { return b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' }
func isASCIIWord(b byte) bool   { return isASCIILetter(b) || b >= '0' && b <= '9' || b == '_' }
func isMarkupSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }

func containsRune(values []rune, wanted rune) bool { return indexRune(values, wanted) >= 0 }
func indexRune(values []rune, wanted rune) int {
	for i, value := range values {
		if value == wanted {
			return i
		}
	}
	return -1
}
func lastIndexRune(values []rune, wanted rune) int {
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] == wanted {
			return i
		}
	}
	return -1
}
