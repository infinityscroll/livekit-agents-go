// SPDX-License-Identifier: Apache-2.0

package llm

import "strings"

const (
	ThinkTagStart = "<think>"
	ThinkTagEnd   = "</think>"
)

// ThinkingTokenFilter incrementally removes model reasoning delimited by
// StartTag and EndTag. It retains only a possible partial delimiter between
// calls, so memory remains bounded even when a provider emits an unterminated
// reasoning block.
//
// A filter is stateful and intended for one stream. It is not safe for
// concurrent calls; provider stream readers are already serialized.
type ThinkingTokenFilter struct {
	StartTag string
	EndTag   string
	buffer   string
	end      string
}

func NewThinkingTokenFilter(startTag, endTag string) *ThinkingTokenFilter {
	if startTag == "" {
		startTag = ThinkTagStart
	}
	if endTag == "" {
		endTag = ThinkTagEnd
	}
	return &ThinkingTokenFilter{StartTag: startTag, EndTag: endTag}
}

// StripThinkingTokens consumes one delta. The returned bool distinguishes no
// visible content from a deliberately visible empty provider delta. final
// flushes any visible partial text and resets the filter for reuse.
func (f *ThinkingTokenFilter) StripThinkingTokens(content *string, final bool) (string, bool) {
	if f == nil {
		if content == nil {
			return "", false
		}
		return *content, true
	}
	if f.StartTag == "" {
		f.StartTag = ThinkTagStart
	}
	if f.EndTag == "" {
		f.EndTag = ThinkTagEnd
	}
	if content != nil {
		f.buffer += *content
	}

	var visible strings.Builder
	for f.buffer != "" {
		if f.end != "" {
			if index := strings.Index(f.buffer, f.end); index >= 0 {
				f.buffer = f.buffer[index+len(f.end):]
				f.end = ""
				continue
			}
			keep := partialMarkerLength(f.buffer, f.end)
			if keep == 0 {
				f.buffer = ""
			} else {
				f.buffer = f.buffer[len(f.buffer)-keep:]
			}
			break
		}

		if index := strings.Index(f.buffer, f.StartTag); index >= 0 {
			visible.WriteString(f.buffer[:index])
			f.buffer = f.buffer[index+len(f.StartTag):]
			f.end = f.EndTag
			continue
		}
		keep := partialMarkerLength(f.buffer, f.StartTag)
		if keep == 0 {
			visible.WriteString(f.buffer)
			f.buffer = ""
		} else {
			visible.WriteString(f.buffer[:len(f.buffer)-keep])
			f.buffer = f.buffer[len(f.buffer)-keep:]
		}
		break
	}

	if final {
		if f.end == "" {
			visible.WriteString(f.buffer)
		}
		f.buffer = ""
		f.end = ""
	}
	result := visible.String()
	return result, result != "" || content != nil && *content == ""
}

// Reset discards buffered visible/reasoning state.
func (f *ThinkingTokenFilter) Reset() {
	if f == nil {
		return
	}
	f.buffer = ""
	f.end = ""
}

// BufferedBytes reports the retained partial delimiter length. It is useful
// for saturation diagnostics and tests.
func (f *ThinkingTokenFilter) BufferedBytes() int {
	if f == nil {
		return 0
	}
	return len(f.buffer)
}

func partialMarkerLength(content, marker string) int {
	longest := 0
	limit := min(len(content), len(marker)-1)
	for length := 1; length <= limit; length++ {
		if strings.HasSuffix(content, marker[:length]) {
			longest = length
		}
	}
	return longest
}
