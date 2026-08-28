// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"strings"
	"testing"
)

func textPointer(value string) *string { return &value }

func TestThinkingTokenFilterChunkBoundaries(t *testing.T) {
	filter := NewThinkingTokenFilter("", "")
	inputs := []string{"visible <thi", "nk>secret", " reasoning</th", "ink> tail"}
	want := []struct {
		text string
		ok   bool
	}{{"visible ", true}, {"", false}, {"", false}, {" tail", true}}
	for index, input := range inputs {
		got, ok := filter.StripThinkingTokens(&input, false)
		if got != want[index].text || ok != want[index].ok {
			t.Fatalf("chunk %d = %q,%t want %q,%t", index, got, ok, want[index].text, want[index].ok)
		}
		if filter.BufferedBytes() >= len(ThinkTagStart) && filter.BufferedBytes() >= len(ThinkTagEnd) {
			t.Fatalf("unbounded partial marker: %d", filter.BufferedBytes())
		}
	}
}

func TestThinkingTokenFilterFinalAndEmpty(t *testing.T) {
	filter := NewThinkingTokenFilter("[reason]", "[/reason]")
	if got, ok := filter.StripThinkingTokens(textPointer("prefix [rea"), false); !ok || got != "prefix " {
		t.Fatalf("first = %q,%t", got, ok)
	}
	if got, ok := filter.StripThinkingTokens(nil, true); !ok || got != "[rea" {
		t.Fatalf("final = %q,%t", got, ok)
	}
	if got, ok := filter.StripThinkingTokens(textPointer(""), false); !ok || got != "" {
		t.Fatalf("empty = %q,%t", got, ok)
	}
	if got, ok := filter.StripThinkingTokens(textPointer("[reason]unterminated"), true); ok || got != "" {
		t.Fatalf("unterminated final = %q,%t", got, ok)
	}
}

func TestThinkingTokenFilterMultipleBlocks(t *testing.T) {
	filter := NewThinkingTokenFilter("", "")
	input := "a<think>x</think>b<think>y</think>c"
	got, ok := filter.StripThinkingTokens(&input, true)
	if !ok || got != "abc" {
		t.Fatalf("got = %q,%t", got, ok)
	}
}

func FuzzThinkingTokenFilter(f *testing.F) {
	f.Add("a<think>x</think>b", uint8(3))
	f.Add("<think>unterminated", uint8(1))
	f.Fuzz(func(t *testing.T, input string, chunks uint8) {
		count := int(chunks%32) + 1
		filter := NewThinkingTokenFilter("", "")
		var output strings.Builder
		for offset, part := 0, 0; offset < len(input); part++ {
			remaining := len(input) - offset
			size := max(1, (remaining+count-part-1)/max(1, count-part))
			if size > remaining {
				size = remaining
			}
			chunk := input[offset : offset+size]
			if text, ok := filter.StripThinkingTokens(&chunk, false); ok {
				output.WriteString(text)
			}
			offset += size
		}
		if text, ok := filter.StripThinkingTokens(nil, true); ok {
			output.WriteString(text)
		}
		if filter.BufferedBytes() != 0 {
			t.Fatalf("buffer not reset")
		}
	})
}
