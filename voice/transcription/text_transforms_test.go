// SPDX-License-Identifier: Apache-2.0

package transcription

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/livekit/agents-go/stream"
)

func chunked(value string, size int) stream.Reader[string] {
	output := stream.NewChannel[string](max(1, len(value)+1))
	for offset := 0; offset < len(value); offset += size {
		end := min(len(value), offset+size)
		_ = output.Send(context.Background(), value[offset:end])
	}
	_ = output.Close()
	return output
}

func chunks(values ...string) stream.Reader[string] {
	output := stream.NewChannel[string](max(1, len(values)))
	for _, value := range values {
		_ = output.Send(context.Background(), value)
	}
	_ = output.Close()
	return output
}

func collect(t *testing.T, reader stream.Reader[string]) (string, []string) {
	t.Helper()
	var text strings.Builder
	var values []string
	for {
		value, err := reader.Recv(context.Background())
		if errors.Is(err, io.EOF) {
			return text.String(), values
		}
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
		text.WriteString(value)
	}
}

func TestFilterMarkdownChunkIndependent(t *testing.T) {
	cases := map[string]string{
		"He said *hello* again.":              "He said hello again.",
		"This is ***very important*** text.":  "This is very important text.",
		"**_mixed_** here.":                   "mixed here.",
		"_**mixed**_ here.":                   "mixed here.",
		"这是**很重要**的文本。":                       "这是很重要的文本。",
		"before\n- - -\nafter":                "before\n\nafter",
		"[LiveKit](https://livekit.io) rocks": "LiveKit rocks",
		"~~remove me~~ keep":                  " keep",
		"2 * 3 = 6":                           "2 * 3 = 6",
		"x**2 + y**2 = z**2":                  "x**2 + y**2 = z**2",
		"__dunder_method__ stays":             "__dunder_method__ stays",
	}
	for input, want := range cases {
		for _, size := range []int{1, 2, 3, 7, 50} {
			got, _ := collect(t, NewMarkdownFilter(chunked(input, size)))
			if got != want {
				t.Fatalf("filter %q size %d = %q; want %q", input, size, got, want)
			}
		}
	}
}

func TestReplaceAcrossChunks(t *testing.T) {
	transform := Replace(map[string]string{"LiveKit": "Lyve Kit", "SQL": "sequel", "boundary": "EDGE"}, false)
	for _, size := range []int{1, 2, 5, 11, 50} {
		got, _ := collect(t, transform.Transform(chunked("LiveKit uses SQL. livekit boundary test.", size)))
		if got != "Lyve Kit uses sequel. Lyve Kit EDGE test." {
			t.Fatalf("size %d = %q", size, got)
		}
	}
}

func TestReplaceHoldbackTopology(t *testing.T) {
	transform := Replace(map[string]string{"LiveKit": "Lyve Kit"}, false)
	_, got := collect(t, transform.Transform(chunks("visit Live", "Kit now")))
	want := []string{"visit ", "Lyve Kit now"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("chunks = %#v", got)
	}
	_, got = collect(t, transform.Transform(chunks("visit Live", "ly now")))
	want = []string{"visit ", "Lively now"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("rejected prefix chunks = %#v", got)
	}
}

func TestReplaceDoesNotCascade(t *testing.T) {
	got, _ := collect(t, Replace(map[string]string{"a": "b", "b": "c"}, true).Transform(chunked("a", 1)))
	if got != "b" {
		t.Fatalf("cascade result = %q", got)
	}
}

func TestFilterEmoji(t *testing.T) {
	got, _ := collect(t, NewEmojiFilter(chunks("hello 😀", " world✨")))
	if got != "hello  world" {
		t.Fatalf("emoji result = %q", got)
	}
}
