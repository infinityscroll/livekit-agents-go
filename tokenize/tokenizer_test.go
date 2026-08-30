// SPDX-License-Identifier: Apache-2.0

package tokenize_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/tokenize"
)

const sentenceText = "Hi! " +
	"LiveKit is a platform for live audio and video applications and services. " +
	"R.T.C stands for Real-Time Communication... again R.T.C. " +
	"Mr. Theo is testing the sentence tokenizer. " +
	"This is a test. Another test. " +
	"A short sentence. " +
	"A longer sentence that is longer than the previous sentence. " +
	"Find additional resources on livekit.com. " +
	"Find additional resources on docs.livekit.com. " +
	"f(x) = x * 2.54 + 42. " +
	"Hey! Hi! Hello! "

var expectedSentences = []string{
	"Hi! LiveKit is a platform for live audio and video applications and services.",
	"R.T.C stands for Real-Time Communication... again R.T.C.",
	"Mr. Theo is testing the sentence tokenizer.",
	"This is a test. Another test.",
	"A short sentence. A longer sentence that is longer than the previous sentence.",
	"Find additional resources on livekit.com.",
	"Find additional resources on docs.livekit.com.",
	"f(x) = x * 2.54 + 42.",
	"Hey! Hi! Hello!",
}

func TestSentenceTokenizerParity(t *testing.T) {
	t.Parallel()
	tok := tokenize.NewSentenceTokenizer()
	if got := tok.Tokenize(sentenceText); !reflect.DeepEqual(got, expectedSentences) {
		t.Fatalf("sentences mismatch:\n got %#v\nwant %#v", got, expectedSentences)
	}

	streamed := drainChunked(t, tok.Stream(), sentenceText, []int{1, 2, 4})
	if !reflect.DeepEqual(streamed, expectedSentences) {
		t.Fatalf("streamed mismatch:\n got %#v\nwant %#v", streamed, expectedSentences)
	}
}

func TestSentenceSpanTrailingStopOffset(t *testing.T) {
	t.Parallel()
	got := tokenize.SplitSentences("!", 1)
	want := []tokenize.Span{{Text: "!", Start: 0, End: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestWordTokenizerParity(t *testing.T) {
	t.Parallel()
	text := "This is a test. Blabla another test! multiple consecutive spaces:     done"
	want := []string{"This", "is", "a", "test", "Blabla", "another", "test", "multiple", "consecutive", "spaces", "done"}
	if got := tokenize.NewWordTokenizer().Tokenize(text); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}

	punctText := `This is <phoneme alphabet="cmu-arpabet" ph="AE K CH UW AH L IY">actually</phoneme> tricky to handle.`
	punctWant := []string{"This", "is", "<phoneme", `alphabet="cmu-arpabet"`, `ph="AE`, "K", "CH", "UW", "AH", "L", `IY">actually</phoneme>`, "tricky", "to", "handle."}
	if got := tokenize.NewWordTokenizer(false).Tokenize(punctText); !reflect.DeepEqual(got, punctWant) {
		t.Fatalf("punctuation got %#v want %#v", got, punctWant)
	}
}

func TestSplitWordsOffsetsAndEmptyPunctuationToken(t *testing.T) {
	t.Parallel()
	spans := tokenize.SplitWords("  hello,   !!! world", true)
	want := []tokenize.Span{{Text: "hello", Start: 2, End: 8}, {Text: "", Start: 11, End: 14}, {Text: "world", Start: 15, End: 20}}
	if !reflect.DeepEqual(spans, want) {
		t.Fatalf("got %#v want %#v", spans, want)
	}
}

func TestWordTokenizerPythonCompatibilityOptions(t *testing.T) {
	t.Parallel()
	tok := tokenize.NewWordTokenizerWithOptions(tokenize.WordOptions{
		SplitCharacter:  true,
		RetainFormat:    true,
		DropEmptyTokens: true,
	})
	got := tok.Tokenize("hello  世界 !!!")
	want := []string{"hello", "  ", "世", "界", " "}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
	if formatted := tok.FormatWords([]string{"hello", "world"}); formatted != "hello world" {
		t.Fatalf("FormatWords = %q", formatted)
	}

	spans := tokenize.SplitWordsWithOptions("ภาษาไทย", tokenize.WordOptions{SplitCharacter: true})
	if len(spans) != 7 {
		t.Fatalf("Thai character split = %#v", spans)
	}
}

func TestHyphenateWordParity(t *testing.T) {
	t.Parallel()
	cases := map[string][]string{
		"Segment":       {"Seg", "ment"},
		"expected":      {"ex", "pect", "ed"},
		"communication": {"com", "mu", "ni", "ca", "tion"},
		"window":        {"win", "dow"},
		"welcome":       {"wel", "come"},
		"bedroom":       {"bed", "room"},
		"associate":     {"as", "so", "ciate"},
	}
	for word, want := range cases {
		if got := tokenize.HyphenateWord(word); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %#v want %#v", word, got, want)
		}
	}
}

func TestSplitParagraphsParity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		text string
		want []tokenize.Span
	}{
		{"Single paragraph.", []tokenize.Span{{Text: "Single paragraph.", Start: 0, End: 17}}},
		{"Paragraph 1.\n\nParagraph 2.", []tokenize.Span{{Text: "Paragraph 1.", Start: 0, End: 12}, {Text: "Paragraph 2.", Start: 14, End: 26}}},
		{"Para 1.\n\n\n\nPara 2.", []tokenize.Span{{Text: "Para 1.", Start: 0, End: 7}, {Text: "Para 2.", Start: 11, End: 18}}},
		{"\n\n  Paragraph with leading and trailing spaces.  \n\n", []tokenize.Span{{Text: "Paragraph with leading and trailing spaces.", Start: 4, End: 47}}},
		{"", []tokenize.Span{}},
		{"\n\n\n", []tokenize.Span{}},
	}
	for _, tc := range cases {
		if got := tokenize.SplitParagraphs(tc.text); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%q: got %#v want %#v", tc.text, got, tc.want)
		}
	}
}

func TestTokenStreamBackpressureAndCancellation(t *testing.T) {
	t.Parallel()
	tok := tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{
		MinSentenceLength: 1, StreamContextLength: 1, OutputCapacity: 1,
	})
	s := tok.Stream()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := s.PushText(ctx, "One complete sentence. Two complete sentences. Three complete sentences.")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected bounded output backpressure, got %v", err)
	}

	first, err := s.Recv(context.Background())
	if err != nil || first.Token == "" {
		t.Fatalf("first buffered token: %#v, %v", first, err)
	}
	done := make(chan error, 1)
	go func() {
		for {
			_, recvErr := s.Recv(context.Background())
			if errors.Is(recvErr, io.EOF) {
				done <- nil
				return
			}
			if recvErr != nil {
				done <- recvErr
				return
			}
		}
	}()
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("retry pending output: %v", err)
	}
	if err := s.EndInput(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := s.PushText(context.Background(), "late"); !errors.Is(err, stream.ErrClosed) {
		t.Fatalf("push after close: %v", err)
	}
}

func TestTokenStreamBufferLimitRejectsWithoutConsuming(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := tokenize.NewWordTokenizerWithOptions(tokenize.WordOptions{MaxBufferedBytes: 8}).Stream()
	if err := s.PushText(ctx, "123456789"); !errors.Is(err, tokenize.ErrBufferLimit) {
		t.Fatalf("oversized push error = %v", err)
	}
	if err := s.PushText(ctx, "ok "); err != nil {
		t.Fatal(err)
	}
	if err := s.EndInput(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := s.Recv(ctx)
	if err != nil || got.Token != "ok" {
		t.Fatalf("token after rejected push = %#v, %v", got, err)
	}
}

func TestFlushChangesSegmentID(t *testing.T) {
	t.Parallel()
	s := tokenize.NewWordTokenizer().Stream()
	if err := s.PushText(context.Background(), "first "); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := s.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PushText(context.Background(), "second "); err != nil {
		t.Fatal(err)
	}
	if err := s.EndInput(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := s.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.SegmentID == second.SegmentID {
		t.Fatalf("segment ID did not change: %q", first.SegmentID)
	}
}

func drainChunked(t *testing.T, s *tokenize.SentenceStream, text string, pattern []int) []string {
	t.Helper()
	ctx := context.Background()
	var out []string
	done := make(chan error, 1)
	go func() {
		for {
			token, err := s.Recv(ctx)
			if errors.Is(err, io.EOF) {
				done <- nil
				return
			}
			if err != nil {
				done <- err
				return
			}
			out = append(out, token.Token)
		}
	}()
	for pos, n := 0, 0; pos < len(text); n++ {
		size := pattern[n%len(pattern)]
		end := min(pos+size, len(text))
		if err := s.PushText(ctx, text[pos:end]); err != nil {
			t.Fatal(err)
		}
		pos = end
	}
	if err := s.EndInput(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return out
}

func FuzzSentenceTokenizer(f *testing.F) {
	for _, seed := range []string{"Hello. World!", "3 < 5", "Mr. Smith", "你好。世界", strings.Repeat("a", 2048)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		tok := tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{MinSentenceLength: 1, XMLAware: true})
		for _, span := range tok.TokenizeSpans(input) {
			if span.Start < 0 || span.End < span.Start || span.End > len(input) {
				t.Fatalf("invalid span %#v for %d bytes", span, len(input))
			}
		}
	})
}

func FuzzWordTokenizer(f *testing.F) {
	for _, seed := range []string{"hello world", "世界", "ภาษาไทย", "!!!", "\xff a"} {
		f.Add(seed, false, false)
	}
	f.Fuzz(func(t *testing.T, input string, splitCharacter, retainFormat bool) {
		spans := tokenize.SplitWordsWithOptions(input, tokenize.WordOptions{
			SplitCharacter: splitCharacter,
			RetainFormat:   retainFormat,
		})
		lastEnd := 0
		for _, span := range spans {
			if span.Start < lastEnd || span.End < span.Start || span.End > len(input) {
				t.Fatalf("invalid span %#v after %d for %d bytes", span, lastEnd, len(input))
			}
			lastEnd = span.End
		}
	})
}

func BenchmarkSentenceTokenizer(b *testing.B) {
	tok := tokenize.NewSentenceTokenizer()
	b.ReportAllocs()
	for b.Loop() {
		_ = tok.Tokenize(sentenceText)
	}
}

func BenchmarkWordTokenizer(b *testing.B) {
	tok := tokenize.NewWordTokenizer()
	b.ReportAllocs()
	for b.Loop() {
		_ = tok.Tokenize(sentenceText)
	}
}

func BenchmarkHyphenateWord(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = tokenize.HyphenateWord("communication")
	}
}
