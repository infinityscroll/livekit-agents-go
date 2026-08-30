// SPDX-License-Identifier: Apache-2.0

package tokenize_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/infinityscroll/livekit-agents-go/tokenize"
)

func TestHasUnclosedXMLTags(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"3 < 5.":                        false,
		"i <3 you":                      false,
		"price < 10 dollars":            false,
		"Hello <emo":                    true,
		"Hello <":                       true,
		"<spell>abc":                    true,
		"Rate this from <1> to <5>":     false,
		"Scores: <3 wins> today.":       false,
		"<spell>abc</spell> done":       false,
		"<sound value=\"laugh\"/> done": false,
	}
	for input, want := range cases {
		if got := tokenize.HasUnclosedXMLTags(input); got != want {
			t.Errorf("%q: got %v want %v", input, got, want)
		}
	}
}

func TestXMLAwareSentenceTokenizer(t *testing.T) {
	t.Parallel()
	tok := tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{MinSentenceLength: 1, XMLAware: true})
	text := `<expression value="speak cheerfully"/> Hello and welcome! ` +
		`<expression value="bright"/> Great specials today. ` +
		`<expression value="excited"/> Try our new sandwich.`
	got := tok.Tokenize(text)
	if len(got) != 3 {
		t.Fatalf("got %#v", got)
	}
	for i, label := range []string{"speak cheerfully", "bright", "excited"} {
		if !strings.Contains(got[i], label) {
			t.Errorf("sentence %d lost tag: %q", i, got[i])
		}
	}

	wrapped := tok.Tokenize("Spell it: <spell>U.S.A.</spell>. Got it?")
	for _, sentence := range wrapped {
		if strings.Contains(sentence, "<spell>") && !strings.Contains(sentence, "</spell>") {
			t.Fatalf("wrapper split: %#v", wrapped)
		}
	}

	last := tok.Tokenize("<x/> Hello there world how are you today. <y/> Bye now")
	want := []string{"<x/> Hello there world how are you today.", "<y/> Bye now"}
	if !reflect.DeepEqual(last, want) {
		t.Fatalf("trailing character regression: got %#v want %#v", last, want)
	}
}

func TestXMLAwareStreamingHoldsPartialTag(t *testing.T) {
	t.Parallel()
	tok := tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{
		MinSentenceLength: 1, StreamContextLength: 5, XMLAware: true,
	})
	s := tok.Stream()
	ctx := context.Background()
	if err := s.PushText(ctx, "Hello. <emo"); err != nil {
		t.Fatal(err)
	}
	if err := s.PushText(ctx, `tion value="happy"/> Great!`); err != nil {
		t.Fatal(err)
	}
	if err := s.EndInput(ctx); err != nil {
		t.Fatal(err)
	}
	var got []string
	for {
		token, err := s.Recv(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, token.Token)
	}
	if !strings.Contains(strings.Join(got, " "), `<emotion value="happy"/>`) {
		t.Fatalf("tag lost or split: %#v", got)
	}
}

func BenchmarkXMLAwareSentenceTokenizer(b *testing.B) {
	tok := tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{XMLAware: true})
	text := `<expr type="expression" label="happy"/> Welcome! <expr type="prosody" label="soft">How are you today?</expr>`
	b.ReportAllocs()
	for b.Loop() {
		_ = tok.Tokenize(text)
	}
}
