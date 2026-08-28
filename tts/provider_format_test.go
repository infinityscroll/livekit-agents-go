// SPDX-License-Identifier: Apache-2.0

package tts_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/tokenize"
	"github.com/livekit/agents-go/tts"
)

const joke = `<expr type="expression" label="say playfully"/> Why did the burger go to the gym? ` +
	`<expr type="break" label="500ms"/> Because it wanted better buns! ` +
	`<expr type="sound" label="laugh"/>`

func TestConvertMarkupXAI(t *testing.T) {
	t.Parallel()
	cases := []struct{ input, want string }{
		{`So I walked in and <expr type="break" label="500ms"/> there it was! <expr type="sound" label="laugh"/> <expr type="prosody" label="whisper">It was a secret.</expr>`, `So I walked in and [pause] there it was! [laugh] <whisper>It was a secret.</whisper>`},
		{`<expr type="break" label="50ms"/>`, `[pause]`},
		{`<expr type="break" label="2s"/>`, `[long-pause]`},
		{`<expr type="sound" label="breathe"/>`, `[breath]`},
		{`<expr type="prosody" label="higher pitch">no way</expr>`, `<higher-pitch>no way</higher-pitch>`},
		{`<expr type="prosody" label="like a pirate">ahoy there</expr>`, `ahoy there`},
		{`<expr type="expression" label="say playfully"/> Hello!`, ` Hello!`},
		{`<expr type="prosody" label="loud">hello there`, `hello there`},
		{`hello there</expr>`, `hello there`},
	}
	for _, tc := range cases {
		if got := tts.ConvertMarkup(tts.ProviderXAI, tc.input); got != tc.want {
			t.Errorf("input %q:\n got %q\nwant %q", tc.input, got, tc.want)
		}
	}
}

func TestConvertMarkupInworld(t *testing.T) {
	t.Parallel()
	want := `[say playfully] Why did the burger go to the gym? <break time="500ms"/> Because it wanted better buns! [laugh]`
	if got := tts.ConvertMarkup(tts.ProviderInworld, joke); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := tts.ConvertMarkup(tts.ProviderInworld, `<expr type="prosody" label="whisper">keep it secret</expr>`); got != `[whisper]keep it secret` {
		t.Fatalf("stray prosody: %q", got)
	}
}

func TestConvertMarkupCartesia(t *testing.T) {
	t.Parallel()
	cases := []struct{ input, want string }{
		{`<expr type="expression" label="excited"/> We won! <expr type="break" label="1s"/> <expr type="sound" label="laugh"/> Unbelievable.`, `<emotion value="excited"/> We won! <break time="1s"/> Unbelievable.`},
		{`Your code is <expr type="spell">A7X9</expr>.`, `Your code is <spell>A7X9</spell>.`},
		{`<expr type="prosody" label="slow"/> One moment.`, `<speed ratio="0.85"/> One moment.`},
		{`<expr type="prosody" label="loud"/> We won!`, `<volume ratio="1.3"/> We won!`},
		{`<expr type="prosody" label="soft">bad news</expr>`, `<volume ratio="0.9"/>bad news`},
		{`<expr type="prosody" label="whisper">keep it secret</expr>`, `keep it secret`},
		{`<expr type="prosody" label="slow"/> One moment. Your code is <expr type="spell">A7X9</expr>.`, `<speed ratio="0.85"/> One moment. Your code is <spell>A7X9</spell>.`},
	}
	for _, tc := range cases {
		if got := tts.ConvertMarkup(tts.ProviderCartesia, tc.input); got != tc.want {
			t.Errorf("input %q:\n got %q\nwant %q", tc.input, got, tc.want)
		}
	}
	for _, provider := range []string{tts.ProviderXAI, tts.ProviderInworld} {
		if got := tts.ConvertMarkup(provider, `Your code is <expr type="spell">A7X9</expr>.`); got != "Your code is A7X9." {
			t.Errorf("%s spell: %q", provider, got)
		}
	}
}

func TestConvertMarkupFishAudio(t *testing.T) {
	t.Parallel()
	cases := []struct{ input, want string }{
		{`<expr type="expression" label="regretful"/> That's on us. <expr type="sound" label="sigh"/>`, `[very regretful] That's on us. [sighing]`},
		{`<expr type="prosody" label="whispering">don't tell anyone</expr>`, `[whispering] don't tell anyone`},
		{`Are you <expr type="prosody" label="emphasis">sure</expr>?`, `Are you [emphasis] sure?`},
		{`<expr type="break" label="500ms"/>`, `[break]`},
		{`<expr type="break" label="2s"/>`, `[long-break]`},
	}
	for _, tc := range cases {
		if got := tts.ConvertMarkup(tts.ProviderFishAudio, tc.input); got != tc.want {
			t.Errorf("input %q:\n got %q\nwant %q", tc.input, got, tc.want)
		}
	}
}

func TestExtractAndStrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		tags  []string
		want  string
	}{
		{`<emotion value="happy"/> Hello!`, []string{"emotion"}, ` Hello!`},
		{`<spell>A.B.C.</spell> confirmed`, []string{"spell"}, `A.B.C. confirmed`},
		{`<emotion value="happy"/> <custom>keep</custom>`, []string{"emotion"}, ` <custom>keep</custom>`},
		{`Press [Enter] <emotion value="happy"/> to open [the docs](https://lk.io)`, []string{"emotion"}, `Press [Enter] to open [the docs](https://lk.io)`},
		{`Right. <emotion value="sad"/> Anyway.`, []string{"emotion"}, `Right. Anyway.`},
		{`a <spell>b</spell> c`, []string{"spell"}, `a b c`},
		{"a\n<emotion value=\"sad\"/>\nb", []string{"emotion"}, "a\n\nb"},
		{`<excited><loud>no way</loud></excited>`, []string{"excited", "loud"}, `no way`},
	}
	for _, tc := range cases {
		got, _ := tts.ExtractAndStrip(tc.input, tc.tags)
		if got != tc.want {
			t.Errorf("input %q: got %q want %q", tc.input, got, tc.want)
		}
	}

	clean, tags := tts.ExtractAndStrip(`<emotion value="happy"/>hi <spell>A7</spell>`, []string{"emotion", "spell"})
	if clean != "hi A7" || !reflect.DeepEqual(tags, []tts.ExpressiveTag{{Type: "emotion", Value: "happy"}, {Type: "spell", Value: "A7"}}) {
		t.Fatalf("clean=%q tags=%#v", clean, tags)
	}
}

func TestSplitAllMarkupAndExpressionAttribute(t *testing.T) {
	t.Parallel()
	clean, tags := tts.SplitAllMarkup(joke)
	if strings.TrimSpace(clean) != "Why did the burger go to the gym? Because it wanted better buns!" {
		t.Fatalf("clean: %q", clean)
	}
	wantTags := []tts.ExpressiveTag{{Type: "expression", Value: "say playfully"}, {Type: "break", Value: "500ms"}, {Type: "sound", Value: "laugh"}}
	if !reflect.DeepEqual(tags, wantTags) {
		t.Fatalf("got %#v want %#v", tags, wantTags)
	}
	attr := tts.ExpressionAttribute(tags)
	if attr[agents.AttributeTranscriptionExpression] != `{"expression":"say playfully","mood":"playful"}` {
		t.Fatalf("attribute: %#v", attr)
	}

	clean, tags = tts.SplitAllMarkup(`<emotion value="happy"/>Hi <expression value="warm"/>there <sound value="giggle"/>[pause] friend`)
	if clean != "Hi there [pause] friend" {
		t.Fatalf("universal clean: %q", clean)
	}
	if len(tags) != 3 {
		t.Fatalf("universal tags: %#v", tags)
	}

	clean, tags = tts.SplitAllMarkup(`<expression value="warm">Hello there</expression>`)
	if clean != "Hello there" || !reflect.DeepEqual(tags, []tts.ExpressiveTag{{Type: "expression", Value: "warm"}}) {
		t.Fatalf("wrapped attribute: %q %#v", clean, tags)
	}
	if got := tts.ExpressionAttribute(tags)[agents.AttributeTranscriptionExpression]; got != `{"expression":"warm","mood":"happy"}` {
		t.Fatalf("warm attribute: %s", got)
	}
}

func TestStripExprMarkupOnly(t *testing.T) {
	t.Parallel()
	input := `<expr type="expression" label="happy"/> Press [Enter] to see <b>bold</b>, read [the docs](https://docs.livekit.io), then 1 < 2. <break time="1s"/> <expr type="prosody" label="whisper">keep it secret</expr>`
	want := ` Press [Enter] to see <b>bold</b>, read [the docs](https://docs.livekit.io), then 1 < 2. <break time="1s"/> keep it secret`
	if got := tts.StripExprMarkup(input); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestTranscriptMarkupStripper(t *testing.T) {
	t.Parallel()
	s := &tts.TranscriptMarkupStripper{}
	out := s.Push("Hi <emo")
	out += s.Push(`tion value="happy"/> the`)
	out += s.Push("re")
	out += s.Flush()
	if strings.Contains(out, "<emotion") || strings.ReplaceAll(out, " ", "") != "Hithere" {
		t.Fatalf("output: %q", out)
	}
	if got := s.ExpressionAttribute()[agents.AttributeTranscriptionExpression]; got != `{"expression":"happy","mood":"happy"}` {
		t.Fatalf("attribute: %q", got)
	}

	markdown := &tts.TranscriptMarkupStripper{}
	first := markdown.Push("Read [the docs](https:")
	rest := markdown.Push("//docs.livekit.io) now.") + markdown.Flush()
	if first+rest != "Read [the docs](https://docs.livekit.io) now." {
		t.Fatalf("markdown: %q", first+rest)
	}

	comparison := &tts.TranscriptMarkupStripper{}
	first = comparison.Push("The value 3 < 5 ")
	rest = comparison.Push("is true.") + comparison.Flush()
	if strings.ReplaceAll(first+rest, " ", "") != "Thevalue3<5istrue." {
		t.Fatalf("comparison: %q", first+rest)
	}

	for _, chunks := range [][]string{
		{"Right. ", `<expr type="sound" label="laugh"/>`, " Anyway."},
		{"Right. ", `<expr type="sound" label="laugh"/> Anyway.`},
		{"Right. ", `<sound value="laugh"/>`, " Anyway."},
	} {
		stripper := &tts.TranscriptMarkupStripper{}
		var got strings.Builder
		for _, chunk := range chunks {
			got.WriteString(stripper.Push(chunk))
		}
		got.WriteString(stripper.Flush())
		if got.String() != "Right. Anyway." {
			t.Errorf("seam %#v: %q", chunks, got.String())
		}
	}

	untagged := &tts.TranscriptMarkupStripper{}
	got := untagged.Push("Right. ") + untagged.Push(" Anyway.") + untagged.Flush()
	if got != "Right.  Anyway." {
		t.Fatalf("untagged whitespace changed: %q", got)
	}

	bounded := &tts.TranscriptMarkupStripper{}
	prefix := strings.Repeat("a", 128<<10)
	if got := bounded.Push(prefix + " <emo"); got != prefix {
		t.Fatalf("partial tag pinned visible prefix: got %d bytes want %d", len(got), len(prefix))
	}
	if got := bounded.Push(`tion value="happy"/> end`) + bounded.Flush(); got != " end" {
		t.Fatalf("bounded partial completion: %q", got)
	}
}

func TestNormalizeMarkup(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{tts.ProviderXAI, tts.ProviderInworld, tts.ProviderCartesia, tts.ProviderFishAudio} {
		input := `<expr type="sound" label="laugh"> Hello`
		want := `<expr type="sound" label="laugh"/> Hello`
		if got := tts.NormalizeMarkup(provider, input); got != want {
			t.Errorf("%s: got %q want %q", provider, got, want)
		}
	}
	input := `<expr type="prosody" label="whisper">hi</expr> <expr type="break" label="1s"/> <expr type="spell">A7X9</expr>`
	if got := tts.NormalizeMarkup(tts.ProviderXAI, input); got != input {
		t.Fatalf("wrapped changed: %q", got)
	}
}

func TestLLMInstructionsAndSteering(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{tts.ProviderXAI, tts.ProviderInworld, tts.ProviderCartesia, tts.ProviderFishAudio} {
		instructions, ok := tts.LLMInstructions(provider, nil)
		if !ok || !strings.Contains(instructions, "<expr") || !strings.Contains(instructions, `<expr type="break" label="`) || !strings.Contains(instructions, "REGISTER of the moment") {
			t.Errorf("%s instructions missing contract", provider)
		}
	}
	if _, ok := tts.LLMInstructions("openai", nil); ok {
		t.Fatal("unexpected openai dialect")
	}

	cartesia, _ := tts.LLMInstructions(tts.ProviderCartesia, nil)
	if !strings.Contains(cartesia, "NOT free-form") || !strings.Contains(cartesia, `<expr type="spell">`) || strings.Contains(cartesia, `type="sound"`) {
		t.Fatalf("cartesia guide mismatch")
	}
	inworld, _ := tts.LLMInstructions(tts.ProviderInworld, nil)
	if !strings.Contains(inworld, "free-form") || !strings.Contains(inworld, "clear throat") || strings.Contains(inworld, `type="prosody"`) {
		t.Fatalf("inworld guide mismatch")
	}
	xai, _ := tts.LLMInstructions(tts.ProviderXAI, nil)
	if !strings.Contains(xai, "tongue-click") || !strings.Contains(xai, `<expr type="prosody" label="STYLE">`) || strings.Contains(xai, `type="expression"`) {
		t.Fatalf("xai guide mismatch")
	}

	steering := tts.SpeechSteeringOptions{NonverbalSounds: tts.NonverbalOptions{Laughing: tts.ToggleDisabled}}
	inworld, _ = tts.LLMInstructions(tts.ProviderInworld, &steering)
	if strings.Contains(inworld, `label="laugh"`) || !strings.Contains(inworld, "clear throat") {
		t.Fatalf("sparse inworld steering mismatch")
	}
	xai, _ = tts.LLMInstructions(tts.ProviderXAI, &steering)
	if strings.Contains(xai, "laugh-speak") || !strings.Contains(xai, "whisper") {
		t.Fatalf("sparse xai steering mismatch")
	}

	allOff := tts.SpeechSteeringOptions{Disfluencies: tts.ToggleDisabled, NonverbalSounds: tts.NonverbalOptions{All: tts.ToggleDisabled}}
	fish, _ := tts.LLMInstructions(tts.ProviderFishAudio, &allOff)
	if strings.Contains(strings.ToLower(fish), "laugh") || strings.Contains(strings.ToLower(fish), "filler") || strings.Contains(fish, "Um, uh") {
		t.Fatalf("disabled concept leaked into Fish guide")
	}
	if got := tts.SteeringInstructions(tts.ProviderInworld, tts.SpeechSteeringOptions{Disfluencies: tts.ToggleDisabled}); !strings.Contains(got, "No fillers") {
		t.Fatalf("disfluency steering: %q", got)
	}
	if got := tts.SteeringInstructions(tts.ProviderInworld, tts.SpeechSteeringOptions{Pace: tts.PaceSlow}); !strings.Contains(got, "slow overall speaking") {
		t.Fatalf("pace steering: %q", got)
	}
}

func TestSupportedNonverbals(t *testing.T) {
	t.Parallel()
	want := map[tts.NonverbalField][]string{
		tts.NonverbalLaughing: {"laughing", "chuckling"}, tts.NonverbalBreathing: {"gasping"},
		tts.NonverbalSighing: {"sighing"}, tts.NonverbalCrying: {"sobbing"},
		tts.NonverbalVocalizing: {"groaning"}, tts.NonverbalReflexSounds: {"clear throat", "yawning"},
	}
	if got := tts.SupportedNonverbals(tts.ProviderFishAudio); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
	if got := tts.SupportedNonverbals(tts.ProviderCartesia); len(got) != 0 {
		t.Fatalf("cartesia: %#v", got)
	}
}

func TestMoodMatching(t *testing.T) {
	t.Parallel()
	cases := map[string]tts.AgentMood{
		"soft, with genuine care":   tts.MoodEmpathetic,
		"gently curious, welcoming": tts.MoodCurious,
		"like a pirate":             tts.MoodCalm,
		"regretful":                 tts.MoodSad,
		"determined":                tts.MoodHopeful,
		"frustrated":                tts.MoodAngry,
	}
	for input, want := range cases {
		if got := tts.MatchMood(input); got != want {
			t.Errorf("%q: got %q want %q", input, got, want)
		}
	}
	if _, ok := tts.MatchMoodOK("like a pirate"); ok {
		t.Fatal("pirate matched irate mid-word")
	}
}

func TestDropBracketCues(t *testing.T) {
	t.Parallel()
	timed := func(text string) agents.TimedString {
		start, end := time.Duration(0), time.Second
		confidence := 0.9
		speaker := "agent"
		return agents.CreateTimedString(agents.TimedStringOptions{Text: text, StartTime: &start, EndTime: &end, Confidence: &confidence, SpeakerID: &speaker})
	}
	held := []agents.TimedString{}
	tokens := []agents.TimedString{timed("Right."), timed(" "), timed("[laughing]"), timed(" "), timed("Anyway.")}
	out := tts.DropBracketCues(tokens, &held, true)
	if got := joinTimed(out); got != "Right. Anyway." {
		t.Fatalf("got %q", got)
	}
	for _, token := range out {
		if token.Confidence == nil || *token.Confidence != 0.9 || token.SpeakerID == nil || *token.SpeakerID != "agent" {
			t.Fatalf("metadata lost: %#v", token)
		}
	}

	held = nil
	out = tts.DropBracketCues([]agents.TimedString{timed("Hello "), timed("[lau")}, &held, false)
	if joinTimed(out) != "Hello " || len(held) == 0 {
		t.Fatalf("first pass: out=%q held=%#v", joinTimed(out), held)
	}
	out = tts.DropBracketCues([]agents.TimedString{timed("ghing] there")}, &held, false)
	if joinTimed(out) != "there" {
		t.Fatalf("second pass: %q", joinTimed(out))
	}

	held = nil
	_ = tts.DropBracketCues([]agents.TimedString{timed("Hello [lau")}, &held, false)
	out = tts.DropBracketCues(nil, &held, true)
	if joinTimed(out) != "[lau" {
		t.Fatalf("final release: %q", joinTimed(out))
	}
}

func TestExpressiveChunking(t *testing.T) {
	t.Parallel()
	reply := `<expr type="expression" label="really amiable and welcoming"/> Hey, good to hear from you! ` +
		`<expr type="expression" label="gently inquisitive"/> How did the interview go? ` +
		`<expr type="expression" label="really bright, upbeat energy"/> I have been thinking about it all week.`
	for _, provider := range []string{tts.ProviderInworld, tts.ProviderXAI, tts.ProviderCartesia} {
		plain := collectTokens(t, tts.SentenceTokenizer(provider, false), reply)
		expressive := collectTokens(t, tts.SentenceTokenizer(provider, true), reply)
		if len(expressive) >= len(plain) || expressive[0] != plain[0] {
			t.Errorf("%s: plain=%#v expressive=%#v", provider, plain, expressive)
		}
	}
	plain := collectTokens(t, tts.SentenceTokenizer(tts.ProviderFishAudio, false), reply)
	expressive := collectTokens(t, tts.SentenceTokenizer(tts.ProviderFishAudio, true), reply)
	if len(plain) != len(expressive) {
		t.Fatalf("Fish should remain per-sentence: %d != %d", len(plain), len(expressive))
	}
}

func collectTokens(t *testing.T, tokenizer *tokenize.SentenceTokenizer, text string) []string {
	t.Helper()
	stream := tokenizer.Stream()
	ctx := context.Background()
	for start := 0; start < len(text); {
		end := min(start+12, len(text))
		if err := stream.PushText(ctx, text[start:end]); err != nil {
			t.Fatal(err)
		}
		start = end
	}
	if err := stream.EndInput(ctx); err != nil {
		t.Fatal(err)
	}
	var out []string
	for {
		token, err := stream.Recv(ctx)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, token.Token)
	}
}

func joinTimed(tokens []agents.TimedString) string {
	var out strings.Builder
	for _, token := range tokens {
		out.WriteString(token.Text)
	}
	return out.String()
}

func FuzzProviderMarkup(f *testing.F) {
	for _, seed := range []string{"plain", joke, `<expr`, `<expression value=\"warm\">hello</expression>`, strings.Repeat("<", 1024)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		for _, provider := range []string{tts.ProviderCartesia, tts.ProviderInworld, tts.ProviderXAI, tts.ProviderFishAudio, "unknown"} {
			_ = tts.NormalizeMarkup(provider, input)
			_ = tts.ConvertMarkup(provider, input)
		}
		clean, _ := tts.SplitAllMarkup(input)
		if !strings.Contains(input, "<") && clean != input {
			t.Fatalf("plain fast path changed text")
		}
	})
}

func BenchmarkConvertMarkup(b *testing.B) {
	for b.Loop() {
		_ = tts.ConvertMarkup(tts.ProviderXAI, joke)
	}
}

func BenchmarkStripPlainTranscript(b *testing.B) {
	text := "This is an ordinary transcript with no expressive markup."
	b.ReportAllocs()
	for b.Loop() {
		_ = tts.StripAllMarkup(text)
	}
}

func ExampleConvertMarkup() {
	text := `<expr type="sound" label="laugh"/> That was a good one.`
	fmt.Println(tts.ConvertMarkup(tts.ProviderXAI, text))
	// Output: [laugh] That was a good one.
}
