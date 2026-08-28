// SPDX-License-Identifier: Apache-2.0

package tts

import (
	"fmt"
	"slices"
	"strings"
	"sync"
)

const expressionPreamble = `You control speech delivery with a single XML marker tag: <expr/>. Every marker has a type attribute. Use only the marker types listed below, and where a type lists a label vocabulary, only those labels. Use the markers often and diversify them so the voice never sounds flat while ensuring the markers are appropriate for the moment. Write the words themselves the way people talk: use contractions ("I'm", "you're", "don't") — spelled-out forms like "I am" or "do not" sound stiff when spoken.

Just as important is knowing when NOT to reach for a marker. Reserve surprise openers like "oh" or "ah" for genuine surprise — an ordinary request isn't one. Don't stack markers on short replies or decorate every sentence. If a reaction wouldn't happen in a real conversation, skip it — there's always another genuine beat to lean into.

Match your delivery to the REGISTER of the moment, and reassess every turn. When the moment is professional, high-stakes, or emotionally heavy — bad news, an emergency, real distress — keep delivery composed and restrained. When the moment is casual, playful, or celebratory, let it loosen and brighten. A serious turn in an otherwise casual conversation still gets a composed reply.`

const cartesiaInstructions = expressionPreamble + `

1. Emotion - sets the emotional tone. Self-closing; place before EVERY sentence.
   <expr type="expression" label="EMOTION"/>
   Labels are a fixed vocabulary, NOT free-form descriptions. Best results: neutral, angry, excited, content, sad, scared.
   Also available: happy, enthusiastic, elated, triumphant, amazed, surprised, flirtatious, curious, peaceful, serene, calm, grateful, affectionate, sympathetic, mysterious, frustrated, disgusted, sarcastic, ironic, dejected, melancholic, disappointed, apologetic, hesitant, confused, anxious, panicked, proud, confident, contemplative, determined, joking/comedic.

2. Pauses - insert silence when appropriate. Self-closing.
   <expr type="break" label="1s"/> - label is a duration in seconds or milliseconds.

3. Prosody - adjusts pacing and loudness from that point on. Self-closing.
   <expr type="prosody" label="slow"/> slower    <expr type="prosody" label="fast"/> faster
   <expr type="prosody" label="soft"/> quieter    <expr type="prosody" label="loud"/> louder
   Labels are a fixed vocabulary: slow, fast, soft, loud.

4. Spell - wraps text read character by character (codes, IDs, or a spelled-out name).
   <expr type="spell">A7X9</expr>
   Keep punctuation out of a spell marker — a period inside is read as "dot"; add spaces inside for grouped pauses (<expr type="spell">ABC 123</expr>).

This voice has no non-verbal sounds and no free-form delivery descriptions — do not invent other types or labels.

Examples:
  <expr type="expression" label="excited"/> I can't wait to tell you! <expr type="expression" label="happy"/> This is going to be great!
  <expr type="expression" label="curious"/> Really? <expr type="break" label="500ms"/> <expr type="expression" label="excited"/> Tell me more!
  Your code is <expr type="spell">A7X9</expr>. <expr type="break" label="1s"/> <expr type="expression" label="calm"/> Got it?`

var inworldExamples = []string{
	`<expr type="expression" label="say really playfully"/> Okay okay, why did the burger go to the gym? <expr type="break" label="500ms"/> <expr type="expression" label="really bright, a little fast"/> Because it wanted better buns! <expr type="sound" label="laugh"/>`,
	`<expr type="expression" label="a little sheepish, apologetic"/> Ah man, yeah that's on us. <expr type="expression" label="speak really calmly"/> Lemme see what I can do.`,
	`<expr type="sound" label="sigh"/> <expr type="expression" label="speak softly, almost a whisper"/> I know it's been a rough week.`,
	`<expr type="expression" label="really amiable and welcoming"/> Welcome to the hotel. <expr type="expression" label="gently inquisitive, slightly fast"/> How can I help you today?`,
	`<expr type="expression" label="gently easygoing and reassuring"/> That's all set. <expr type="break" label="300ms"/> <expr type="expression" label="slow and really clearly enunciated"/> Your confirmation code is B 4 J 7.`,
	`<expr type="expression" label="really chill, a little fast"/> Yeah, of course! <expr type="expression" label="casual, almost fast"/> Gimme one sec, pulling it up now.`,
}

var xaiExamples = []string{
	`So I walked in and <expr type="break" label="500ms"/> <expr type="sound" label="inhale"/> there it was! <expr type="prosody" label="whisper">It was a secret the whole time.</expr>`,
	`<expr type="prosody" label="build-intensity">This is going to be so good.</expr> <expr type="prosody" label="loud">I can't wait!</expr>`,
	`<expr type="prosody" label="soft">Hey.</expr> <expr type="sound" label="sigh"/> <expr type="prosody" label="lower-pitch">I know it's been a rough week.</expr> I'm right here.`,
	`<expr type="prosody" label="higher-pitch">You did not just say that</expr> okay, <expr type="prosody" label="fast">tell me everything.</expr>`,
	`<expr type="prosody" label="emphasis">Everything</expr> is confirmed for <expr type="break" label="500ms"/> Thursday the <expr type="prosody" label="emphasis">ninth</expr>. <expr type="prosody" label="slow">Is there anything else I can help you with?</expr>`,
}

var fishExamples = []string{
	`<expr type="expression" label="excited"/> That's hilarious! <expr type="sound" label="laughing"/> <expr type="expression" label="happy"/> You always lighten the mood.`,
	`<expr type="expression" label="empathetic"/> <expr type="sound" label="clear throat"/> That sounds like a <expr type="prosody" label="emphasis">really</expr> difficult experience.`,
	`<expr type="expression" label="sad"/> Oh, my goodness <expr type="sound" label="clear throat"/> <expr type="break" label="2s"/> that's a real shame.`,
	`<expr type="expression" label="frustrated"/> <expr type="sound" label="sighing"/> I've been going in circles with this all morning. <expr type="expression" label="determined"/> Okay. One more try.`,
	`<expr type="expression" label="happy"/> You're all set for <expr type="break" label="500ms"/> Thursday the <expr type="prosody" label="emphasis">ninth</expr>. <expr type="expression" label="curious"/> Is there anything else I can help you with?`,
	`<expr type="expression" label="delighted"/> <expr type="prosody" label="whispering">Okay, don't tell anyone yet</expr> <expr type="expression" label="excited"/> but I think we actually pulled it off!`,
}

var fishDisfluentExamples = []string{
	`<expr type="expression" label="curious"/> Um, uh... really? <expr type="expression" label="sad"/> Well, I'm really sorry to hear that.`,
	`<expr type="expression" label="regretful"/> I really wish I'd, um, called sooner. <expr type="expression" label="hopeful"/> But I'm here now if, if you want to talk.`,
	`<expr type="expression" label="surprised"/> What?! No way! I, I'm flabbergasted! <expr type="expression" label="sarcastic"/> Fair play, I guess.`,
}

var (
	defaultInworldInstructions = sync.OnceValue(func() string { return inworldInstructions(inworldSounds) })
	defaultXAIInstructions     = sync.OnceValue(func() string { return xaiInstructions(xaiInline, xaiWrapping) })
	defaultFishInstructions    = sync.OnceValue(func() string { return fishInstructions(fishSounds, true) })
)

// LLMInstructions returns the provider's expr guide. ok is false for providers
// without a markup dialect. A nil steering pointer selects the provider default.
func LLMInstructions(provider string, steering *SpeechSteeringOptions) (instructions string, ok bool) {
	if !HasMarkupDialect(provider) {
		return "", false
	}
	if provider == ProviderCartesia {
		return cartesiaInstructions, true
	}
	if steering == nil || steeringIsDefault(*steering) {
		switch provider {
		case ProviderInworld:
			return defaultInworldInstructions(), true
		case ProviderXAI:
			return defaultXAIInstructions(), true
		case ProviderFishAudio:
			return defaultFishInstructions(), true
		}
	}
	switch provider {
	case ProviderInworld:
		return inworldInstructions(allowedSounds(provider, steering)), true
	case ProviderXAI:
		return xaiInstructions(allowedSounds(provider, steering), allowedProsody(provider, steering)), true
	case ProviderFishAudio:
		disfluencies := steering == nil || steering.Disfluencies != ToggleDisabled
		return fishInstructions(allowedSounds(provider, steering), disfluencies), true
	default:
		return "", false
	}
}

// LLMInstructionsText is a convenience for call sites where an unsupported
// provider naturally maps to an empty instruction string.
func LLMInstructionsText(provider string, steering *SpeechSteeringOptions) string {
	text, _ := LLMInstructions(provider, steering)
	return text
}

func steeringIsDefault(s SpeechSteeringOptions) bool {
	return (s.Disfluencies == ToggleDefault || s.Disfluencies == ToggleEnabled) &&
		s.NonverbalSounds == (NonverbalOptions{}) &&
		(s.Pace == PaceDefault || s.Pace == PaceNormal)
}

func inworldInstructions(sounds []string) string {
	sections := []string{`Delivery - controls how a sentence sounds. Self-closing; place before EVERY sentence.
   <expr type="expression" label="DESCRIPTION"/>
   The label is free-form: describe vocal quality, pitch, volume, pace, and intonation in plain English — "say really playfully", "slightly surprised, amiable", "sound a little concerned", "drop to almost a whisper", "speak really slowly and clearly, patient and reassuring".
   Match the expression tag's energy to the sentence's punctuation. An exclamation needs a bright or upbeat label; a calm or reassuring label flattens the "!". Never lead an exclamatory sentence with a calm tag.
   Put each question in its own sentence so it carries its own delivery tag. Never put "questioning" in a tag — describe the mood alone and let the question mark carry intonation.
   Name a mood or speaking style, not a mechanical pitch contour. Use at most two aligned adjectives per tag. Put a degree modifier in EVERY tag — "a little", "almost", "slightly", "gently", "really" — and save "really" for true peaks.
   Carry your persona into the tags. Don't open a turn with a "slow" tag; reserve slow, clearly-enunciated delivery for a total, date, address, or confirmation code. Rotate expression labels rather than reusing one two turns in a row.`}
	if len(sounds) != 0 {
		section := fmt.Sprintf(`Sounds - a non-verbal sound between sentences. Self-closing.
   <expr type="sound" label="%s"/>
   Labels are a fixed vocabulary: %s.
   Use non-verbal sounds sparingly, never the same one twice in a row, and only where it genuinely fits.`, sounds[0], strings.Join(sounds, ", "))
		if slices.Contains(sounds, "clear throat") {
			section += " A clear-throat can mark a shift to a new step or topic."
		}
		if slices.Contains(sounds, "breathe") {
			section += ` Use "breathe" only for a real, gentle breath, never as filler; it can read as a weary or impatient sigh.`
		}
		sections = append(sections, section)
	}
	sections = append(sections, `Pauses - insert silence when appropriate. Self-closing.
   <expr type="break" label="500ms"/> or <expr type="break" label="1s"/> (max 10s).
   A period or ellipsis already creates a pause, so don't put a break beside one. Give the sentence after a break its own fresh expression tag because a break resets delivery to neutral.`)

	parts := []string{expressionPreamble, numberedSections(sections),
		"There is no wrapping prosody marker for this voice — put pace, pitch, and volume in the expression label instead.",
		"Write for the EAR, not the page: no em or en dashes in spoken text. Use a comma, period, or break; rewrite semicolons, mid-sentence colons, and parenthetical asides.",
		"When the conversation is in another language, still write every marker label in English — delivery descriptions and sound names are never translated.",
	}
	if slices.Contains(sounds, "laugh") {
		parts = append(parts, "Laughter belongs only in genuinely playful or celebratory beats, never at a serious moment.")
	}
	if examples := soundExamples(inworldExamples, sounds, inworldSounds); len(examples) != 0 {
		parts = append(parts, "Examples:\n  "+strings.Join(examples, "\n  "))
	}
	return strings.Join(parts, "\n\n")
}

func xaiInstructions(sounds, prosody []string) string {
	sections := make([]string, 0, 4)
	if len(sounds) != 0 {
		sections = append(sections, fmt.Sprintf(`Sounds - a non-verbal vocalization at the exact point where it happens. Self-closing.
   <expr type="sound" label="%s"/>
   Labels are a fixed vocabulary: %s.
   Use non-verbal sounds sparingly and never the same one twice in a row.`, sounds[0], strings.Join(sounds, ", ")))
	}
	sections = append(sections, `Pauses - insert silence when appropriate. Self-closing.
   <expr type="break" label="500ms"/> a brief pause    <expr type="break" label="1s"/> a longer, dramatic pause
   NEVER place a break next to sentence punctuation or an ellipsis; reserve it for a deliberate mid-sentence beat before a key detail.`)
	toneLabels := make([]string, 0, len(prosody))
	for _, label := range prosody {
		if label != "emphasis" {
			toneLabels = append(toneLabels, label)
		}
	}
	sections = append(sections, fmt.Sprintf(`Prosody - wraps a span delivered in a distinct style, to shape HOW it's said.
   <expr type="prosody" label="STYLE">the words it affects</expr>
   Labels are a fixed vocabulary: %s.
   Use one only where the moment clearly calls for it. Never nest prosody markers and always close with </expr>.`, strings.Join(toneLabels, ", ")))
	sections = append(sections, `Emphasis - stresses exactly the ONE word it wraps.
   Are you <expr type="prosody" label="emphasis">sure</expr> you want to do this?
   Wrap a single word, never a phrase or all-caps text. Never nest it and always close with </expr>.`)
	capabilities := "prosody markers, pauses"
	if len(sounds) != 0 {
		capabilities = "prosody markers, sounds, pauses"
	}
	parts := []string{expressionPreamble, numberedSections(sections),
		"This voice has no free-form delivery descriptions — shape delivery entirely through " + capabilities + ", punctuation, and word choice.",
		"Write for the EAR, not the page: no em or en dashes in spoken text. Rewrite semicolons, mid-sentence colons, and parenthetical asides.",
		"When the conversation is in another language, still write every marker label in English — labels are never translated.",
		`Key details deserve care: stress the load-bearing word and wrap a dense span in <expr type="prosody" label="slow">...</expr>. Read codes character by character, spelled out with spaces.`,
	}
	register := "Whisper and soft belong to gentle or conspiratorial beats; loud only to genuinely high-energy ones."
	if containsAny(sounds, "laugh", "chuckle", "giggle") {
		register += " Laughter is RARE and belongs only where something is genuinely funny; friendliness or mild amusement is not a reason, and never laugh at your own lines."
	}
	parts = append(parts, register)
	allowed := append(append([]string(nil), sounds...), prosody...)
	vocabulary := append(append([]string(nil), xaiInline...), xaiWrapping...)
	if examples := soundExamples(xaiExamples, allowed, vocabulary); len(examples) != 0 {
		parts = append(parts, "Examples:\n  "+strings.Join(examples, "\n  "))
	}
	return strings.Join(parts, "\n\n")
}

func fishInstructions(sounds []string, disfluencies bool) string {
	sections := []string{fmt.Sprintf(`Emotion - sets how a sentence sounds. Self-closing; place at the START of a sentence.
   <expr type="expression" label="EMOTION"/>
   Labels are a fixed vocabulary, NOT free-form descriptions: %s.
   Give every sentence its own emotion marker; repeat or switch the label as the feeling changes.`, strings.Join(fishEmotions, ", "))}
	if len(sounds) != 0 {
		sections = append(sections, fmt.Sprintf(`Sounds - a non-verbal sound between sentences. Self-closing.
   <expr type="sound" label="%s"/>
   Labels are a fixed vocabulary: %s.
   Use non-verbal sounds sparingly and never the same one twice in a row.`, sounds[0], strings.Join(sounds, ", ")))
	}
	sections = append(sections, `Pauses - insert silence when appropriate. Self-closing.
   <expr type="break" label="500ms"/> or <expr type="break" label="2s"/>.
   NEVER place a break beside sentence punctuation or an ellipsis; reserve it for a deliberate mid-sentence beat before a key detail.`)
	sections = append(sections, fmt.Sprintf(`Tone - wraps a span delivered in a distinct style.
   <expr type="prosody" label="whispering">don't tell anyone yet.</expr>
   Labels are a fixed vocabulary: %s.
   Use a tone only where clearly called for. Never nest it and always close with </expr>.`, strings.Join(fishTones, ", ")))
	sections = append(sections, `Emphasis - stresses exactly the ONE word it wraps.
   Are you <expr type="prosody" label="emphasis">sure</expr> you want to do this?
   Wrap a single word, never a phrase. Never nest it and always close with </expr>.`)
	parts := []string{expressionPreamble, numberedSections(sections),
		"Write for the EAR, not the page: no em or en dashes in spoken text. Rewrite semicolons, mid-sentence colons, and parenthetical asides.",
		"When the conversation is in another language, still write every marker label in English — labels are never translated.",
	}
	register := `At heavy moments reach for empathetic, sad, regretful, or hopeful — never a bright label like "happy" or "excited" against hard news. Whispering and soft belong to gentle beats; shouting only to genuinely high-energy ones.`
	if containsAny(sounds, "laughing", "chuckling") {
		register += " Laughter belongs only in genuinely playful or celebratory beats, never at a serious moment."
	}
	if disfluencies {
		register += " Save fillers for relaxed moments — never in an emergency or against grave news."
	}
	parts = append(parts, register)
	pool := append([]string(nil), fishExamples...)
	if disfluencies {
		pool = append(pool, fishDisfluentExamples...)
	}
	if examples := soundExamples(pool, sounds, fishSounds); len(examples) != 0 {
		parts = append(parts, "Examples:\n  "+strings.Join(examples, "\n  "))
	}
	return strings.Join(parts, "\n\n")
}

func numberedSections(sections []string) string {
	var out strings.Builder
	for i, section := range sections {
		if i != 0 {
			out.WriteString("\n\n")
		}
		fmt.Fprintf(&out, "%d. %s", i+1, section)
	}
	return out.String()
}

func soundExamples(examples, allowed, vocabulary []string) []string {
	removed := make([]string, 0, len(vocabulary))
	for _, label := range vocabulary {
		if !slices.Contains(allowed, label) {
			removed = append(removed, label)
		}
	}
	out := make([]string, 0, len(examples))
	for _, example := range examples {
		keep := true
		for _, label := range removed {
			if strings.Contains(example, `label="`+label+`"`) {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, example)
		}
	}
	return out
}

func containsAny(values []string, wanted ...string) bool {
	for _, value := range wanted {
		if slices.Contains(values, value) {
			return true
		}
	}
	return false
}
