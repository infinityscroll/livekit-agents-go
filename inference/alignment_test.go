// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"errors"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/tts"
)

func TestDecodeTTSAlignmentProviderDialects(t *testing.T) {
	event := TTSServerEvent{
		Words: []TTSWordTimestamp{{Word: "hello", Start: .1, End: .4}},
		Chars: []TTSCharTimestamp{{Char: "ignored", Start: 1, End: 2}},
	}
	got := DecodeTTSAlignment(event, tts.ProviderCartesia)
	if len(got) != 1 || got[0].Text != "hello " || got[0].StartTime == nil || *got[0].StartTime != 100*time.Millisecond || got[0].EndTime == nil || *got[0].EndTime != 400*time.Millisecond {
		t.Fatalf("Cartesia alignment = %+v", got)
	}
	got = DecodeTTSAlignment(TTSServerEvent{Chars: []TTSCharTimestamp{{Char: "x", Start: .25, End: .5}}}, "elevenlabs")
	if len(got) != 1 || got[0].Text != "x" || *got[0].StartTime != 250*time.Millisecond || *got[0].EndTime != 500*time.Millisecond {
		t.Fatalf("character alignment = %+v", got)
	}
}

func TestTTSAlignmentDecoderSeam(t *testing.T) {
	want := errors.New("private alignment dialect failed")
	decoder := TTSAlignmentDecoderFunc(func(ctx context.Context, _ TTSServerEvent, provider string) ([]agents.TimedString, error) {
		if ctx == nil || provider != "private" {
			t.Fatalf("decoder args: ctx=%v provider=%q", ctx, provider)
		}
		return nil, want
	})
	_, err := decoder.DecodeTTSAlignment(context.Background(), TTSServerEvent{}, "private")
	if !errors.Is(err, want) {
		t.Fatalf("decoder error = %v", err)
	}
	if DefaultTTSAlignmentDecoder() == nil {
		t.Fatal("default decoder is nil")
	}
}
