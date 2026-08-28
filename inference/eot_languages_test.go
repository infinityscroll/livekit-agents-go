// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"encoding/json"
	"maps"
	"math"
	"sync"
	"testing"

	agents "github.com/livekit/agents-go"
)

func TestLocalTurnThresholdsExact(t *testing.T) {
	want := map[string]float64{
		"ar": .35, "de": .245, "en": .36, "es": .35, "fr": .285,
		"hi": .305, "id": .345, "it": .23, "ja": .295, "ko": .4,
		"nl": .2, "pt": .32, "tr": .255, "zh": .355,
	}
	if !maps.Equal(LocalTurnThresholds, want) {
		t.Fatalf("local thresholds = %#v, want %#v", LocalTurnThresholds, want)
	}

	// The exported compatibility snapshot cannot corrupt process-wide model
	// calibration.
	original := LocalTurnThresholds["en"]
	LocalTurnThresholds["en"] = .99
	t.Cleanup(func() { LocalTurnThresholds["en"] = original })
	options, err := NewThresholdOptions(TurnDetectorModelV1Mini, ThresholdOverride{}, ThresholdOverride{})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := options.Lookup(agents.AsLanguageCode("en")); !ok || got != .36 {
		t.Fatalf("English threshold = %v, %t", got, ok)
	}
}

func TestThresholdOptionsLayersAndLanguages(t *testing.T) {
	zero := ScalarThreshold(0)
	options, err := NewThresholdOptions(TurnDetectorModelV1Mini, zero, zero)
	if err != nil {
		t.Fatal(err)
	}
	for _, language := range []string{"en-US", "English", "mandarin", "xx"} {
		if got, ok := options.Lookup(agents.AsLanguageCode(language)); !ok || got != 0 {
			t.Errorf("Lookup(%q) = %v, %t; want explicit zero", language, got, ok)
		}
		if !options.Supports(agents.AsLanguageCode(language)) {
			t.Errorf("Supports(%q) = false with scalar override", language)
		}
	}
	if _, ok := options.LookupBackchannel(agents.AsLanguageCode("en")); ok {
		t.Fatal("non-positive backchannel override was not disabled")
	}

	options, err = NewThresholdOptions(
		TurnDetectorModelV1Mini,
		LanguageThresholds(map[string]float64{"English": .12, "zh-CN": .22}),
		ScalarThreshold(.7),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := options.Lookup(agents.AsLanguageCode("en-GB")); got != .12 {
		t.Fatalf("English override = %v", got)
	}
	if got, _ := options.Lookup(agents.AsLanguageCode("Mandarin")); got != .22 {
		t.Fatalf("Mandarin override = %v", got)
	}
	if got, _ := options.Lookup(agents.AsLanguageCode("fr")); got != .285 {
		t.Fatalf("French unlikely threshold = %v", got)
	}
	if got, ok := options.LookupBackchannel(agents.AsLanguageCode("fr-FR")); !ok || got != .7 {
		t.Fatalf("French backchannel threshold = %v, %t", got, ok)
	}
}

func TestCloudThresholdDefaultsAndLocalRescale(t *testing.T) {
	options, err := NewThresholdOptions(
		TurnDetectorModelV1,
		LanguageThresholds(map[string]float64{"en": .4, "fr": .3}),
		ThresholdOverride{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !options.Supports(agents.AsLanguageCode("tl")) {
		t.Fatal("pending cloud thresholds should optimistically support a language")
	}
	if _, ok := options.Lookup(agents.AsLanguageCode("en")); ok {
		t.Fatal("map override resolved before cloud defaults")
	}
	if err := options.UpdateServerDefaults(
		map[string]float32{"en": .8, "fr": .6}, .7,
		map[string]float32{"en": .9}, .85,
	); err != nil {
		t.Fatal(err)
	}
	if got, _ := options.Lookup(agents.AsLanguageCode("fr")); got != .3 {
		t.Fatalf("cloud override = %v", got)
	}
	if got, ok := options.LookupBackchannel(agents.AsLanguageCode("fr")); !ok || got != .85 {
		t.Fatalf("cloud backchannel fallback = %v, %t", got, ok)
	}

	options.ToLocalFallback()
	if options.Model() != TurnDetectorModelV1Mini {
		t.Fatalf("model = %q", options.Model())
	}
	if got, _ := options.Lookup(agents.AsLanguageCode("en")); math.Abs(got-.18) > 1e-12 {
		t.Fatalf("rescaled English = %.8f, want .18", got)
	}
	if got, _ := options.Lookup(agents.AsLanguageCode("fr")); math.Abs(got-.1425) > 1e-12 {
		t.Fatalf("rescaled French = %.8f, want .1425", got)
	}
	if _, ok := options.LookupBackchannel(agents.AsLanguageCode("en")); ok {
		t.Fatal("local fallback retained cloud backchannel defaults")
	}
	if err := options.UpdateServerDefaults(nil, 0, nil, 0); err == nil {
		t.Fatal("degenerate server defaults were accepted")
	}
}

func TestThresholdSnapshotJSONPreservesUnion(t *testing.T) {
	options, err := NewThresholdOptions(TurnDetectorModelV1Mini, ScalarThreshold(.25), LanguageThresholds(map[string]float64{"en": .8}))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(options.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	if got["overrides"] != .25 {
		t.Fatalf("scalar override JSON = %#v", got["overrides"])
	}
	backchannel, ok := got["backchannelOverrides"].(map[string]any)
	if !ok || backchannel["en"] != .8 {
		t.Fatalf("map override JSON = %#v", got["backchannelOverrides"])
	}
}

func TestThresholdOptionsConcurrentUpdateAndLookup(t *testing.T) {
	options, err := NewThresholdOptions(TurnDetectorModelV1Mini, ThresholdOverride{}, ThresholdOverride{})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for i := 0; i < 1_000; i++ {
				if worker%2 == 0 {
					value := float64(i%100) / 100
					if err := options.UpdateOverrides(ScalarThreshold(value)); err != nil {
						t.Errorf("UpdateOverrides: %v", err)
						return
					}
				} else {
					options.Lookup(agents.AsLanguageCode("en-US"))
					options.LookupBackchannel(agents.AsLanguageCode("fr"))
					options.Supports(agents.AsLanguageCode("ja"))
					_ = options.Snapshot()
				}
			}
		}(worker)
	}
	group.Wait()
}
