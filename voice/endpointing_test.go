// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"testing"
	"time"
)

func timestamp(milliseconds int) time.Time { return time.UnixMilli(int64(milliseconds)) }

func TestDynamicEndpointingLearnsPauses(t *testing.T) {
	endpointing, err := NewDynamicEndpointing(EndpointingOptions{Mode: EndpointingDynamic, MinDelay: 300 * time.Millisecond, MaxDelay: time.Second, Alpha: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	endpointing.OnEndOfSpeech(timestamp(100_000), false)
	endpointing.OnStartOfSpeech(timestamp(100_400), false)
	endpointing.OnEndOfSpeech(timestamp(100_500), false)
	if got := endpointing.MinDelay(); got != 350*time.Millisecond {
		t.Fatalf("learned delay = %s", got)
	}
}

func TestDynamicEndpointingUpdatePreservesLearnedValueForAlpha(t *testing.T) {
	endpointing, _ := NewDynamicEndpointing(EndpointingOptions{Mode: EndpointingDynamic, MinDelay: 300 * time.Millisecond, MaxDelay: time.Second, Alpha: 0.5})
	endpointing.OnEndOfSpeech(timestamp(100_000), false)
	endpointing.OnStartOfSpeech(timestamp(101_000), false)
	endpointing.OnEndOfSpeech(timestamp(101_500), false)
	if got := endpointing.MinDelay(); got != 650*time.Millisecond {
		t.Fatalf("learned = %s", got)
	}
	alpha := 0.2
	if err := endpointing.Update(EndpointingUpdate{Alpha: &alpha}); err != nil {
		t.Fatal(err)
	}
	if got := endpointing.MinDelay(); got != 650*time.Millisecond {
		t.Fatalf("update reset learned value: %s", got)
	}
	endpointing.OnStartOfSpeech(timestamp(102_500), false)
	endpointing.OnEndOfSpeech(timestamp(103_000), false)
	if got := endpointing.MinDelay(); got != 930*time.Millisecond {
		t.Fatalf("updated alpha learned = %s", got)
	}
}

func TestDynamicEndpointingIgnoredOverlap(t *testing.T) {
	endpointing, _ := NewDynamicEndpointing(EndpointingOptions{Mode: EndpointingDynamic, MinDelay: 300 * time.Millisecond, MaxDelay: time.Second, Alpha: 0.5})
	endpointing.OnEndOfSpeech(timestamp(100_000), false)
	endpointing.OnStartOfAgentSpeech(timestamp(100_500))
	endpointing.OnStartOfSpeech(timestamp(101_500), true)
	endpointing.OnEndOfSpeech(timestamp(101_800), true)
	if got := endpointing.MinDelay(); got != 300*time.Millisecond {
		t.Fatalf("ignored overlap changed delay: %s", got)
	}
	if endpointing.Overlapping() {
		t.Fatal("overlap was not cleared")
	}
}

func TestEndpointingFactoryAndValidation(t *testing.T) {
	fixed, err := NewEndpointing(DefaultEndpointingOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fixed.(*FixedEndpointing); !ok {
		t.Fatalf("factory returned %T", fixed)
	}
	if _, err := NewEndpointing(EndpointingOptions{Mode: "unknown"}); err == nil {
		t.Fatal("expected unknown-mode error")
	}
}

func TestEndpointingUpdateIsAtomicOnValidationFailure(t *testing.T) {
	endpointing, err := NewDynamicEndpointing(EndpointingOptions{
		Mode: EndpointingDynamic, MinDelay: 300 * time.Millisecond,
		MaxDelay: time.Second, Alpha: 0.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	minDelay := 600 * time.Millisecond
	invalidAlpha := 2.0
	if err := endpointing.Update(EndpointingUpdate{MinDelay: &minDelay, Alpha: &invalidAlpha}); err == nil {
		t.Fatal("expected invalid-alpha error")
	}
	if got := endpointing.MinDelay(); got != 300*time.Millisecond {
		t.Fatalf("partially-applied min delay = %s", got)
	}
}
