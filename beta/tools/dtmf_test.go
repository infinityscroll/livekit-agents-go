// SPDX-License-Identifier: Apache-2.0

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/llm"
)

type dtmfTestPublisher struct {
	values [][2]any
	failAt int
}

func (p *dtmfTestPublisher) PublishDTMF(_ context.Context, code uint32, digit string) error {
	p.values = append(p.values, [2]any{code, digit})
	if p.failAt > 0 && len(p.values) == p.failAt {
		return errors.New("publish failed")
	}
	return nil
}

func TestDTMFEventCodes(t *testing.T) {
	want := map[DTMFEvent]uint32{DTMF0: 0, DTMF1: 1, DTMF9: 9, DTMFStar: 10, DTMFPound: 11, DTMFA: 12, DTMFB: 13, DTMFC: 14, DTMFD: 15}
	for event, code := range want {
		got, err := DTMFEventCode(event)
		if err != nil || got != code {
			t.Fatalf("event=%q code=%d err=%v", event, got, err)
		}
	}
	if _, err := DTMFEventCode("x"); err == nil {
		t.Fatal("invalid event accepted")
	}
}

func TestSendDTMFEventsSequentialOutputAndFailure(t *testing.T) {
	publisher := &dtmfTestPublisher{}
	result := SendDTMFEvents(t.Context(), publisher, []DTMFEvent{DTMF1, DTMF2, DTMFPound}, SendDTMFOptions{PublishDelay: time.Nanosecond})
	if result != "Successfully sent DTMF events: 1, 2, #" {
		t.Fatalf("result=%q", result)
	}
	want := [][2]any{{uint32(1), "1"}, {uint32(2), "2"}, {uint32(11), "#"}}
	if !reflect.DeepEqual(publisher.values, want) {
		t.Fatalf("published=%#v", publisher.values)
	}

	failing := &dtmfTestPublisher{failAt: 2}
	result = SendDTMFEvents(t.Context(), failing, []DTMFEvent{DTMF1, DTMF2, DTMF3}, SendDTMFOptions{PublishDelay: time.Nanosecond})
	if result != "Failed to send DTMF event: 2. Error: publish failed" || len(failing.values) != 2 {
		t.Fatalf("result=%q values=%v", result, failing.values)
	}
}

func TestSendDTMFEventsToolContract(t *testing.T) {
	publisher := &dtmfTestPublisher{}
	tool, err := NewSendDTMFEventsTool(SendDTMFEventsToolOptions{Publisher: publisher, PublishDelay: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	if tool.Name() != "send_dtmf_events" || tool.Flags() != llm.ToolFlagNone {
		t.Fatalf("tool=%s flags=%d", tool.Name(), tool.Flags())
	}
	value, err := tool.Execute(t.Context(), json.RawMessage(`{"events":["A","D"]}`), llm.ToolOptions{})
	if err != nil || value != "Successfully sent DTMF events: A, D" {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if !reflect.DeepEqual(publisher.values, [][2]any{{uint32(12), "A"}, {uint32(15), "D"}}) {
		t.Fatalf("published=%v", publisher.values)
	}
	if _, err = tool.Execute(t.Context(), json.RawMessage(`{"events":["x"]}`), llm.ToolOptions{}); err == nil {
		t.Fatal("schema-invalid event accepted")
	}
}
