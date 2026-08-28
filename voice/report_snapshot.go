// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"
)

// FrozenReportEvent is an immutable, ownership-safe event snapshot for a
// SessionReport. It retains only the canonical report wire form, so live model,
// speech, audio, and tool objects cannot be retained accidentally by a
// long-running session recorder.
type FrozenReportEvent struct {
	kind      EventType
	createdAt time.Time
	raw       json.RawMessage
}

// FreezeReportEvent converts an event to the canonical, snake_case report wire
// contract and severs every reference to its source object graph. Metrics and
// usage events are valid inputs even though SessionReportToJSON intentionally
// omits them for cross-SDK parity.
func FreezeReportEvent(event Event) (*FrozenReportEvent, error) {
	if event == nil || isNilInterface(event) {
		return nil, errors.New("voice report event is nil")
	}
	wire, err := eventToReportJSON(event)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	return &FrozenReportEvent{
		kind: event.Type(), createdAt: event.Time(), raw: append(json.RawMessage(nil), raw...),
	}, nil
}

func (e *FrozenReportEvent) Type() EventType {
	if e == nil {
		return ""
	}
	return e.kind
}

func (e *FrozenReportEvent) Time() time.Time {
	if e == nil {
		return time.Time{}
	}
	return e.createdAt
}

// EncodedSize is the exact number of retained canonical JSON bytes.
func (e *FrozenReportEvent) EncodedSize() int {
	if e == nil {
		return 0
	}
	return len(e.raw)
}

func (e *FrozenReportEvent) MarshalJSON() ([]byte, error) {
	if e == nil || len(e.raw) == 0 {
		return nil, errors.New("voice frozen report event is empty")
	}
	return append([]byte(nil), e.raw...), nil
}

func (e *FrozenReportEvent) reportJSON() (map[string]any, error) {
	if e == nil || len(e.raw) == 0 {
		return nil, errors.New("voice frozen report event is empty")
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewReader(e.raw))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("voice frozen report event is not an object")
	}
	return result, nil
}
