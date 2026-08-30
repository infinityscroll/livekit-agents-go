// SPDX-License-Identifier: Apache-2.0

// Package tools contains the beta LiveKit agent tools.
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

const DefaultDTMFPublishDelay = 300 * time.Millisecond

type DTMFEvent string

const (
	DTMF0     DTMFEvent = "0"
	DTMF1     DTMFEvent = "1"
	DTMF2     DTMFEvent = "2"
	DTMF3     DTMFEvent = "3"
	DTMF4     DTMFEvent = "4"
	DTMF5     DTMFEvent = "5"
	DTMF6     DTMFEvent = "6"
	DTMF7     DTMFEvent = "7"
	DTMF8     DTMFEvent = "8"
	DTMF9     DTMFEvent = "9"
	DTMFStar  DTMFEvent = "*"
	DTMFPound DTMFEvent = "#"
	DTMFA     DTMFEvent = "A"
	DTMFB     DTMFEvent = "B"
	DTMFC     DTMFEvent = "C"
	DTMFD     DTMFEvent = "D"
)

var DTMFEvents = []DTMFEvent{DTMF1, DTMF2, DTMF3, DTMF4, DTMF5, DTMF6, DTMF7, DTMF8, DTMF9, DTMF0, DTMFStar, DTMFPound, DTMFA, DTMFB, DTMFC, DTMFD}

// DtmfEvent is the deprecated initialism alias retained for agents-js beta
// migrations. Prefer DTMFEvent.
type DtmfEvent = DTMFEvent

func DTMFEventCode(event DTMFEvent) (uint32, error) {
	if len(event) == 1 && event[0] >= '0' && event[0] <= '9' {
		return uint32(event[0] - '0'), nil
	}
	switch event {
	case DTMFStar:
		return 10, nil
	case DTMFPound:
		return 11, nil
	case DTMFA, DTMFB, DTMFC, DTMFD:
		return uint32(event[0]-'A') + 12, nil
	default:
		return 0, fmt.Errorf("invalid DTMF event %q", event)
	}
}

type DTMFPublisher interface {
	PublishDTMF(context.Context, uint32, string) error
}

// LocalParticipantDTMFPublisher adapts server-sdk-go's typed SIP DTMF data
// packet. PublishDataPacket is synchronous but not context-aware; the adapter
// checks cancellation immediately before the call.
type LocalParticipantDTMFPublisher struct{ Participant *lksdk.LocalParticipant }

func RoomDTMFPublisher(room *lksdk.Room) DTMFPublisher {
	if room == nil {
		return LocalParticipantDTMFPublisher{}
	}
	return LocalParticipantDTMFPublisher{Participant: room.LocalParticipant}
}

func AdaptDTMFJobContext[UserData any](job *agents.JobContext[UserData]) DTMFPublisher {
	if job == nil {
		return LocalParticipantDTMFPublisher{}
	}
	return RoomDTMFPublisher(job.Room())
}

func (p LocalParticipantDTMFPublisher) PublishDTMF(ctx context.Context, code uint32, digit string) error {
	if p.Participant == nil {
		return errors.New("room local participant is not available")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return p.Participant.PublishDataPacket(&livekit.SipDTMF{Code: code, Digit: digit}, lksdk.WithDataPublishReliable(true))
}

type SendDTMFOptions struct{ PublishDelay time.Duration }

// SendDTMFEvents publishes events sequentially and preserves agents-js's exact
// human-readable tool output contract. Failures are returned in-band so the LLM
// can report which digit failed.
func SendDTMFEvents(ctx context.Context, publisher DTMFPublisher, events []DTMFEvent, options SendDTMFOptions) string {
	if ctx == nil {
		ctx = context.Background()
	}
	delay := options.PublishDelay
	if delay == 0 {
		delay = DefaultDTMFPublishDelay
	}
	for _, event := range events {
		code, err := DTMFEventCode(event)
		if err == nil {
			if publisher == nil {
				err = errors.New("room local participant is not available")
			} else {
				err = publisher.PublishDTMF(ctx, code, string(event))
			}
		}
		if err == nil && delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				err = context.Cause(ctx)
			}
		}
		if err != nil {
			return fmt.Sprintf("Failed to send DTMF event: %s. Error: %s", event, err)
		}
	}
	values := make([]string, len(events))
	for index, event := range events {
		values[index] = string(event)
	}
	return "Successfully sent DTMF events: " + strings.Join(values, ", ")
}

// SendDtmfEvents is deprecated; prefer SendDTMFEvents.
func SendDtmfEvents(ctx context.Context, publisher DTMFPublisher, events []DtmfEvent, options SendDTMFOptions) string {
	return SendDTMFEvents(ctx, publisher, events, options)
}

type SendDTMFEventsToolOptions struct {
	Publisher        DTMFPublisher
	ResolvePublisher func(llm.ToolOptions) (DTMFPublisher, error)
	PublishDelay     time.Duration
}

type SendDTMFEventsInput struct {
	Events []DTMFEvent `json:"events"`
}

func NewSendDTMFEventsTool(options SendDTMFEventsToolOptions) (*llm.FunctionTool[SendDTMFEventsInput, string], error) {
	if options.PublishDelay < 0 {
		return nil, errors.New("beta/tools: DTMF PublishDelay must not be negative")
	}
	values := make([]string, len(DTMFEvents))
	for index, event := range DTMFEvents {
		values[index] = string(event)
	}
	schema, _ := json.Marshal(map[string]any{
		"type": "object", "properties": map[string]any{"events": map[string]any{
			"type": "array", "items": map[string]any{"type": "string", "enum": values}, "description": "The DTMF events to send.",
		}}, "required": []string{"events"}, "additionalProperties": false,
	})
	return llm.NewTool(llm.FunctionToolOptions[SendDTMFEventsInput, string]{
		Name:        "send_dtmf_events",
		Description: "\nSend a list of DTMF events to the telephony provider.\n\nCall when:\n- User wants to send DTMF events\n",
		Parameters:  schema,
		Validate: func(input *SendDTMFEventsInput) error {
			for _, event := range input.Events {
				if _, err := DTMFEventCode(event); err != nil {
					return err
				}
			}
			return nil
		},
		Execute: func(ctx context.Context, input SendDTMFEventsInput, toolOptions llm.ToolOptions) (string, error) {
			publisher := options.Publisher
			if options.ResolvePublisher != nil {
				var err error
				publisher, err = options.ResolvePublisher(toolOptions)
				if err != nil {
					return failureForFirst(input.Events, err), nil
				}
			}
			if publisher == nil {
				publisher = publisherFromSession(toolOptions)
			}
			return SendDTMFEvents(ctx, publisher, input.Events, SendDTMFOptions{PublishDelay: options.PublishDelay}), nil
		},
	})
}

func MustSendDTMFEventsTool(options SendDTMFEventsToolOptions) *llm.FunctionTool[SendDTMFEventsInput, string] {
	tool, err := NewSendDTMFEventsTool(options)
	if err != nil {
		panic(err)
	}
	return tool
}

// NewSendDtmfEventsTool is deprecated; prefer NewSendDTMFEventsTool.
func NewSendDtmfEventsTool(options SendDTMFEventsToolOptions) (*llm.FunctionTool[SendDTMFEventsInput, string], error) {
	return NewSendDTMFEventsTool(options)
}

func publisherFromSession(options llm.ToolOptions) DTMFPublisher {
	if options.Context == nil {
		return nil
	}
	switch value := options.Context.Session.(type) {
	case DTMFPublisher:
		return value
	case *lksdk.LocalParticipant:
		return LocalParticipantDTMFPublisher{Participant: value}
	case *lksdk.Room:
		if value != nil {
			return LocalParticipantDTMFPublisher{Participant: value.LocalParticipant}
		}
	case interface{ RTCRoom() *lksdk.Room }:
		if room := value.RTCRoom(); room != nil {
			return LocalParticipantDTMFPublisher{Participant: room.LocalParticipant}
		}
	case interface{ Room() *lksdk.Room }:
		if room := value.Room(); room != nil {
			return LocalParticipantDTMFPublisher{Participant: room.LocalParticipant}
		}
	}
	return nil
}

func failureForFirst(events []DTMFEvent, err error) string {
	event := DTMFEvent("")
	if len(events) != 0 {
		event = events[0]
	}
	return fmt.Sprintf("Failed to send DTMF event: %s. Error: %s", event, err)
}
