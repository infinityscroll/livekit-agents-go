// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"unicode"

	"github.com/livekit/agents-go/rtcbridge"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

var ErrRTCBridgeRequired = errors.New("agents: a construction-time RTCBridge is required for event-driven room waits")

// ParticipantNotFoundError reports a participant that was absent before an
// attribute wait could be armed.
type ParticipantNotFoundError struct{ Identity string }

func (e *ParticipantNotFoundError) Error() string {
	return fmt.Sprintf("agents: participant %q is not in the room", e.Identity)
}

// ParticipantWaitDisconnectedError reports a requested participant or the
// room disappearing before a wait condition was satisfied.
type ParticipantWaitDisconnectedError struct {
	Identity  string
	Attribute string
	Room      bool
}

func (e *ParticipantWaitDisconnectedError) Error() string {
	if e.Room {
		if e.Attribute == "" {
			return "agents: room disconnected while waiting for track publication"
		}
		return fmt.Sprintf("agents: room disconnected while waiting for participant %q attribute %q", e.Identity, e.Attribute)
	}
	if e.Attribute == "" {
		return fmt.Sprintf("agents: participant %q disconnected while waiting for track publication", e.Identity)
	}
	return fmt.Sprintf("agents: participant %q disconnected while waiting for attribute %q", e.Identity, e.Attribute)
}

// Dedent strips a common leading run of spaces/tabs from non-empty lines,
// removes one initial newline, and trims trailing Unicode whitespace. It is the
// Go string equivalent of the agents-js dedent tagged template helper.
func Dedent(value string) string {
	value = strings.TrimPrefix(value, "\n")
	lines := strings.Split(value, "\n")
	minimum := -1
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := 0
		for indent < len(line) && (line[indent] == ' ' || line[indent] == '\t') {
			indent++
		}
		if minimum < 0 || indent < minimum {
			minimum = indent
		}
	}
	if minimum > 0 {
		for index, line := range lines {
			if len(line) >= minimum {
				lines[index] = line[minimum:]
			} else {
				// JavaScript's String.slice returns an empty string when the
				// requested offset is beyond the end. A shorter line can only be
				// whitespace here because non-empty lines contributed to minimum.
				lines[index] = ""
			}
		}
	}
	return strings.TrimRightFunc(strings.Join(lines, "\n"), unicode.IsSpace)
}

// IsCloud reports whether rawURL is hosted on a LiveKit Cloud production or
// run domain. Malformed and hostname-less URLs return false.
func IsCloud(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	hostname := strings.ToLower(parsed.Hostname())
	return strings.HasSuffix(hostname, ".livekit.cloud") || strings.HasSuffix(hostname, ".livekit.run")
}

// IsDevMode reports whether the process was launched in a LiveKit development
// mode such as dev, connect, or console.
func IsDevMode() bool { return os.Getenv("LIVEKIT_DEV_MODE") == "1" }

// IsHosted reports whether the worker is hosted by LiveKit Cloud. Presence is
// significant even when the environment variable is explicitly empty.
func IsHosted() bool {
	_, present := os.LookupEnv("LIVEKIT_REMOTE_EOT_URL")
	return present
}

// WaitForParticipantAttribute waits without polling until a remote
// participant's attribute equals value. bridge must be the callback bridge
// installed when room was constructed; server-sdk-go v2.18.1 has no safe
// post-construction callback registration API.
func WaitForParticipantAttribute(
	ctx context.Context,
	room *lksdk.Room,
	bridge *rtcbridge.RTCBridge,
	identity, attribute, value string,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if room == nil || room.ConnectionState() != lksdk.ConnectionStateConnected {
		return ErrRoomNotConnected
	}
	if bridge == nil {
		return ErrRTCBridgeRequired
	}
	if room.GetParticipantByIdentity(identity) == nil {
		return &ParticipantNotFoundError{Identity: identity}
	}
	subscription, err := bridge.SubscribeTypes(8,
		rtcbridge.RTCEventParticipantAttributesChanged,
		rtcbridge.RTCEventParticipantDisconnected,
		rtcbridge.RTCEventDisconnected,
	)
	if err != nil {
		return err
	}
	defer subscription.Close()
	participant := room.GetParticipantByIdentity(identity)
	if participant == nil {
		return &ParticipantNotFoundError{Identity: identity}
	}
	if participant.Attributes()[attribute] == value {
		return nil
	}
	if room.ConnectionState() != lksdk.ConnectionStateConnected {
		return &ParticipantWaitDisconnectedError{Identity: identity, Attribute: attribute, Room: true}
	}
	for {
		event, err := subscription.Recv(ctx)
		if err != nil {
			return err
		}
		switch event.Type {
		case rtcbridge.RTCEventParticipantAttributesChanged:
			if event.Participant != nil && event.Participant.Identity() == identity && event.Participant.Attributes()[attribute] == value {
				return nil
			}
		case rtcbridge.RTCEventParticipantDisconnected:
			if event.Participant != nil && event.Participant.Identity() == identity {
				return &ParticipantWaitDisconnectedError{Identity: identity, Attribute: attribute}
			}
		case rtcbridge.RTCEventDisconnected:
			return &ParticipantWaitDisconnectedError{Identity: identity, Attribute: attribute, Room: true}
		}
	}
}

// WaitForTrackPublicationOptions matches the TypeScript/Python helper. Empty
// Identity and Kind values mean any participant and any kind respectively.
// Local publications resolve when published and ignore WaitForSubscription.
type WaitForTrackPublicationOptions struct {
	Identity            string
	Kind                lksdk.TrackKind
	IncludeLocal        bool
	WaitForSubscription bool
	EventCapacity       int
}

// WaitForTrackPublication waits without polling for a matching local or remote
// publication and returns the SDK-owned publication handle.
func WaitForTrackPublication(
	ctx context.Context,
	room *lksdk.Room,
	bridge *rtcbridge.RTCBridge,
	options WaitForTrackPublicationOptions,
) (lksdk.TrackPublication, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if room == nil || room.ConnectionState() != lksdk.ConnectionStateConnected {
		return nil, ErrRoomNotConnected
	}
	if bridge == nil {
		return nil, ErrRTCBridgeRequired
	}
	capacity := options.EventCapacity
	if capacity <= 0 {
		capacity = 16
	}
	events := []rtcbridge.RTCEventType{
		rtcbridge.RTCEventParticipantDisconnected,
		rtcbridge.RTCEventDisconnected,
	}
	if options.WaitForSubscription {
		events = append(events, rtcbridge.RTCEventTrackSubscribed)
	} else {
		events = append(events, rtcbridge.RTCEventTrackPublished)
	}
	if options.IncludeLocal {
		events = append(events, rtcbridge.RTCEventLocalTrackPublished)
	}
	subscription, err := bridge.SubscribeTypes(capacity, events...)
	if err != nil {
		return nil, err
	}
	defer subscription.Close()
	if publication := findTrackPublication(room, options); publication != nil {
		return publication, nil
	}
	if room.ConnectionState() != lksdk.ConnectionStateConnected {
		return nil, &ParticipantWaitDisconnectedError{Identity: options.Identity, Room: true}
	}
	for {
		event, err := subscription.Recv(ctx)
		if err != nil {
			return nil, err
		}
		switch event.Type {
		case rtcbridge.RTCEventTrackPublished, rtcbridge.RTCEventTrackSubscribed:
			if remotePublicationMatches(event.Publication, event.Participant, options) {
				return event.Publication, nil
			}
		case rtcbridge.RTCEventLocalTrackPublished:
			if localPublicationMatches(event.LocalPublication, room.LocalParticipant, options) {
				return event.LocalPublication, nil
			}
		case rtcbridge.RTCEventParticipantDisconnected:
			if options.Identity != "" && event.Participant != nil && event.Participant.Identity() == options.Identity {
				return nil, &ParticipantWaitDisconnectedError{Identity: options.Identity}
			}
		case rtcbridge.RTCEventDisconnected:
			return nil, &ParticipantWaitDisconnectedError{Identity: options.Identity, Room: true}
		}
	}
}

func findTrackPublication(room *lksdk.Room, options WaitForTrackPublicationOptions) lksdk.TrackPublication {
	if options.IncludeLocal && room.LocalParticipant != nil && (options.Identity == "" || room.LocalParticipant.Identity() == options.Identity) {
		for _, publication := range room.LocalParticipant.TrackPublications() {
			if localPublicationMatches(publication, room.LocalParticipant, options) {
				return publication
			}
		}
	}
	for _, participant := range room.GetRemoteParticipants() {
		for _, publication := range participant.TrackPublications() {
			remote, ok := publication.(*lksdk.RemoteTrackPublication)
			if ok && remotePublicationMatches(remote, participant, options) {
				return remote
			}
		}
	}
	return nil
}

func remotePublicationMatches(publication *lksdk.RemoteTrackPublication, participant *lksdk.RemoteParticipant, options WaitForTrackPublicationOptions) bool {
	if publication == nil || participant == nil || options.Identity != "" && participant.Identity() != options.Identity ||
		options.Kind != "" && publication.Kind() != options.Kind {
		return false
	}
	return !options.WaitForSubscription || publication.IsSubscribed() && publication.Track() != nil
}

func localPublicationMatches(publication lksdk.TrackPublication, participant *lksdk.LocalParticipant, options WaitForTrackPublicationOptions) bool {
	return publication != nil && participant != nil && (options.Identity == "" || participant.Identity() == options.Identity) &&
		(options.Kind == "" || publication.Kind() == options.Kind)
}

// WaitForParticipantAttribute uses the JobContext's construction-time bridge.
func (j *JobContext[T]) WaitForParticipantAttribute(ctx context.Context, identity, attribute, value string) error {
	if j == nil {
		return errors.New("agents: nil job context")
	}
	return WaitForParticipantAttribute(ctx, j.room, j.rtcBridge, identity, attribute, value)
}

// WaitForTrackPublication uses the JobContext's construction-time bridge.
func (j *JobContext[T]) WaitForTrackPublication(ctx context.Context, options WaitForTrackPublicationOptions) (lksdk.TrackPublication, error) {
	if j == nil {
		return nil, errors.New("agents: nil job context")
	}
	return WaitForTrackPublication(ctx, j.room, j.rtcBridge, options)
}
