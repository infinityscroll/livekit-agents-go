// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"errors"
	"testing"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"
)

func TestRTCBridgeComposesCallbacksAndIsolatesPanic(t *testing.T) {
	t.Parallel()
	base := lksdk.NewRoomCallback()
	called := 0
	base.OnParticipantConnected = func(*lksdk.RemoteParticipant) {
		called++
		panic("boom")
	}
	panics := make(chan error, 1)
	bridge := NewRTCBridge(base, RTCBridgeOptions{OnCallbackPanic: func(err error) { panics <- err }})
	t.Cleanup(func() { _ = bridge.Close() })
	sub, err := bridge.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}

	bridge.Callback().OnParticipantConnected(nil)
	if called != 1 {
		t.Fatalf("base callback called %d times", called)
	}
	select {
	case err := <-panics:
		var panicErr *CallbackPanicError
		if !errors.As(err, &panicErr) || panicErr.Panic != "boom" || len(panicErr.Stack) == 0 {
			t.Fatalf("unexpected panic report: %#v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("callback panic was not reported")
	}
	event, err := sub.Recv(context.Background())
	if err != nil || event.Type != RTCEventParticipantConnected {
		t.Fatalf("event = %#v, %v", event, err)
	}
}

func TestRTCBridgeOverflowIsVisible(t *testing.T) {
	t.Parallel()
	bridge := NewRTCBridge(nil, RTCBridgeOptions{})
	t.Cleanup(func() { _ = bridge.Close() })
	sub, err := bridge.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	bridge.Callback().OnReconnecting()
	bridge.Callback().OnReconnected()
	first, err := sub.Recv(context.Background())
	if err != nil || first.Type != RTCEventReconnecting {
		t.Fatalf("first event = %#v, %v", first, err)
	}
	if _, err := sub.Recv(context.Background()); !errors.Is(err, ErrRTCEventOverflow) {
		t.Fatalf("overflow error = %v", err)
	}
}

func TestRTCBridgeClonesUserData(t *testing.T) {
	t.Parallel()
	bridge := NewRTCBridge(nil, RTCBridgeOptions{})
	t.Cleanup(func() { _ = bridge.Close() })
	sub, _ := bridge.Subscribe(1)
	payload := []byte{1, 2, 3}
	bridge.Callback().OnDataPacket(&lksdk.UserDataPacket{Payload: payload, Topic: "topic"}, lksdk.DataReceiveParams{})
	payload[0] = 9
	event, err := sub.Recv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	packet, ok := event.DataPacket.(*lksdk.UserDataPacket)
	if !ok || packet.Payload[0] != 1 || packet.Topic != "topic" {
		t.Fatalf("cloned packet = %#v", event.DataPacket)
	}
}

func TestRTCBridgeIsolatesLegacyDisconnectPanic(t *testing.T) {
	t.Parallel()
	base := lksdk.NewRoomCallback()
	base.OnDisconnected = func() { panic("legacy disconnect") }
	panics := make(chan error, 1)
	bridge := NewRTCBridge(base, RTCBridgeOptions{OnCallbackPanic: func(err error) { panics <- err }})
	sub, err := bridge.SubscribeTypes(1, RTCEventDisconnected)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// server-sdk-go invokes these in this order. The first panic must not stop
	// the reason callback (or the SDK's cleanup that follows it).
	bridge.Callback().OnDisconnected()
	bridge.Callback().OnDisconnectedWithReason(lksdk.RoomClosed)
	select {
	case err := <-panics:
		var panicErr *CallbackPanicError
		if !errors.As(err, &panicErr) || panicErr.Callback != "RTC" || len(panicErr.Stack) == 0 {
			t.Fatalf("panic report = %#v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy callback panic was not reported")
	}
	event, err := sub.Recv(context.Background())
	if err != nil || event.Type != RTCEventDisconnected {
		t.Fatalf("disconnect event = %#v, %v", event, err)
	}
}

func TestRTCBridgeTypeFilterIgnoresDataFlood(t *testing.T) {
	t.Parallel()
	bridge := NewRTCBridge(nil, RTCBridgeOptions{})
	sub, err := bridge.SubscribeTypes(1, RTCEventReconnected)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for index := 0; index < DefaultRTCEventCapacity*4; index++ {
		bridge.Callback().OnDataPacket(&lksdk.UserDataPacket{Payload: []byte{byte(index)}}, lksdk.DataReceiveParams{})
	}
	bridge.Callback().OnReconnected()
	event, err := sub.Recv(context.Background())
	if err != nil || event.Type != RTCEventReconnected {
		t.Fatalf("filtered event = %#v, %v", event, err)
	}
}
