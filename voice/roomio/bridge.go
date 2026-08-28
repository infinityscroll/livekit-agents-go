// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"runtime/debug"

	"github.com/livekit/agents-go/rtcbridge"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

const DefaultRTCEventCapacity = rtcbridge.DefaultRTCEventCapacity

var (
	ErrRTCBridgeClosed  = rtcbridge.ErrRTCBridgeClosed
	ErrRTCEventOverflow = rtcbridge.ErrRTCEventOverflow
)

type CallbackPanicError = rtcbridge.CallbackPanicError
type RTCEventType = rtcbridge.RTCEventType

const (
	RTCEventParticipantConnected    = rtcbridge.RTCEventParticipantConnected
	RTCEventParticipantDisconnected = rtcbridge.RTCEventParticipantDisconnected
	RTCEventTrackPublished          = rtcbridge.RTCEventTrackPublished
	RTCEventTrackUnpublished        = rtcbridge.RTCEventTrackUnpublished
	RTCEventTrackSubscribed         = rtcbridge.RTCEventTrackSubscribed
	RTCEventTrackUnsubscribed       = rtcbridge.RTCEventTrackUnsubscribed
	RTCEventLocalTrackPublished     = rtcbridge.RTCEventLocalTrackPublished
	RTCEventLocalTrackUnpublished   = rtcbridge.RTCEventLocalTrackUnpublished
	RTCEventLocalTrackSubscribed    = rtcbridge.RTCEventLocalTrackSubscribed
	RTCEventReconnecting            = rtcbridge.RTCEventReconnecting
	RTCEventReconnected             = rtcbridge.RTCEventReconnected
	RTCEventDisconnected            = rtcbridge.RTCEventDisconnected
	RTCEventDataPacket              = rtcbridge.RTCEventDataPacket
	RTCEventTranscription           = rtcbridge.RTCEventTranscription
)

type RTCEvent = rtcbridge.RTCEvent
type RTCBridgeOptions = rtcbridge.RTCBridgeOptions
type RTCSubscription = rtcbridge.RTCSubscription
type RTCBridge = rtcbridge.RTCBridge

func NewRTCBridge(base *lksdk.RoomCallback, options RTCBridgeOptions) *RTCBridge {
	return rtcbridge.NewRTCBridge(base, options)
}

func NewRoom(base *lksdk.RoomCallback, options RTCBridgeOptions) (*lksdk.Room, *RTCBridge) {
	return rtcbridge.NewRoom(base, options)
}

// RoomIO callbacks use the same panic representation as RTC callbacks while
// remaining local to the roomio package.
func invokeCallback(name string, callback func()) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newCallbackPanicError(name, recovered)
		}
	}()
	callback()
	return nil
}

func invokeErrorCallback(name string, callback func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newCallbackPanicError(name, recovered)
		}
	}()
	return callback()
}

func newCallbackPanicError(name string, recovered any) *CallbackPanicError {
	return &CallbackPanicError{Callback: name, Panic: recovered, Stack: debug.Stack()}
}
