// SPDX-License-Identifier: Apache-2.0

// Package avatar implements provider-independent LiveKit avatar sessions and
// bounded PCM transports.
//
// The canonical inference flow is:
//
//  1. Construct an InferenceSession.
//  2. Start it before starting voice.AgentSession.
//  3. WaitForJoin until the avatar publishes video.
//  4. Start the voice session and always Close the avatar during shutdown.
//
// Rooms created with voice/roomio should pass roomioadapter.ReadyWaiter to the
// session. The adapter lives in a subpackage so queue/WebSocket avatar plugins
// do not import the raw-media codec stack during process startup.
package avatar
