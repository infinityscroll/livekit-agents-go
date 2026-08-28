// SPDX-License-Identifier: Apache-2.0

// Package livekit binds a voice.AgentSession to the current job's LiveKit
// room, recording, observability, report, and primary-session lifecycle.
//
// Start is the one-step production path corresponding to AgentSession.start in
// the TypeScript and Python SDKs. The lower-level voice, roomio, recorderio,
// and telemetry packages remain available when an application needs explicit
// ownership of individual components.
package livekit
