# Capability boundaries

The SDK fails explicitly when the pinned Go RTC/runtime stack cannot provide a
safe equivalent. It does not silently accept a configuration and discard data.
These boundaries are part of the `v0.1.0` compatibility contract.

## Raw video

[`server-sdk-go` v2.18.1](https://pkg.go.dev/github.com/livekit/server-sdk-go/v2@v2.18.1)
does not expose the raw-video publication/consumption and AV-synchronizer
surface used by the Node and Python agent runtimes. `voice/roomio` therefore
returns `roomio.ErrUnsupportedRawVideo`, and remote-session video update fields
are decoded but reported as unsupported. Audio, text, transcription, data,
RPC, E2EE hooks, participant selection, reconnect, and playout accounting are
implemented.

## Local inference models

The core module contains no eager native model and performs no model loading at
import time. `agents.RegisterInferenceRunner` is the supported extension point;
workers share registered runners across jobs, and the built-in VAD/EOT clients
discover that executor through the job/session context. LiveKit Cloud
transports remain available when no local runner is registered.

The SDK defines the stateful `lk_vad` runner protocol but does not bundle a
Silero model or ONNX Runtime shared library. Current Go Silero integrations
require a native ONNX runtime and platform-specific model artifact. Shipping an
energy threshold under the Silero name would be behaviorally incorrect, so an
unavailable local runner is visible and the documented fallback path is used.

Runner factories used with process-isolated workers must be linked and
registered before worker startup so the same executable registers them in the
supervised inference child. Dynamic runtime closures can instead use explicit
in-process executor mode.

## RTC callback and stream ownership

The pinned Go Room stores its callback privately and has no post-construction
callback registration method. Rooms created by `JobContext` already carry the
bounded construction-time bridge (`job.RTCBridge()`); for caller-created rooms,
use `roomio.NewRoom` or install an `RTCBridge` while constructing the Room.
Attaching RoomIO to an arbitrary already-created Room cannot recover events
that happened before the bridge existed.

The Go RTC SDK's byte-stream reader buffers internally and exposes neither a
cancel method nor a context-aware read. This SDK bounds declared sizes, its own
queues, and all downstream copies, but it cannot stop SDK-owned growth when a
malicious sender lies about an unknown stream length. Byte-stream writes and
close also have no provider delivery result. These are upstream ownership
limits, not converted into detached timeout goroutines.

## Native audio builds

`voice/roomio` uses the pinned LiveKit media SDK. Native builds require cgo,
`pkg-config`, Ogg, Opus, Opusfile, and SoXR development libraries. Portable core, model,
ElevenLabs, voice-session, avatar queue I/O, background-audio, recorder, CLI,
and workflow packages compile with `CGO_ENABLED=0`; RoomIO and its optional
avatar/warm-transfer adapters require the native toolchain.

## Remote transport security and statistics

The raw TCP remote-session transport is intentionally a framing transport, not
an authentication protocol. Bind it to loopback or place it inside an
authenticated TLS tunnel. Room RPC/data transport uses LiveKit's authenticated
room connection. The pinned Go RTC SDK has no public RTC statistics getter, so
remote statistics responses are empty rather than fabricated.

## Language-level compatibility

Go cannot share TypeScript/Python import syntax, promises, structural types, or
async iterators. Drop-in compatibility means the same architecture, lifecycle,
defaults, event and wire shapes, retry/ordering behavior, and provider options,
translated to `context.Context`, typed errors, interfaces, generics, and bounded
streams. The exact mechanical mappings are in
[`compatibility.md`](compatibility.md), and every exported Go declaration is
frozen in [`api-manifest.json`](api-manifest.json).
