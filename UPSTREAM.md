# Upstream compatibility baseline

This repository is an independent Go implementation of the public LiveKit
Agents contracts. Compatibility work is source-audited against immutable
upstream revisions so a release can be reproduced even after upstream moves.

## Frozen revisions

| Surface | Revision / version | Purpose |
|---|---|---|
| [`livekit/agents-js`](https://github.com/livekit/agents-js/tree/128f3f6a230616e960325b112067861ce1f1a17f) | `128f3f6a230616e960325b112067861ce1f1a17f`; `@livekit/agents` 1.7.1 | Primary symbol, behavior, default, event, protocol, and test baseline |
| [`livekit/agents`](https://github.com/livekit/agents/tree/cdb37ade6f8e80822e6c5ec4e6de457f2dcaf637) | `cdb37ade6f8e80822e6c5ec4e6de457f2dcaf637`; `livekit-agents` 1.7.1 | Python DX and semantics that are more explicit than TypeScript |
| [`livekit/protocol`](https://pkg.go.dev/github.com/livekit/protocol@v1.50.4) | Go module `v1.50.4` | Worker, job, room, RPC, telemetry, and inference wire types |
| [`livekit/server-sdk-go`](https://pkg.go.dev/github.com/livekit/server-sdk-go/v2@v2.18.1) | Go module `v2.18.1` | Room/API/RTC integration |
| ElevenLabs API | Documentation retrieved 2026-08-28 | STT/TTS HTTP and WebSocket behavior |

The implementation scope is the agents-js core package and its ElevenLabs
plugin. Upstream examples and all other plugins are intentionally excluded.

## Compatibility policy

“Drop-in” means semantic and workflow compatibility where Go permits it:

- public concepts retain their upstream names unless Go convention materially
  improves safety;
- event discriminators, JSON keys, protocol messages, retry classification,
  defaults, time units, and ordering are wire-compatible;
- `context.Context` is the first argument of blocking Go APIs;
- asynchronous iterators become bounded `Recv`, `Send`/`Push`, `Flush`,
  `EndInput`, and `Close` streams with explicit terminal errors;
- JavaScript `undefined | false | value` configuration becomes `Override[T]`,
  preserving inherit/disable/value as three distinct states;
- callback exceptions cannot crash an unrelated media/provider goroutine;
- providers, sessions, streams, rooms, and subprocesses have explicit ownership
  and idempotent shutdown.

Exact import syntax cannot be shared across Go, Python, and TypeScript. The
mapping and migration examples live in `docs/compatibility.md`.

## Deliberate corrections to upstream quirks

The Go SDK does not reproduce a defect merely because a pinned upstream build
contains it. Each correction below retains the intended public behavior:

| Upstream quirk | Go behavior |
|---|---|
| Explicit numeric zero can fall through a JavaScript `||` default. | Valid zero values are represented explicitly; tri-state options distinguish unset from zero/false. |
| Some worker request callbacks are not awaited or do not settle after timeout. | Admission, assignment, and rejection always settle or return a typed context/timeout error. |
| Several async queues are effectively unbounded. | Queues are bounded and cancellation-aware; control events are not silently discarded. |
| TTS fallback polls on a 10 ms timer and can leak listeners. | Fallback and recovery are event-driven, bounded, and unsubscribe deterministically. |
| STT fallback provider health can be mutated concurrently without serialization. | Health and recovery state is synchronized; only one recovery probe runs per provider. |
| ElevenLabs realtime error messages may be logged and swallowed. | Documented provider errors terminate the operation with typed retryability. |
| ElevenLabs MP3 may be advertised while bytes are treated as PCM. | Unsupported/ambiguous encodings are rejected before network I/O unless a real decoder is active. |
| Voice/user callbacks can disrupt an unrelated dispatcher. | Callbacks execute outside internal locks and panic isolation preserves the remaining ordered subscribers. |

## Updating the baseline

An upstream refresh is a compatibility change, not a dependency-only edit. A
maintainer must:

1. pin the new JS and Python commits and package versions here;
2. diff every root/namespace export and ElevenLabs declaration;
3. regenerate the API manifest and cross-language golden fixtures;
4. record changed defaults, errors, discriminators, wire fields, and quirks;
5. run unit, integration, race, vet, fuzz, cross-build, and performance gates;
6. update the compatibility and divergence tables before tagging a release.

