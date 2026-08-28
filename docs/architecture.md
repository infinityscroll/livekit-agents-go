# Architecture and ownership

The SDK is split so applications pay only for the features they import:

```text
agents (worker/job/process)
├── rtcbridge (construction-time bounded Room callback fan-out)
├── llm / stt / tts / vad / stream / tokenize
├── inference (LiveKit gateway adapters)
├── voice (session, generation, recognition, tools)
│   ├── voice/roomio (RTC/media adapters; cgo/native audio)
│   ├── voice/livekit (job-owned one-step session lifecycle)
│   ├── voice/recorderio (bounded lazy Ogg/Opus recording)
│   └── voice/backgroundaudio (lazy bounded mixer)
├── telemetry (OTel and session upload)
├── workflows / beta
└── plugins/elevenlabs
```

There is no import-time plugin scan or provider initialization. Core model
interfaces know nothing about ElevenLabs. Voice depends on model interfaces,
not concrete providers. Room I/O is isolated because it carries native media
dependencies that text-only and worker-only agents do not need.

## Concurrency model

Each long-lived component owns a context and one small set of supervised
goroutines. Inputs and outputs are bounded channels or ring buffers. Producers
either backpressure, perform an explicitly documented realtime drop policy, or
surface overflow; they never grow an unbounded slice behind an iterator.

State mutation happens under narrow locks. Network I/O, user callbacks,
provider callbacks, tool execution, and closing child resources happen outside
those locks. Event ordering is preserved by one dispatcher; callback panics are
isolated so later subscribers still receive the event.

## Lifecycle and ownership

Construction is side-effect free except for validating/copying configuration.
`Start` may allocate supervised resources. `Close` is idempotent and waits for
owned goroutines/resources up to the caller's context deadline. Partial startup
rolls back in reverse ownership order.

Caller-supplied rooms and providers remain caller-owned. Adapters created from
model strings are owned by the component that created them. A fallback adapter
owns its children by default but exposes `KeepProvidersOpen` when instances are
shared. Room I/O never closes a caller-owned room or changes E2EE ownership.

## Job isolation

Production jobs run in separate processes by default. Parent/child IPC is
authenticated, loopback-only, framed, bounded, and cancellation-aware. The
parent supervises prewarm, initialization, ping/pong health, memory limits,
shutdown, and forced termination. In-process execution exists for low-latency
development and deterministic tests, and must be selected explicitly.

## Data boundaries

Mutable chat items, audio frames, protocol payloads, event metadata, and room
callbacks are copied when they cross asynchronous ownership boundaries. JSON,
protobuf, WebSocket, multipart, and OTLP decoders enforce size limits and reject
ambiguous formats. Secrets are kept out of URLs/userinfo and structured logs.
