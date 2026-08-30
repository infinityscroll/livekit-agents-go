# TypeScript/Python compatibility

This module is a semantic Go implementation of `@livekit/agents` and
`livekit-agents` 1.7.1. The immutable source revisions are recorded in
[`UPSTREAM.md`](../UPSTREAM.md). Upstream examples and plugins are outside the
scope, except for ElevenLabs.

Go cannot share import syntax, promises, structural types, or async iterators
with TypeScript and Python. “Drop-in” therefore means that an application keeps
the same architecture, lifecycle, defaults, events, wire formats, and provider
behavior while translating those language mechanisms into normal Go.

## Package map

| TypeScript / Python | Go |
|---|---|
| `@livekit/agents` / `livekit.agents` | `github.com/infinityscroll/livekit-agents-go` |
| `llm` | `github.com/infinityscroll/livekit-agents-go/llm` |
| `stt` | `github.com/infinityscroll/livekit-agents-go/stt` |
| `tts` | `github.com/infinityscroll/livekit-agents-go/tts` |
| `vad` | `github.com/infinityscroll/livekit-agents-go/vad` |
| `inference` | `github.com/infinityscroll/livekit-agents-go/inference` |
| `voice` | `github.com/infinityscroll/livekit-agents-go/voice` |
| `AgentSession.start` production binding | `github.com/infinityscroll/livekit-agents-go/voice/livekit.Start` |
| `voice.RoomIO` | `github.com/infinityscroll/livekit-agents-go/voice/roomio` |
| `voice.RecorderIO` | `github.com/infinityscroll/livekit-agents-go/voice/recorderio` |
| `voice.BackgroundAudioPlayer` | `github.com/infinityscroll/livekit-agents-go/voice/backgroundaudio` |
| `tokenize` | `github.com/infinityscroll/livekit-agents-go/tokenize` |
| `telemetry` | `github.com/infinityscroll/livekit-agents-go/telemetry` |
| `plugins.elevenlabs` | `github.com/infinityscroll/livekit-agents-go/plugins/elevenlabs` |
| beta tools/workflows | `github.com/infinityscroll/livekit-agents-go/beta/...` |

## Mechanical translation rules

| Upstream idiom | Go equivalent |
|---|---|
| `await operation(...)` | `operation(ctx, ...)` with `context.Context` first |
| `AsyncIterable<T>` / `ReadableStream<T>` | bounded `Recv(ctx)`, `Send`/`Push`, `Flush`, `EndInput`, and `Close` |
| `undefined \| false \| T` | zero-valued `agents.Override[T]`, `agents.Disable[T]()`, or `agents.Use(value)` |
| event emitter callback | typed subscription returning an unsubscribe function |
| `async dispose` / `aclose()` | idempotent `Close(ctx)` with an explicit deadline |
| object option updates | pointer fields for sparse updates; zero values remain usable |
| thrown provider error | typed error return with retryability and provider metadata |
| dynamically typed tool schema | generic `llm.NewTool` plus JSON Schema |

Streams are deliberately bounded. A slow consumer blocks or receives a visible
overflow/terminal error instead of growing the heap without limit. Provider and
media callbacks execute outside internal locks and are isolated from panics.

## Worker migration

TypeScript:

```ts
cli.runApp(new WorkerOptions({
  agent: resolve(import.meta.url),
  entrypoint: async (ctx) => {
    await ctx.connect();
  },
}));
```

Go:

```go
err := agents.Run(context.Background(), agents.WorkerOptions[struct{}]{
    Entrypoint: func(ctx context.Context, job *agents.JobContext[struct{}]) error {
        return job.Connect(ctx, agents.ConnectOptions{})
    },
})
```

The Go worker keeps registration, availability, assignment, migration,
termination, drain, reconnect, health, load, memory-limit, and process-per-job
semantics. Process isolation is the production default; in-process execution is
an explicit development/test choice.

## Voice-agent migration

The same three objects remain central: an `Agent`, an `AgentSession`, and a room
I/O adapter. Model objects can be supplied directly or selected through LiveKit
Inference model strings. Session methods retain the upstream names (`Start`,
`Say`, `GenerateReply`, `Interrupt`, `UpdateAgent`, and `Close`) and return a
`SpeechHandle` where upstream returns one.

Go makes ownership explicit:

- a caller-supplied LLM, STT, TTS, VAD, room, or realtime model stays
  caller-owned unless the option says otherwise;
- models constructed from inference strings are closed by their owning session
  or adapter;
- every long-lived session, stream, subscription, and subprocess has an
  idempotent close path;
- handoffs prepare the next activity before draining the old one, so a failed
  handoff rolls back without losing the running agent.

Realtime LLMs and cascaded LLM→TTS agents share the same session surface. Go
uses explicit tri-state overrides to distinguish “inherit”, “disabled”, and “a
specific model” during handoffs.

For the normal job-owned path, use `voice/livekit.Start`. It automatically uses
`job.Room()` and `job.RTCBridge()`, connects idempotently, installs RoomIO,
starts recording/Cloud telemetry according to the dispatch and sparse session
override, and registers the primary session for close/report finalization before
the room disconnects. This is the closest mechanical replacement for upstream
`session.start({ room: ctx.room, ... })`. Direct `session.Start` remains the
lower-level path for caller-owned media/telemetry.

Session report events are converted immediately to immutable canonical wire
snapshots and retained under explicit count and byte ceilings. When a ceiling
is reached the oldest snapshot is evicted, `Runtime.ReportStats` reports the
loss, and the final close event is retained. This is an intentional Go safety
improvement over upstream unbounded event retention.

## Chat and tools

`llm.ChatContext` supports messages, tool calls/results, handoffs,
configuration updates, copying, truncation, merge, exact upstream JSON, and
provider-format conversion. Mutating methods are synchronized. Unlike the JS
implementation, copies deep-copy mutable payloads so one goroutine cannot
silently mutate another request snapshot.

Tools use a typed input parameter and JSON Schema:

```go
tool, err := llm.NewTool(llm.FunctionToolOptions[WeatherInput, Weather]{
    Name:       "weather",
    Parameters: schema,
    Execute: func(ctx context.Context, input WeatherInput, options llm.ToolOptions) (Weather, error) {
        return lookup(ctx, input)
    },
})
```

Duplicate calls, progressive tool execution, handoff outputs, cancellation,
non-blocking tools, error templates, and ordered chat commits retain upstream
semantics. Tool and user callbacks never run while a framework mutex is held.

## STT, TTS, VAD, and inference

- Base interfaces retain labels, providers, models, capabilities, metrics, and
  error events.
- Batch-only STT and TTS implementations can be adapted to streams without
  changing the session.
- Fallback adapters are ordered, bounded, event-driven, and expose provider
  availability. They never retry visible TTS audio unless explicitly enabled.
- LiveKit Inference STT/TTS/LLM, EOT, interruption, VAD, and alignment wire
  messages match the pinned gateway contracts.
- Session keyterms preserve static terms across handoffs and apply confirmed
  background-detected terms only when an STT advertises support.

## ElevenLabs

The ElevenLabs package supplies batch and realtime STT, HTTP and WebSocket TTS,
alignment, voice/settings models, pronunciation dictionaries, sparse option
updates, connection pooling, provider errors, and plugin registration. Queues,
reconnect replay, WebSocket contexts, and completed-stream retention are
bounded. Unsupported or ambiguous encodings fail before network I/O instead of
being interpreted as PCM.

## Events, metrics, and telemetry

Event discriminators and JSON field names match upstream. Ordered event buses
support bounded subscribers and a barrier used to guarantee the final `close`
event before dispatcher shutdown. Metrics retain request identifiers, model
metadata, token/audio durations, time-to-first-token/byte, and interruption
state.

Telemetry supports caller-owned OpenTelemetry providers, the LiveKit Cloud
OTLP/HTTP exporter, dynamic metadata processors, additional span processors,
session uploads, recording-disabled gating, and a bounded `slog` bridge with
trace/span correlation. Merely importing the SDK does not install a global
tracer or logger.

## Intentional Go safety differences

The implementation does not reproduce known upstream defects:

- valid explicit zero values are not replaced by truthiness defaults;
- admission/assignment operations settle on timeout instead of only logging;
- async queues are bounded;
- fallback recovery does not poll;
- provider error frames are not swallowed;
- mutable chat, audio, and callback payloads are copied at concurrency
  boundaries;
- shutdown reports deadline failures rather than abandoning goroutines.

## Native RTC constraints

`voice/roomio` uses the pinned LiveKit Go RTC/media SDK and therefore requires
the platform's Opus and SoXR development libraries when cgo is enabled. A room
callback bridge must be installed when constructing the room because the
current Go server SDK does not allow callback replacement after construction.
Every `JobContext.Room()` already carries one and exposes it through
`JobContext.RTCBridge()`; `roomio.NewRoom` does the same for caller-created
rooms.

The pinned Go RTC SDK has no raw-video publication/consumption surface matching
the Node/Python SDKs. Audio, text/transcription, data, RPC, E2EE hooks, playback
accounting, reconnection, and participant selection are implemented. Raw-video
support cannot be emulated safely without a lower-level RTC dependency and is
reported as a capability gap instead of silently no-oping.

The complete, versioned boundary ledger—including local model artifacts,
native build requirements, RTC byte-stream ownership, remote transport
security, and statistics—is in [`capability-gaps.md`](capability-gaps.md).
