# Workflow migration (agents-js / agents-python to Go)

This package follows agents-js at commit `128f3f6a230616e960325b112067861ce1f1a17f`.
Blocking operations take `context.Context` first, constructors return validation
errors, and all RTC/SIP ownership is explicit.

## TaskGroup

| agents-js | Go |
| --- | --- |
| `new TaskGroup(options)` | `workflows.NewTaskGroup(options)` |
| `.add(factory, { id, description })` | `.Add(factory, workflows.TaskRegistration{ID: id, Description: description})` |
| `summarizeChatCtx` | `SummarizeChatCtx` (`nil` means the parity default `true`) |
| `returnExceptions` | `ReturnExceptions` |
| `onTaskCompleted(event)` | `OnTaskCompleted(ctx, event)` |
| `task.run()` | `group.Run(ctx)` |

Adapt a typed `*voice.AgentTask[R, State]` with `AdaptAgentTask`. Its optional
runner is the application's foreground-handoff boundary. The current Go voice
core deliberately makes `AgentTask.Run` a wait operation and does not yet
expose `AgentSession.RunTask`; passing a runner avoids hidden session globals.

TaskGroup retains the agents-js sequential order, context merge, generated
`out_of_scope` regression tool, callback/error behavior, and optional
keep-last-turns-zero summary. `MaxExecutions` adds a production safety bound to
malicious or accidental infinite regressions (default 1024).

## Warm transfer

| agents-js | Go |
| --- | --- |
| `createWarmTransferTask(options)` | `workflows.CreateWarmTransferTask(options)` |
| `new WarmTransferTask(options)` | `workflows.NewWarmTransferTask(options)` |
| `abortSignal` | `Run(ctx)` and optional `AbortContext` |
| `sipTrunkId: undefined / null / value` | zero `agents.Override`, `agents.Disable[string]()`, `agents.Use(value)` |
| `holdAudio: undefined / null / value` | zero `agents.Override`, `agents.Disable[WarmTransferHoldAudio]()`, `agents.Use(value)` |
| string speech | `workflows.TextSpeech(text)` |
| speech callback | `workflows.SpeechFunc(callback)` |
| full instruction string | `workflows.FullWarmTransferInstructions(text)` |
| `InstructionParts` | `workflows.PartialWarmTransferInstructions(parts)` |
| `.run()` | `task.Run(ctx)` |
| use as a voice agent task | `task.VoiceTask()` |

`WarmTransferBackend` is the narrow, testable lifecycle contract.
`workflows/livekit.LiveKitWarmTransferBackend` supplies the optional official server-sdk-go implementation
for room tokens, SIP dialing, participant move/removal, RoomIO, cleanup, and
caller I/O restoration. It accepts either a filtered RoomIO `RTCBridge` or an
application-owned bounded caller-disconnect stream.

Three Go SDK boundaries are intentionally explicit:

1. server-sdk-go has no agents-js built-in hold-clip player, so configure
   `HoldFactory` or disable hold audio; silence is never substituted silently;
2. the caller room created by the current `JobContext` does not expose a
   composable participant-disconnect callback, so pass a RoomIO bridge or a
   bounded event stream;
3. an `AgentSession` does not expose its owned model set, so configure
   `ConsultationSessionOptions` (or a `ConsultationFactory`) instead of relying
   on ambient model lookup.

The backend owns only the consultation room. It never disconnects or changes
the caller room's E2EE manager. Use `RoomConnectOptions` to install the matching
E2EE connect option for the consultation leg.

Every exit path is bounded and ordered: stop/close the consultation, delete the
temporary room, stop hold audio, then restore the caller's exact original I/O
flags. A completed participant move wins a concurrent cancellation, matching
agents-js.
