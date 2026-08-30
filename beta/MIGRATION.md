# Beta compatibility migration

The root `beta` package and `beta/workflows` retain deprecated aliases for the
pinned agents-js beta exports. New code should import stable workflows directly:

```go
import "github.com/infinityscroll/livekit-agents-go/workflows"
```

## DTMF

- `sendDtmfEvents` maps to the tool returned by
  `tools.NewSendDTMFEventsTool`.
- Direct callers use `tools.SendDTMFEvents(ctx, publisher, events, options)`.
- `DtmfEvent`, `SendDtmfEvents`, and `NewSendDtmfEventsTool` remain deprecated
  spelling aliases; prefer the `DTMF` initialism.
- The default cadence is exactly 300 ms after every published event, including
  the final event. Codes are 0-9, `*`=10, `#`=11, and A-D=12-15. Success and
  failure strings match agents-js.

Go has no async-local job context. Supply a publisher in tool options, resolve
one from the tool run context, or put a compatible Room/RoomIO object in
`llm.RunContext.Session`.

## End call

`createEndCallTool(options)` maps to `tools.CreateEndCallTool(options)`. The Go
toolset keeps the `end_call` ID/name, exact description, default room deletion,
default `"say goodbye to the user"` output, optional ignore-on-enter flag, and
five-second reply bound.

Use zero `EndInstructions` for the default, `agents.Use(text)` for custom text,
and `agents.Disable[string]()` for JavaScript `null`. Adapt a job with
`tools.AdaptEndCallJobContext(job)` so deletion and shutdown remain explicit.

The current Go tool run context does not expose the owning `SpeechHandle`.
Instead, the compatibility tool waits for the bounded session state transition
back to idle/listening (or the reply timeout) before closing. Closing the
toolset cancels this waiter and never closes the session as a side effect.
