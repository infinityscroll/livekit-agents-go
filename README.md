# LiveKit Agents for Go

Production-oriented Go SDK for realtime, programmable LiveKit agents. Its public
surface follows `@livekit/agents` and `livekit-agents`, expressed with idiomatic
Go contexts, interfaces, typed options, and explicit error returns.

The compatibility baseline is:

- `livekit/agents-js` commit `128f3f6a230616e960325b112067861ce1f1a17f`
  (`@livekit/agents` 1.7.1)
- `livekit/agents` commit `cdb37ade6f8e80822e6c5ec4e6de457f2dcaf637`
- LiveKit protocol 1.50.4
- LiveKit Go server/RTC SDK 2.18.1

The core package intentionally avoids provider SDKs and eager initialization.
Providers are isolated in subpackages so unused integrations do not affect
binary size or process startup. The only bundled provider is ElevenLabs.

```sh
go get github.com/infinityscroll/livekit-agents-go@v0.1.0
```

## Production voice agent

```go
package main

import (
    "context"
    "os"

    agents "github.com/infinityscroll/livekit-agents-go"
    "github.com/infinityscroll/livekit-agents-go/agentscli"
    "github.com/infinityscroll/livekit-agents-go/llm"
    "github.com/infinityscroll/livekit-agents-go/voice"
    voicekit "github.com/infinityscroll/livekit-agents-go/voice/livekit"
)

func main() {
    options := agents.WorkerOptions[struct{}]{
        JobEntrypoint: func(ctx context.Context, job *agents.JobContext[struct{}]) error {
            session, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{
                ParentContext: job.Context(),
                STTModel:      "deepgram/nova-3:en",
                LLMModel:      "openai/gpt-4.1-mini",
                TTSModel:      "cartesia/sonic-3:9626c31c-bec5-4cca-baa8-f8ba9e84c8bc",
            })
            if err != nil {
                return err
            }
            agent, err := voice.NewAgent(voice.AgentOptions[struct{}]{
                ID: "assistant",
                Instructions: llm.NewInstructions(
                    "You are a concise, helpful voice assistant.",
                    "You are a concise, helpful text assistant.",
                ),
            })
            if err != nil {
                return err
            }
            _, err = voicekit.Start(ctx, session, agent, voicekit.StartOptions[struct{}]{
                Job: job,
            })
            return err
        },
    }
    os.Exit(agentscli.Main(options, os.Args[1:]))
}
```

`voicekit.Start` is the one-step equivalent of the Python/TypeScript session
start path: it connects the room, installs RoomIO, claims the primary session,
resolves inherited recording/redaction, configures bounded report capture and
LiveKit Cloud observability, starts RecorderIO when requested, and registers
ordered shutdown/report finalization with the job. Partial startup rolls back
in reverse order. Secondary sessions are returned to the caller for explicit
closure and cannot accidentally claim primary recording.

Set `LIVEKIT_URL`, `LIVEKIT_API_KEY`, and `LIVEKIT_API_SECRET`, then build and
run `your-agent dev` locally or `your-agent start` in production. The embedded
CLI also supports `connect`, `console [--record]`, `download-files`, `version`,
and `help`, matching the upstream workflow. Applications embedding their own
command layer can call `agents.Run(ctx, options)` directly. The normal
production executor uses a process per job; select `ExecutorModeInProcess` only
when shared-process behavior is intentional.

## Installation and native audio

The worker, model, telemetry, ElevenLabs, and voice-session packages are normal
Go modules. RTC RoomIO additionally uses cgo through the pinned LiveKit media
stack:

```sh
# Debian / Ubuntu
sudo apt-get install pkg-config libogg-dev libopus-dev libopusfile-dev libsoxr-dev

# macOS (Homebrew)
brew install pkg-config libogg opus opusfile soxr
```

Text-only/core consumers can keep RoomIO out of their import graph. See
`docs/capability-gaps.md` for the exact native and raw-video boundaries.

## What is included

- process-isolated worker/job lifecycle, health, reconnect, drain, memory and
  authenticated bounded IPC;
- LiveKit Inference LLM/STT/TTS, turn detection, VAD, adaptive interruption,
  alignment and avatar gateway adapters;
- cascaded and realtime voice sessions, RoomIO, recorder, background audio,
  remote/console sessions, AMD, tools, handoffs, workflows and telemetry;
- STT/TTS streaming and fallback adapters, tokenization/transcription, metrics,
  provider-format conversion, and the ElevenLabs plugin;
- machine-checked API and cross-language parity manifests, dependency/license
  policy, fuzz/race/static-analysis gates, cold-start probes and benchmarks.

See `docs/compatibility.md` and `docs/parity-manifest.json` for the exact
TypeScript/Python-to-Go mapping, `docs/performance.md` for reproducible startup,
CPU, allocation, and memory measurements, and `docs/capability-gaps.md` for the
explicit boundaries imposed by the pinned Go RTC/native stack.

## Supported Go

The module targets Go 1.26 or newer, matching the current LiveKit Go RTC SDK.
CI also builds with the current stable Go toolchain.

## License

Apache-2.0.
