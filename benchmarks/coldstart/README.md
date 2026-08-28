# Cold-start probes

These deliberately tiny programs measure the cost of importing the core,
voice, or ElevenLabs package graph. Go's linker retains only the selected graph,
so each binary is a realistic lower bound for that deployment shape.

Build without the compiler cache influencing process measurements:

```sh
mkdir -p .bench
CGO_ENABLED=0 go build -trimpath -o .bench/core ./benchmarks/coldstart/core
CGO_ENABLED=0 go build -trimpath -o .bench/voice ./benchmarks/coldstart/voice
CGO_ENABLED=0 go build -trimpath -o .bench/elevenlabs ./benchmarks/coldstart/elevenlabs
CGO_ENABLED=0 go build -trimpath -o .bench/measure ./benchmarks/coldstart/measure

CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o .bench/core.stripped ./benchmarks/coldstart/core
CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o .bench/voice.stripped ./benchmarks/coldstart/voice
CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o .bench/elevenlabs.stripped ./benchmarks/coldstart/elevenlabs
```

Measure fresh processes (the warmups affect only filesystem caches):

```sh
.bench/measure -binary "$PWD/.bench/core" -samples 200 -warmups 10
.bench/measure -binary "$PWD/.bench/voice" -samples 200 -warmups 10
.bench/measure -binary "$PWD/.bench/elevenlabs" -samples 200 -warmups 10
```

Pass `-json` to retain every sample in nanoseconds for a checked baseline:

```sh
.bench/measure -json -binary "$PWD/.bench/core" -samples 200 -warmups 10 > cold-core.json
```

For peak RSS, run one probe through the platform's `time` executable. On
macOS use `/usr/bin/time -l`; on Linux use `/usr/bin/time -v`. Record the OS,
hardware, power mode, Go version, build flags, and raw samples with published
results. Do not compare results collected on different hosts. See the
[`../baselines`](../baselines) directory for the checked release evidence.
