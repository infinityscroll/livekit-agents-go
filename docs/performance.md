# Performance contract

Low CPU usage and fast process readiness are release requirements, not informal
aspirations. The SDK is designed so importing a package performs no networking,
model loading, goroutine creation, filesystem access, or environment discovery.
Provider registries and expensive tokenizer/inference data are initialized
lazily.

## Design invariants

- All media, model, event, and subprocess queues have explicit capacities.
- Hot audio paths use reusable buffers or allocation-free rings where ownership
  permits it.
- Recovery, transcript synchronization, fallback, and playout are event-driven;
  there are no fixed-interval polling loops.
- A provider package is linked only when imported. ElevenLabs remains a leaf
  package and does not affect a core-only executable.
- Process-per-job isolation is the production default. Prewarm runs before job
  admission; in-process execution is opt-in.
- Connection pools are generation-aware, reject stale sessions lazily, and
  close without detached cleanup goroutines.
- Context cancellation and close barriers bound every long-lived operation.

## Reproducible measurements

Run the full benchmark set on an otherwise idle machine:

```sh
go test -p 1 -run '^$' -bench . -benchmem ./...
```

Run focused hot-path benchmarks with a stable sample count and compare them
with `benchstat`:

```sh
CGO_ENABLED=0 go test -p 1 -run '^$' -bench . -benchmem -count 10 \
  ./inference ./stt ./tts ./tokenize \
  ./voice/backgroundaudio ./voice/recorderio > new.txt
benchstat baseline.txt new.txt
```

For cold-start and resident-memory measurements, build a small application that
imports only the packages used in production, then launch a fresh process for
every sample. Do not benchmark repeated calls inside one process: that measures
warm caches rather than startup. Record hardware, OS, Go version, build flags,
CPU governor/power mode, sample count, and raw results with every release.

## Audited development baseline

The following results were measured on an otherwise idle Apple M5 (10 logical
CPUs, 24 GiB RAM), macOS 26.6.2, `darwin/arm64`, with Go 1.27. They are
implementation evidence, not portable promises; compare only like-for-like
runners and retain the raw samples when updating a release baseline. CI runs a
short benchmark and fresh-process smoke pass; maintainers make regression
decisions from longer samples collected on a controlled runner. Every tag
release permanently attaches its sequential hot-path output, per-process cold
samples, source revision and contract hashes, binary sizes, and peak-RSS
metadata alongside the source archive.

### Cold process start

The three probes in [`benchmarks/coldstart`](../benchmarks/coldstart) retain a
representative public constructor but deliberately do not call it. They
therefore measure Go process startup plus the selected SDK package graph,
without mixing in credentials, networking, model prewarm, or provider latency.
Each probe was built with `CGO_ENABLED=0 go build -trimpath`, warmed 10 times,
then launched as a fresh process 200 times. Peak RSS is one `/usr/bin/time -l`
sample; binary sizes are also shown for a second build with `-ldflags='-s -w'`.

| Deployment graph | Mean | p50 | p95 | p99 | Peak RSS | Binary | Stripped binary |
|---|---:|---:|---:|---:|---:|---:|---:|
| Core worker/server | 6.392 ms | 6.416 ms | 6.781 ms | 6.899 ms | 22.00 MiB | 39.44 MiB | 27.11 MiB |
| Voice session | 6.700 ms | 6.685 ms | 7.128 ms | 8.122 ms | 22.73 MiB | 42.21 MiB | 29.01 MiB |
| ElevenLabs STT + TTS | 5.887 ms | 5.907 ms | 6.199 ms | 6.503 ms | 22.22 MiB | 30.22 MiB | 20.73 MiB |

These are lower-bound import/link-graph probes, not time-to-first-audio. A real
agent's readiness additionally includes the explicitly configured prewarm,
network, room, and model-provider work.

### Focused hot paths

The following are medians of three runs of:

```sh
CGO_ENABLED=0 go test -p 1 -run '^$' -bench . -benchmem -count=3 \
  ./inference ./stt ./tts ./tokenize \
  ./voice/backgroundaudio ./voice/recorderio
```

Bytes and allocations are Go benchmark accounting per operation.

| Operation | Time/op | Bytes/op | Allocs/op |
|---|---:|---:|---:|
| LiveKit Inference STT 50 ms PCM packetization | 1.345 µs | 0 | 0 |
| Interruption probability update | 67.51 ns | 0 | 0 |
| Local turn audio-window update | 3.740 µs | 40,959 | 1 |
| Inference final-transcript decode | 873.6 ns | 1,163 | 10 |
| STT fallback primary recognize | 657.4 ns | 1,241 | 18 |
| 48 kHz→24 kHz PCM fallback resample | 556.5 ns | 1,504 | 2 |
| Sentence tokenization | 54.46 µs | 16,439 | 70 |
| Word tokenization | 3.254 µs | 5,424 | 27 |
| Provider markup conversion | 485.8 ns | 696 | 9 |
| Plain transcript markup strip | 3.405 ns | 0 | 0 |
| Background mixer, immediate two-stream 100 ms block | 5.210 µs | 0 | 0 |
| Background 48 kHz mono converter, hot buffer | 16.25 ns | 64 | 1 |
| Recorder stereo 100 ms mix | 8.348 µs | 20,480 | 1 |
| Recorder mono 100 ms downmix | 2.744 µs | 20,480 | 1 |

The zero-allocation packetizer, interruption update, transcript fast path, and
immediate mixer are explicit regression gates. Other allocation counts include
the returned owned data and, where noted by the benchmark, per-operation state
construction.

## Release gates

A release is blocked by any of the following unless the change is reviewed and
the baseline is intentionally updated:

- a statistically significant regression over 10% in a named hot-path
  benchmark;
- a new allocation in a benchmark documented as zero-allocation;
- an unbounded channel, queue, response body, replay buffer, or retained stream;
- a new package `init` function that performs I/O, starts a goroutine, parses a
  large data file, or reads credentials;
- a polling loop used where a notification or deadline can express the same
  state transition;
- a leaked goroutine/resource in normal, cancellation, timeout, or partial-start
  paths;
- a startup/RSS regression whose source is not documented in release notes.

Race tests, fuzzers, leak-sensitive lifecycle tests, and representative real
subprocess tests run alongside benchmarks because a fast racy path is not an
optimization.
