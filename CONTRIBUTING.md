# Contributing

Use the Go version selected by `go.mod`. Install Ogg, Opus, Opusfile, SoXR, and
`pkg-config` before testing the native RoomIO/background-audio packages.

Before submitting a change:

```sh
go fmt ./...
go mod tidy
go test ./...
go test -race ./...
go vet ./...
go install honnef.co/go/tools/cmd/staticcheck@v0.8.1
staticcheck -tests=false ./...
```

Run the focused fuzzers and benchmarks for any parser, resampler, queue, or hot
media path you change. Add deterministic cancellation, timeout, partial-start,
concurrent-close, and saturation tests for long-lived components.

Public API changes must regenerate the checked-in contract with
`go run ./internal/cmd/apimanifest -root . -out docs/api-manifest.json` and
update `docs/compatibility.md`, Go examples, and the upstream mapping. An
upstream refresh must follow the checklist in `UPSTREAM.md`. Do not add provider
dependencies to the core packages or perform I/O, environment discovery, model
loading, or goroutine startup in `init`.
