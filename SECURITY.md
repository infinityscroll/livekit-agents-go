# Security policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Send a private report
through the repository's GitHub Security Advisory page and include the affected
version, impact, reproduction, and any suggested mitigation. Maintainers should
acknowledge a complete report within three business days and provide a
remediation/status update within seven business days.

## Supported versions

Security fixes are applied to the current minor release line. Until a stable
`v1` tag exists, only the latest tagged pre-1.0 release is supported.

## Security boundaries

- Worker WebSocket, IPC, inference, provider, RPC/data, and telemetry inputs are
  untrusted and must remain size-bounded.
- API keys and JWTs must be supplied through explicit options or environment
  variables and must not appear in URLs, errors, traces, or logs.
- Caller callbacks and tools are application code; the SDK isolates panics but
  cannot sandbox their filesystem, network, or process access.
- Process-per-job mode provides fault and memory isolation, not a hostile-code
  security sandbox.
- Room media security and E2EE depend on the pinned LiveKit RTC SDK and the
  application's key provider.

Every release runs tests with the race detector and `go vet`, scans reachable
dependency code with `govulncheck`, and publishes an SBOM/provenance artifact.

