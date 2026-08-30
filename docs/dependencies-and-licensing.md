# Dependencies, licensing, and release provenance

This document records the dependency and licensing review for the module graph
resolved on 2026-08-31. It is an engineering release control, not legal advice.
The authoritative evidence is each downloaded module's own root `LICENSE`,
`LICENCE`, `COPYING`, or `UNLICENSE` file. SPDX identifiers are not inferred
from repository badges, package-index metadata, or memory.

## Current release status

**The main-module release gate passes.** One exact, reviewed graph exclusion is
required for
[`github.com/gotranspile/g722@v0.0.0-20240123003956-384a1bb16a19`](https://github.com/gotranspile/g722/tree/384a1bb16a19875ec5a7630bf36659b85d5b646f).
That upstream revision has no license file. Its README's nonspecific “BSD
license” statement and separate public-domain attribution do not establish a
specific SPDX license covering the complete revision. The module remains
**unlicensed, unaccepted, and unclassified**; there is no policy exception for
it.

The dependency is introduced by
`github.com/livekit/media-sdk@v0.0.0-20260605212526-4c11a51d3c97`:
its reviewed [`go.mod`](https://github.com/livekit/media-sdk/blob/4c11a51d3c977bccbb1b4805757303cd3d7beb69/go.mod)
requires `github.com/gotranspile/g722`, its optional
[`g722` package](https://github.com/livekit/media-sdk/blob/4c11a51d3c977bccbb1b4805757303cd3d7beb69/g722/g722.go)
imports that implementation, and its convenience
[`all` package](https://github.com/livekit/media-sdk/blob/4c11a51d3c977bccbb1b4805757303cd3d7beb69/all/all.go)
blank-imports the wrapper. This SDK imports neither package. `go mod why -m`
and the full `go list -deps -test ./...` package graph show no path from any SDK
package or test to `github.com/gotranspile/g722`; the CI cross-compilation
matrix supplies complementary build-constraint coverage. The
exact-version `exclude` in this module's `go.mod` therefore removes an otherwise
graph-only dependency.
After `go mod tidy`, it is absent from `go list -m all`, `go.sum`, package
compilation, linked binaries, and the artifacts distributed by this module.

The audit treats this as a strict negative invariant. It requires the reviewed
`exclude` directive verbatim, rejects any selected version of the excluded
module path, and fails if `go list -deps -test ./...` exposes the module or any
of its packages. A future import of either `github.com/gotranspile/g722` or an
upstream package that reaches it cannot silently pass the licensing gate.

This containment is intentionally narrow and is not a license determination.
The durable upstream remedies remain an unambiguous license file covering the
whole module, or a clearly licensed replacement. When either becomes available,
review its actual license evidence before removing or changing the exclusion.

### Important downstream limitation

Go applies `exclude` directives only in the main module; it ignores exclusions
in modules used as dependencies. Consequently, this SDK's exclusion **does not
propagate to consumers**. Go's pruned module graph currently keeps the revision
out of a simple downstream application that only imports this SDK, but that is
not an inherited exclusion or a durable guarantee. Another requirement, graph
expansion, or a G.722-related import can select the unlicensed revision. A
downstream project enforcing the same graph policy must independently verify
package unreachability and add the exact exclusion in its own main `go.mod`, or
wait for an upstream licensing fix. A consumer that imports
`github.com/livekit/media-sdk/g722` or
`github.com/livekit/media-sdk/all` must not use this containment rationale; it
needs a properly licensed implementation. See the official Go documentation
for the [main-module-only semantics of `exclude`](https://go.dev/ref/mod#go-mod-file-exclude).

## Inventory summary

The current `go list -m all` graph contains **195 third-party modules**:
**14 direct** and **181 transitive**. All **195 are classified** from an actual
root license file. The excluded, unlicensed G.722 revision is not counted as a
resolved dependency and is recorded separately in policy. A module can
contribute more than one identifier when its license file covers differently
licensed portions, so the counts below are not mutually exclusive.

| SPDX identifier | Modules containing this license |
| --- | ---: |
| Apache-2.0 | 90 |
| BSD-2-Clause | 6 |
| BSD-3-Clause | 43 |
| CC-BY-SA-4.0 | 1 |
| CC0-1.0 | 1 |
| ISC | 1 |
| MIT | 68 |
| MPL-2.0 | 3 |

The CC-BY-SA-4.0 evidence is `LICENSE.docs` in
`github.com/opencontainers/go-digest`; it applies to that module's documentation.
If that documentation is redistributed or adapted, preserve attribution and
the applicable share-alike terms. Several OpenTelemetry modules contain both
Apache-2.0 and BSD-3-Clause text in one `LICENSE`. The YAML modules contain both
Apache-2.0 and MIT-covered portions. The checker retains these compound
expressions instead of collapsing them to a single headline license.

## Reviewed direct dependencies

Every direct dependency is pinned by module path, version, exact license-file
SHA-256, SPDX expression, and canonical source URL in
[`internal/cmd/depaudit/policy.json`](../internal/cmd/depaudit/policy.json).
Changing any of those fields requires an explicit policy review.

| Module | Version | SPDX expression | Source |
| --- | --- | --- | --- |
| `github.com/google/uuid` | `v1.6.0` | BSD-3-Clause | [source](https://github.com/google/uuid) |
| `github.com/gorilla/websocket` | `v1.5.4-0.20250319132907-e064f32e3674` | BSD-2-Clause | [source](https://github.com/gorilla/websocket) |
| `github.com/livekit/media-sdk` | `v0.0.0-20260605212526-4c11a51d3c97` | Apache-2.0 | [source](https://github.com/livekit/media-sdk) |
| `github.com/livekit/protocol` | `v1.50.4` | Apache-2.0 | [source](https://github.com/livekit/protocol) |
| `github.com/livekit/server-sdk-go/v2` | `v2.18.1` | Apache-2.0 | [source](https://github.com/livekit/server-sdk-go) |
| `github.com/pion/webrtc/v4` | `v4.2.15` | MIT | [source](https://github.com/pion/webrtc) |
| `github.com/twitchtv/twirp` | `v8.1.3+incompatible` | Apache-2.0 | [source](https://github.com/twitchtv/twirp) |
| `go.opentelemetry.io/otel` | `v1.44.0` | Apache-2.0 AND BSD-3-Clause | [source](https://github.com/open-telemetry/opentelemetry-go) |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp` | `v1.44.0` | Apache-2.0 AND BSD-3-Clause | [source](https://github.com/open-telemetry/opentelemetry-go) |
| `go.opentelemetry.io/otel/sdk` | `v1.44.0` | Apache-2.0 AND BSD-3-Clause | [source](https://github.com/open-telemetry/opentelemetry-go) |
| `go.opentelemetry.io/otel/trace` | `v1.44.0` | Apache-2.0 AND BSD-3-Clause | [source](https://github.com/open-telemetry/opentelemetry-go) |
| `go.opentelemetry.io/proto/otlp` | `v1.10.0` | Apache-2.0 | [source](https://github.com/open-telemetry/opentelemetry-proto-go) |
| `google.golang.org/genproto/googleapis/rpc` | `v0.0.0-20260526163538-3dc84a4a5aaa` | Apache-2.0 | [source](https://github.com/googleapis/go-genproto) |
| `google.golang.org/protobuf` | `v1.36.12` | BSD-3-Clause | [source](https://github.com/protocolbuffers/protobuf-go) |

## Native and system dependencies

The Go module graph is not the whole distribution. CGO builds resolve native
libraries through `pkg-config`, and those libraries must also be present in the
runtime or package image when dynamically linked.

| Component | Why it is present | SPDX | Distribution requirement |
| --- | --- | --- | --- |
| [libopus](https://github.com/xiph/opus/blob/main/COPYING) | `gopkg.in/hraban/opus.v2` uses `#cgo pkg-config: opus` for Opus encode/decode. | BSD-3-Clause | Retain its copyright and license notice. Review the royalty-free patent grants and termination terms recorded in upstream `COPYING`. |
| [libopusfile](https://github.com/xiph/opusfile/blob/master/COPYING) | The same wrapper uses `#cgo pkg-config: opusfile` for Ogg/Opus stream reading unless built with `-tags nolibopusfile`. | BSD-3-Clause | Retain its copyright and license notice. |
| [libogg](https://github.com/xiph/ogg/blob/master/COPYING) | Native dependency of libopusfile for the Ogg container. | BSD-3-Clause | Retain its copyright and license notice. |
| [libsoxr](https://github.com/chirlu/soxr/blob/master/LICENCE) | `github.com/livekit/media-sdk/resample_soxr.go` uses `#cgo pkg-config: soxr`. | LGPL-2.1-or-later | Prefer a separately installed or dynamically linked system library. Bundled or static distributions need LGPL source/relinking and license-compliance review. The optional upstream `pffft.c` path has additional terms that must be reviewed if enabled. |

The CI image installs the development packages `libopus-dev`,
`libopusfile-dev`, `libogg-dev`, `libsoxr-dev`, and `pkg-config`. `pkg-config` is
a build tool, not a shipped runtime library. A release container or OS package
must declare the corresponding runtime libraries explicitly and capture their
exact package versions in the release SBOM.

`voice/recorderio` can launch an external FFmpeg executable to create Ogg/Opus
recordings. FFmpeg is not linked into or embedded in this Go module. Its enabled
codecs and license vary by downstream build configuration, so a distributor who
ships FFmpeg must inventory that exact binary separately; this policy cannot
assign it one universal SPDX identifier.

## Embedded Ogg assets

`voice/backgroundaudio/source.go` embeds four `.ogg` files into binaries with
`//go:embed`. They are byte-identical Apache-2.0 resources from
[`livekit/agents-js` 1.7.1 at commit `128f3f6a230616e960325b112067861ce1f1a17f`](https://github.com/livekit/agents-js/tree/128f3f6a230616e960325b112067861ce1f1a17f/agents/resources).
Their immutable attribution is also recorded in
[`voice/backgroundaudio/resources/NOTICE.md`](../voice/backgroundaudio/resources/NOTICE.md).

| Embedded file | SHA-256 |
| --- | --- |
| `hold_music.ogg` | `cfb2a77542ac01011f41593a5fe9b919ee898343bcc66d72499b77b2640def28` |
| `keyboard-typing.ogg` | `56258677554c75019be52f8a16e8494ee5bccfb74c01f490b5a1befd48cbbeae` |
| `keyboard-typing2.ogg` | `1b8244f26ae315561aefdbb09387bf16388c3380d74c2e1f0008be0998169954` |
| `office-ambience.ogg` | `967a445f505e202d3ec03968cff71846284c3b70d4353b3096ce9121d34224c3` |

The audit fails if any embedded byte changes. Adding, replacing, transcoding, or
renaming an asset requires recording its source revision, license, checksum,
and attribution before updating policy.

## Running and reviewing the audit

Run the strict human-readable check from the module root:

```sh
go run ./internal/cmd/depaudit -root .
```

Emit deterministic machine-readable inventory for an SBOM pipeline:

```sh
go run ./internal/cmd/depaudit -root . -format json > dependency-inventory.json
```

The command first runs `go mod download -json all`, verifies the module cache
with `go mod verify`, then runs `go list -mod=readonly -m -json all`,
`go mod edit -json`, and `go list -mod=readonly -deps -test ./...` with
`GOWORK=off`. The latter two commands enforce exact exclusion policy and prove
that excluded package paths remain unreachable. Downloading first makes the
actual license files available rather than trusting registry metadata, and
verification checks extracted module content against the downloaded module
archives. Download and list operations use an isolated copy of `go.mod` and
`go.sum`, so running the audit cannot rewrite the repository's module files.
See
the official Go module reference for [`go mod download`](https://go.dev/ref/mod#go-mod-download)
and [`go list -m`](https://go.dev/ref/mod#go-list-m).

The check fails on:

- a missing or unreadable root license file;
- license text that cannot be mapped to a reviewed SPDX identifier;
- an explicitly disallowed or not-yet-reviewed license;
- a direct dependency being added, removed, or version-changed;
- any release-time `replace` directive, which changes source provenance;
- an unreviewed or missing graph exclusion, a selected excluded module path, or
  any package reachable from an excluded path;
- a changed direct-dependency license-file set or SHA-256; or
- a missing or changed embedded asset.

When intentionally updating a dependency, inspect the new module archive and
all root license/notice files, review native or generated-code changes, update
the direct-dependency source URL and exact hashes, regenerate the JSON
inventory, and include that evidence in the change review. Never make a red
audit green by assigning an SPDX identifier that is not supported by the
downloaded license text.

## NOTICE, SBOM, and provenance obligations

The repository's [`NOTICE`](../NOTICE) identifies upstream-derived API design
and embedded assets. It is not a substitute for third-party license texts. For
each release artifact:

- preserve every copyright, license, and NOTICE text required by the modules,
  assets, and native libraries actually distributed;
- include a third-party notices bundle with source/binary distributions when
  the applicable licenses require reproduction of those notices;
- include the four embedded-asset attribution records because those bytes are
  inside the Go binary;
- capture native package names and exact versions from the build image, not only
  the Go graph; and
- keep any separately shipped FFmpeg binary in its own component inventory.

The JSON output is evidence for, but not by itself, a complete SBOM. Convert or
merge it into a standard such as the current [SPDX specification](https://spdx.dev/use/specifications/),
and supplement it with the packages actually linked into each `GOOS`/`GOARCH`/
CGO artifact. Attach the SBOM, source revision, Go toolchain version, build
flags/tags, `go.sum`, module-proxy/checksum settings, native package manifest,
artifact digest, and builder identity to signed build provenance. The
[SLSA provenance model](https://slsa.dev/spec/v1.2/provenance) describes this as
verifiable information connecting an artifact to where, when, and how it was
produced.

Because `go list -m all` is a resolution-time module graph rather than a precise
linked-code bill of materials, validate each final binary as well (for example,
with `go version -m`) and reconcile it with the graph-level audit. Both views
matter: the graph gate catches dependency-policy drift, while artifact-level
evidence describes what a customer actually receives.
