# Checked performance baselines

Each directory records evidence from one named host/toolchain. Baselines are
not portable promises and must never be compared across different machines,
operating systems, power modes, or Go versions.

The `apple-m5-darwin-arm64-go1.27` baseline measures source commit
`ca6bbcc5de5e51d8d6aca82114e98b0a9c464a65`. Its files contain:

- `hot-path.txt`: five sequential (`go test -p 1`) samples for every focused
  benchmark, including allocation counts;
- `cold-*.json`: 200 fresh-process samples in capture order, after 10 warmups;
- `metadata.txt`: source-contract hashes, exact build flags, OS/CPU/toolchain,
  binary sizes and hashes, and raw peak-RSS output.

This capture predates the public repository move to
`github.com/infinityscroll/livekit-agents-go`. Its package paths, source
contract hashes, and binary hashes remain byte-for-byte historical evidence;
they are not text-rewritten to resemble a measurement that did not occur.
Tag releases generate the same artifact shapes from the exact tagged revision
on their runner and attach them permanently to the release.

Validate the cold files with `jq`, and use `benchstat` on compatible
`hot-path.txt` results. A release baseline changes only after an explained,
reviewed performance decision; a faster result collected under a different
environment is not a valid replacement.
