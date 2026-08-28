// SPDX-License-Identifier: Apache-2.0

package apimanifest

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPublicAPISnapshot(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	manifest, err := Generate(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "docs", "api-manifest.json")
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read public API snapshot: %v (run go run ./internal/cmd/apimanifest -root . -out docs/api-manifest.json)", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("public API snapshot is stale; run go run ./internal/cmd/apimanifest -root . -out docs/api-manifest.json")
	}
}
