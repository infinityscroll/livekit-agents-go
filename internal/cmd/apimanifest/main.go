// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/infinityscroll/livekit-agents-go/internal/apimanifest"
)

func main() {
	root := flag.String("root", ".", "module root")
	output := flag.String("out", "docs/api-manifest.json", "output path relative to root")
	flag.Parse()
	manifest, err := apimanifest.Generate(*root)
	if err != nil {
		fail(err)
	}
	data, err := apimanifest.Marshal(manifest)
	if err != nil {
		fail(err)
	}
	path := *output
	if !filepath.IsAbs(path) {
		path = filepath.Join(*root, path)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		fail(err)
	}
}

func fail(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
