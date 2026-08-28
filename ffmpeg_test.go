// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"path/filepath"
	"testing"
)

func TestResolveFFmpegPathPrefersEnvironment(t *testing.T) {
	want := filepath.Join(t.TempDir(), "custom-ffmpeg")
	t.Setenv(FFmpegPathEnvironment, "  "+want+"  ")
	if got := ResolveFFmpegPath(); got != want {
		t.Fatalf("ResolveFFmpegPath=%q want %q", got, want)
	}
	if got := ConfigureFFmpeg(); got != want {
		t.Fatalf("ConfigureFFmpeg=%q want %q", got, want)
	}
	command := FFmpegCommand(context.Background(), "-version")
	if command.Path != want {
		t.Fatalf("command path=%q want %q", command.Path, want)
	}
}
