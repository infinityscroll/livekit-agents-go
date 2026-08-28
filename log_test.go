// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
)

func TestInitializeLoggerJSONAndRegion(t *testing.T) {
	t.Setenv("LIVEKIT_REGION_NAME", "ca-central")
	var output bytes.Buffer
	logger, err := InitializeLogger(LoggerOptions{Writer: &output, Format: LogFormatJSON, Level: slog.LevelDebug})
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("ready", "worker_id", "W1")
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["msg"] != "ready" || record["worker_id"] != "W1" || record["region"] != "ca-central" {
		t.Fatalf("record = %#v", record)
	}
	options, ok := CurrentLoggerOptions()
	if !ok || options.Level != slog.LevelDebug || options.Format != LogFormatJSON {
		t.Fatalf("options = %#v, %v", options, ok)
	}
}

func TestLoggerConcurrentSwapAndRead(t *testing.T) {
	first := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	second := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if index%8 == 0 {
					if iteration%2 == 0 {
						_ = SetLogger(first)
					} else {
						_ = SetLogger(second)
					}
				} else {
					Log().Debug("test")
				}
			}
		}(index)
	}
	wait.Wait()
}

func TestParseLogLevel(t *testing.T) {
	for value, expected := range map[string]slog.Level{"trace": LogLevelTrace, "debug": slog.LevelDebug, "info": slog.LevelInfo, "warning": slog.LevelWarn, "fatal": slog.LevelError, "silent": LogLevelSilent} {
		actual, err := ParseLogLevel(value)
		if err != nil || actual != expected {
			t.Fatalf("ParseLogLevel(%q) = %v, %v", value, actual, err)
		}
	}
	if _, err := ParseLogLevel("loud"); err == nil {
		t.Fatal("unknown log level accepted")
	}
}
