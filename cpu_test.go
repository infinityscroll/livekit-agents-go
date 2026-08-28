// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeCPUMonitorAndEnvironmentOverride(t *testing.T) {
	t.Setenv(CPUCountEnvironment, "3.5")
	monitor := NewRuntimeCPUMonitor()
	if got := monitor.CPUCount(); got != 3.5 {
		t.Fatalf("CPUCount=%v", got)
	}
	value, err := monitor.CPUPercent(context.Background(), time.Millisecond)
	if err != nil || value < 0 || value > 1 {
		t.Fatalf("CPUPercent=%v err=%v", value, err)
	}
	t.Setenv(CPUCountEnvironment, "not-a-number")
	if got := monitor.CPUCount(); got < 1 {
		t.Fatalf("invalid env should fall back, got %v", got)
	}
}

func TestCGroupV2CPUCountUsageAndCancellation(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "cpu.max"), []byte("200000 100000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stat := filepath.Join(root, "cpu.stat")
	if err := os.WriteFile(stat, []byte("usage_usec 1000000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	monitor := NewCGroupV2CPUMonitor(root)
	if got := monitor.CPUCount(); got != 2 {
		t.Fatalf("CPUCount=%v", got)
	}
	go func() {
		time.Sleep(5 * time.Millisecond)
		_ = os.WriteFile(stat, []byte("usage_usec 1010000\n"), 0o600)
	}()
	value, err := monitor.CPUPercent(context.Background(), 20*time.Millisecond)
	if err != nil || value <= 0 || value > 1 {
		t.Fatalf("CPUPercent=%v err=%v", value, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := monitor.CPUPercent(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestCGroupV2MaxAndMalformedUsage(t *testing.T) {
	root := t.TempDir()
	_ = os.WriteFile(filepath.Join(root, "cpu.max"), []byte("max 100000\n"), 0o600)
	_ = os.WriteFile(filepath.Join(root, "cpu.stat"), []byte("user_usec 10\n"), 0o600)
	monitor := NewCGroupV2CPUMonitor(root)
	if monitor.CPUCount() < 1 {
		t.Fatal("max quota did not fall back")
	}
	if _, err := monitor.usage(); !errors.Is(err, ErrCPUUsageUnavailable) {
		t.Fatalf("expected unavailable error, got %v", err)
	}
}

func TestCGroupV1CPUCountUsage(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "cpu"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "cpuacct"), 0o700); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(root, "cpu", "cpu.cfs_quota_us"), []byte("50000"), 0o600)
	_ = os.WriteFile(filepath.Join(root, "cpu", "cpu.cfs_period_us"), []byte("100000"), 0o600)
	usage := filepath.Join(root, "cpuacct", "cpuacct.usage")
	_ = os.WriteFile(usage, []byte("1000000000"), 0o600)
	monitor := NewCGroupV1CPUMonitor(root)
	if got := monitor.CPUCount(); got != 0.5 {
		t.Fatalf("CPUCount=%v", got)
	}
	go func() {
		time.Sleep(5 * time.Millisecond)
		_ = os.WriteFile(usage, []byte("1010000000"), 0o600)
	}()
	value, err := monitor.CPUPercent(context.Background(), 20*time.Millisecond)
	if err != nil || value <= 0 || value > 1 {
		t.Fatalf("CPUPercent=%v err=%v", value, err)
	}

	missing := NewCGroupV1CPUMonitor(t.TempDir())
	if got := missing.CPUCount(); got != DefaultCGroupV1CPUCount {
		t.Fatalf("fallback count=%v", got)
	}
}

func TestCPUPercentHelpers(t *testing.T) {
	if got := percentBetween(cpuSnapshot{busy: 1, total: 2}, cpuSnapshot{busy: 2, total: 4}); got != 0.5 {
		t.Fatalf("percent=%v", got)
	}
	if got := percentBetween(cpuSnapshot{busy: 2, total: 2}, cpuSnapshot{busy: 10, total: 3}); got != 1 {
		t.Fatalf("clamped percent=%v", got)
	}
	if _, err := NewRuntimeCPUMonitor().CPUPercent(context.Background(), 0); err == nil {
		t.Fatal("zero interval accepted")
	}
}
