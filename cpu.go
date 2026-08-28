// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"strconv"
	"strings"
	"time"
)

const (
	CPUCountEnvironment      = "NUM_CPUS"
	DefaultCPUSampleInterval = 500 * time.Millisecond
	DefaultCGroupV1CPUCount  = 2.0
)

var ErrCPUUsageUnavailable = errors.New("CPU usage is unavailable")

// CPUMonitor reports normalized load in [0,1] and the effective CPU quota.
// CPUPercent uses no helper goroutine and always honors ctx cancellation.
type CPUMonitor interface {
	CPUCount() float64
	CPUPercent(context.Context, time.Duration) (float64, error)
}

// GetCPUMonitor selects Linux cgroup accounting when available, then falls
// back to Go runtime CPU-class metrics. Detection is explicit and lazy.
func GetCPUMonitor() CPUMonitor {
	if fileExists(filepath.Join(defaultCGroupRoot(), "cpu.stat")) {
		return NewCGroupV2CPUMonitor(defaultCGroupRoot())
	}
	if fileExists(filepath.Join(defaultCGroupRoot(), "cpuacct", "cpuacct.usage")) {
		return NewCGroupV1CPUMonitor(defaultCGroupRoot())
	}
	return NewRuntimeCPUMonitor()
}

func defaultCGroupRoot() string { return "/sys/fs/cgroup" }

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func environmentCPUCount() (float64, bool) {
	raw, exists := os.LookupEnv(CPUCountEnvironment)
	if !exists {
		return 0, false
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

func runtimeCPUCount() float64 {
	if value, ok := environmentCPUCount(); ok {
		return value
	}
	count := runtime.GOMAXPROCS(0)
	if count < 1 {
		count = 1
	}
	return float64(count)
}

type cpuSnapshot struct {
	busy  float64
	total float64
}

func percentBetween(start, end cpuSnapshot) float64 {
	total := end.total - start.total
	if total <= 0 {
		return 0
	}
	return clampCPU((end.busy - start.busy) / total)
}

func clampCPU(value float64) float64 {
	if math.IsNaN(value) || value <= 0 {
		return 0
	}
	if math.IsInf(value, 0) || value >= 1 {
		return 1
	}
	return value
}

func sampleAfter(ctx context.Context, interval time.Duration, sample func() (cpuSnapshot, error)) (float64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if interval <= 0 {
		return 0, errors.New("CPU sample interval must be positive")
	}
	start, err := sample()
	if err != nil {
		return 0, err
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return 0, context.Cause(ctx)
	case <-timer.C:
	}
	end, err := sample()
	if err != nil {
		return 0, err
	}
	return percentBetween(start, end), nil
}

// RuntimeCPUMonitor uses the stable runtime/metrics CPU classes. It measures
// this Go process rather than unrelated host work and is portable without cgo.
type RuntimeCPUMonitor struct{}

func NewRuntimeCPUMonitor() *RuntimeCPUMonitor { return &RuntimeCPUMonitor{} }
func (*RuntimeCPUMonitor) CPUCount() float64   { return runtimeCPUCount() }

func (*RuntimeCPUMonitor) CPUPercent(ctx context.Context, interval time.Duration) (float64, error) {
	return sampleAfter(ctx, interval, runtimeCPUSnapshot)
}

func runtimeCPUSnapshot() (cpuSnapshot, error) {
	samples := []runtimemetrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
	}
	runtimemetrics.Read(samples)
	if samples[0].Value.Kind() != runtimemetrics.KindFloat64 || samples[1].Value.Kind() != runtimemetrics.KindFloat64 {
		return cpuSnapshot{}, ErrCPUUsageUnavailable
	}
	total := samples[0].Value.Float64()
	idle := samples[1].Value.Float64()
	return cpuSnapshot{busy: max(total-idle, 0), total: total}, nil
}

// CGroupV2CPUMonitor reads unified-hierarchy CPU quota and usage.
type CGroupV2CPUMonitor struct{ root string }

func NewCGroupV2CPUMonitor(root string) *CGroupV2CPUMonitor {
	if root == "" {
		root = defaultCGroupRoot()
	}
	return &CGroupV2CPUMonitor{root: root}
}

func (m *CGroupV2CPUMonitor) CPUCount() float64 {
	if value, ok := environmentCPUCount(); ok {
		return value
	}
	content, err := os.ReadFile(filepath.Join(m.root, "cpu.max"))
	if err != nil {
		return runtimeCPUCount()
	}
	fields := strings.Fields(string(content))
	if len(fields) < 2 || fields[0] == "max" {
		return runtimeCPUCount()
	}
	quota, quotaErr := strconv.ParseFloat(fields[0], 64)
	period, periodErr := strconv.ParseFloat(fields[1], 64)
	if quotaErr != nil || periodErr != nil || quota <= 0 || period <= 0 {
		return runtimeCPUCount()
	}
	return quota / period
}

func (m *CGroupV2CPUMonitor) CPUPercent(ctx context.Context, interval time.Duration) (float64, error) {
	count := m.CPUCount()
	started := time.Now()
	start, err := m.usage()
	if err != nil {
		return 0, err
	}
	if err := waitCPUInterval(ctx, interval); err != nil {
		return 0, err
	}
	end, err := m.usage()
	if err != nil {
		return 0, err
	}
	elapsed := time.Since(started).Seconds()
	if count <= 0 || elapsed <= 0 {
		return 0, nil
	}
	return clampCPU((end - start) / (elapsed * count)), nil
}

func (m *CGroupV2CPUMonitor) usage() (float64, error) {
	content, err := os.ReadFile(filepath.Join(m.root, "cpu.stat"))
	if err != nil {
		return 0, fmt.Errorf("read cgroup v2 CPU usage: %w", err)
	}
	for _, line := range strings.Split(string(content), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "usage_usec" {
			value, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				return 0, fmt.Errorf("parse cgroup v2 CPU usage: %w", err)
			}
			return value / 1_000_000, nil
		}
	}
	return 0, fmt.Errorf("%w: usage_usec missing from cpu.stat", ErrCPUUsageUnavailable)
}

// CGroupV1CPUMonitor reads the legacy cpu/cpuacct controllers.
type CGroupV1CPUMonitor struct{ root string }

func NewCGroupV1CPUMonitor(root string) *CGroupV1CPUMonitor {
	if root == "" {
		root = defaultCGroupRoot()
	}
	return &CGroupV1CPUMonitor{root: root}
}

func (m *CGroupV1CPUMonitor) CPUCount() float64 {
	if value, ok := environmentCPUCount(); ok {
		return value
	}
	quota, quotaOK := readCPUFloat(filepath.Join(m.root, "cpu", "cpu.cfs_quota_us"))
	period, periodOK := readCPUFloat(filepath.Join(m.root, "cpu", "cpu.cfs_period_us"))
	if !quotaOK || !periodOK || quota < 0 || period <= 0 {
		return DefaultCGroupV1CPUCount
	}
	return quota / period
}

func readCPUFloat(path string) (float64, bool) {
	content, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(string(content)), 64)
	return value, err == nil
}

func (m *CGroupV1CPUMonitor) CPUPercent(ctx context.Context, interval time.Duration) (float64, error) {
	count := m.CPUCount()
	started := time.Now()
	start, err := m.usage()
	if err != nil {
		return 0, err
	}
	if err := waitCPUInterval(ctx, interval); err != nil {
		return 0, err
	}
	end, err := m.usage()
	if err != nil {
		return 0, err
	}
	elapsed := time.Since(started).Seconds()
	if count <= 0 || elapsed <= 0 {
		return 0, nil
	}
	return clampCPU((end - start) / (elapsed * count)), nil
}

func (m *CGroupV1CPUMonitor) usage() (float64, error) {
	value, ok := readCPUFloat(filepath.Join(m.root, "cpuacct", "cpuacct.usage"))
	if !ok {
		return 0, fmt.Errorf("%w: read cgroup v1 cpuacct.usage", ErrCPUUsageUnavailable)
	}
	return value / 1_000_000_000, nil
}

func waitCPUInterval(ctx context.Context, interval time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if interval <= 0 {
		return errors.New("CPU sample interval must be positive")
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

var _ CPUMonitor = (*RuntimeCPUMonitor)(nil)
var _ CPUMonitor = (*CGroupV2CPUMonitor)(nil)
var _ CPUMonitor = (*CGroupV1CPUMonitor)(nil)
