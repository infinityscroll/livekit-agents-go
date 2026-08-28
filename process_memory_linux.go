//go:build linux

// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"fmt"
	"os"
	"strings"
)

func memoryMonitoringSupported() bool { return true }

func processRSSBytes(pid int) (int64, error) {
	payload, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid))
	if err != nil {
		return 0, err
	}
	var totalPages, residentPages int64
	if _, err := fmt.Fscan(strings.NewReader(string(payload)), &totalPages, &residentPages); err != nil {
		return 0, fmt.Errorf("parse /proc/%d/statm: %w", pid, err)
	}
	return residentPages * int64(os.Getpagesize()), nil
}
