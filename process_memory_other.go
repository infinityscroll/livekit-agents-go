//go:build !linux

// SPDX-License-Identifier: Apache-2.0

package agents

import "errors"

func memoryMonitoringSupported() bool { return false }

func processRSSBytes(int) (int64, error) {
	return 0, errors.New("process memory monitoring is unsupported on this operating system")
}
