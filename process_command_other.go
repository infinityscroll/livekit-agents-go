//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

// SPDX-License-Identifier: Apache-2.0

package agents

import "os/exec"

func configureJobCommand(*exec.Cmd) {}

func killJobProcess(command *exec.Cmd) error {
	if command == nil || command.Process == nil {
		return nil
	}
	return command.Process.Kill()
}
