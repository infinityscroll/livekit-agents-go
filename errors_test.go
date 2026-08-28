// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"errors"
	"testing"
)

func TestAPIStatusErrorRetryability(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		status    int
		requested bool
		want      bool
	}{
		{400, true, false},
		{401, true, false},
		{408, true, true},
		{429, true, true},
		{499, true, true},
		{500, true, true},
		{503, false, false},
	} {
		err := NewAPIStatusError("failed", tt.status, "request", nil, tt.requested, nil)
		if got := err.Retryable(); got != tt.want {
			t.Errorf("status %d: Retryable() = %t, want %t", tt.status, got, tt.want)
		}
		if !IsAPIError(err) {
			t.Errorf("status %d: IsAPIError returned false", tt.status)
		}
	}
}

func TestAPIErrorUnwrap(t *testing.T) {
	t.Parallel()
	cause := errors.New("network")
	err := NewAPITimeoutError("", true, cause)
	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is(%v, cause) = false", err)
	}
}

func TestErrorDefaultsMatchUpstream(t *testing.T) {
	t.Parallel()
	if got := (&AssignmentTimeoutError{}).Error(); got != "Assignment timeout occurred" {
		t.Fatalf("AssignmentTimeoutError default = %q", got)
	}
	status := NewAPIStatusError("", 0, "", nil, true, nil)
	if status.Message != "API error." || status.StatusCode != -1 || !status.Retryable() {
		t.Fatalf("APIStatusError defaults = %#v", status)
	}
}
