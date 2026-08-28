// SPDX-License-Identifier: Apache-2.0

package agents

import "testing"

func TestOverrideTriState(t *testing.T) {
	t.Parallel()
	if got, enabled := (Override[string]{}).Resolve("parent", true); got != "parent" || !enabled {
		t.Fatalf("inherit = %q, %t", got, enabled)
	}
	if got, enabled := Use("child").Resolve("parent", true); got != "child" || !enabled {
		t.Fatalf("use = %q, %t", got, enabled)
	}
	if got, enabled := Disable[string]().Resolve("parent", true); got != "" || enabled {
		t.Fatalf("disable = %q, %t", got, enabled)
	}
}
