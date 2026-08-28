// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"crypto/rand"
	"strings"
)

// ShortUUID returns a URL-safe, process-independent identifier with at least
// 100 bits of randomness. crypto/rand.Text panics only if OS entropy fails.
func ShortUUID(prefix string) string {
	text := strings.ToLower(rand.Text())
	if len(text) > 20 {
		text = text[:20]
	}
	return prefix + text
}
