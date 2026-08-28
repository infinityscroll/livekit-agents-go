// SPDX-License-Identifier: Apache-2.0

package agents

import "testing"

func TestNormalizeLanguage(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]LanguageCode{
		" English ":  "en",
		"eng":        "en",
		"pt_br":      "pt-BR",
		"zh-hant-tw": "zh-Hant-TW",
		"Slovenian":  "sl",
	} {
		if got := NormalizeLanguage(input); got != want {
			t.Errorf("NormalizeLanguage(%q) = %q, want %q", input, got, want)
		}
	}
}
