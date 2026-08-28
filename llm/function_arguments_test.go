// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseFunctionArgumentsStrictAndNested(t *testing.T) {
	want := map[string]any{"a": float64(1), "b": []any{float64(2), float64(3)}}
	got, err := ParseFunctionArguments(`{"a":1,"b":[2,3]}`)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v err=%v", got, err)
	}
	got, err = ParseFunctionArguments(`"{\"a\":1}"`)
	if err != nil || got["a"] != float64(1) {
		t.Fatalf("nested got=%#v err=%v", got, err)
	}
	got, err = ParseFunctionArguments("null")
	if err != nil || len(got) != 0 {
		t.Fatalf("null got=%#v err=%v", got, err)
	}
}

func TestParseFunctionArgumentsRepairsConservativeSyntax(t *testing.T) {
	cases := []struct {
		raw  string
		want map[string]any
	}{
		{`{"arg1":"hi","optArg2":"yo"`, map[string]any{"arg1": "hi", "optArg2": "yo"}},
		{`{arg1: 'hi',}`, map[string]any{"arg1": "hi"}},
		{`{"nested": {value: 3}`, map[string]any{"nested": map[string]any{"value": float64(3)}}},
	}
	for _, test := range cases {
		got, err := ParseFunctionArguments(test.raw)
		if err != nil || !reflect.DeepEqual(got, test.want) {
			t.Errorf("ParseFunctionArguments(%q)=%#v,%v want %#v", test.raw, got, err, test.want)
		}
	}
}

func TestParseFunctionArgumentsTemplateTokensOnlyAfterRepair(t *testing.T) {
	strict, err := ParseFunctionArguments(`{"arg1":"<|safe|>"}`)
	if err != nil || strict["arg1"] != "<|safe|>" {
		t.Fatalf("strict=%#v err=%v", strict, err)
	}
	leaked := `{"orderId":["<|\"|\"O_WAAB70<|\"|\"]}`
	repaired, err := ParseFunctionArguments(leaked)
	if err != nil {
		t.Fatal(err)
	}
	values, ok := repaired["orderId"].([]any)
	if !ok || len(values) != 1 || values[0] != "O_WAAB70" {
		t.Fatalf("repaired=%#v", repaired)
	}
}

func TestParseFunctionArgumentsRejectsNonObjectsAndOversize(t *testing.T) {
	for _, raw := range []string{`[1,2]`, `"just a string"`, `true`, `{} trailing`, ``} {
		if _, err := ParseFunctionArguments(raw); err == nil {
			t.Errorf("expected error for %q", raw)
		}
	}
	if _, err := ParseFunctionArguments(strings.Repeat("x", MaxFunctionArgumentsBytes+1)); err == nil {
		t.Fatal("expected size error")
	}
}

func FuzzParseFunctionArguments(f *testing.F) {
	f.Add(`{"a":1}`)
	f.Add(`{a:'b'}`)
	f.Add(`{"a":`)
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 32<<10 {
			t.Skip()
		}
		value, err := ParseFunctionArguments(raw)
		if err == nil && value == nil {
			t.Fatal("successful parse returned nil")
		}
	})
}
