// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxFunctionArgumentsBytes = 1 << 20

// ParseFunctionArguments accepts a JSON string or an already-decoded object.
// It first uses strict JSON, then applies a conservative repair pass for the
// malformed object syntax commonly produced by streaming language models
// (unquoted keys, single-quoted strings, trailing commas, and omitted closing
// delimiters). Template tokens are stripped only on the repaired path, so a
// legitimate strict-JSON value such as "<|safe|>" remains untouched.
func ParseFunctionArguments(raw any) (map[string]any, error) {
	switch value := raw.(type) {
	case map[string]any:
		return value, nil
	case nil:
		return map[string]any{}, nil
	case string:
		if len(value) > MaxFunctionArgumentsBytes {
			return nil, fmt.Errorf("function arguments exceed %d bytes", MaxFunctionArgumentsBytes)
		}
		return parseFunctionArgumentsString(value)
	default:
		return nil, fmt.Errorf("expected JSON string or object from function arguments, got %T", raw)
	}
}

func parseFunctionArgumentsString(raw string) (map[string]any, error) {
	value, strictErr := decodeFunctionJSON(raw)
	repaired := false
	if strictErr != nil {
		fixed, err := repairFunctionJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("could not parse function arguments as JSON: %v: %s", strictErr, truncateArgumentError(raw))
		}
		value, err = decodeFunctionJSON(fixed)
		if err != nil {
			return nil, fmt.Errorf("could not parse function arguments as JSON: %v: %s", strictErr, truncateArgumentError(raw))
		}
		value = stripLeakedTemplateTokens(value)
		repaired = true
	}

	for {
		stringValue, ok := value.(string)
		if !ok {
			break
		}
		nested, err := decodeFunctionJSON(stringValue)
		if err != nil {
			return nil, fmt.Errorf("function arguments decoded to a non-JSON string: %s", truncateArgumentError(stringValue))
		}
		value = nested
	}
	if value == nil {
		return map[string]any{}, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected object from function arguments: %s", truncateArgumentError(raw))
	}
	if repaired {
		if stripped, ok := stripLeakedTemplateTokens(object).(map[string]any); ok {
			object = stripped
		}
	}
	return object, nil
}

func decodeFunctionJSON(raw string) (any, error) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return nil, errors.New("multiple JSON values")
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}
	return value, nil
}

func truncateArgumentError(value string) string {
	if len(value) <= 200 {
		return value
	}
	end := 200
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

// repairFunctionJSON deliberately does not attempt arbitrary natural-language
// recovery. It repairs a small deterministic JSON-like grammar and otherwise
// returns an error, avoiding surprising reinterpretation of tool arguments.
func repairFunctionJSON(input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", errors.New("empty arguments")
	}
	var out strings.Builder
	out.Grow(len(input) + 8)
	stack := make([]byte, 0, 8)
	inDouble, inSingle, escaped := false, false, false
	expectingKey := false

	for index := 0; index < len(input); {
		ch := input[index]
		if inDouble {
			if escaped {
				out.WriteByte(ch)
				escaped = false
				index++
				continue
			}
			switch ch {
			case '\\':
				escaped = true
				out.WriteByte(ch)
			case '"':
				inDouble = false
				out.WriteByte(ch)
			default:
				// A model occasionally escapes the intended final quote and
				// leaves only closing container delimiters. Insert a real quote
				// before that suffix; the escaped quote remains content and is
				// removed later only if it is a leaked template token.
				if (ch == ']' || ch == '}') && onlyClosingSuffix(input[index:]) {
					out.WriteByte('"')
					inDouble = false
					continue
				}
				out.WriteByte(ch)
			}
			index++
			continue
		}
		if inSingle {
			if escaped {
				switch ch {
				case '\'', '\\':
					out.WriteByte(ch)
				default:
					out.WriteByte('\\')
					out.WriteByte(ch)
				}
				escaped = false
				index++
				continue
			}
			switch ch {
			case '\\':
				escaped = true
			case '\'':
				inSingle = false
				out.WriteByte('"')
			case '"':
				out.WriteString(`\"`)
			case '\n':
				out.WriteString(`\n`)
			case '\r':
				out.WriteString(`\r`)
			case '\t':
				out.WriteString(`\t`)
			default:
				out.WriteByte(ch)
			}
			index++
			continue
		}

		if unicode.IsSpace(rune(ch)) {
			out.WriteByte(ch)
			index++
			continue
		}
		if expectingKey && isBareKeyStart(ch) {
			end := index + 1
			for end < len(input) && isBareKeyContinue(input[end]) {
				end++
			}
			probe := end
			for probe < len(input) && unicode.IsSpace(rune(input[probe])) {
				probe++
			}
			if probe < len(input) && input[probe] == ':' {
				encoded, _ := json.Marshal(input[index:end])
				out.Write(encoded)
				index = end
				expectingKey = false
				continue
			}
		}

		switch ch {
		case '"':
			inDouble = true
			out.WriteByte(ch)
			expectingKey = false
		case '\'':
			inSingle = true
			out.WriteByte('"')
			expectingKey = false
		case '{':
			stack = append(stack, '}')
			out.WriteByte(ch)
			expectingKey = true
		case '[':
			stack = append(stack, ']')
			out.WriteByte(ch)
			expectingKey = false
		case '}', ']':
			trimTrailingComma(&out)
			if len(stack) == 0 || stack[len(stack)-1] != ch {
				return "", fmt.Errorf("unbalanced delimiter %q", ch)
			}
			stack = stack[:len(stack)-1]
			out.WriteByte(ch)
			expectingKey = false
		case ',':
			out.WriteByte(ch)
			expectingKey = len(stack) != 0 && stack[len(stack)-1] == '}'
		case ':':
			out.WriteByte(ch)
			expectingKey = false
		default:
			out.WriteByte(ch)
		}
		index++
	}
	if inSingle {
		out.WriteByte('"')
	} else if inDouble {
		if escaped {
			out.WriteByte('"')
		}
		out.WriteByte('"')
	}
	for index := len(stack) - 1; index >= 0; index-- {
		trimTrailingComma(&out)
		out.WriteByte(stack[index])
	}
	return out.String(), nil
}

func onlyClosingSuffix(value string) bool {
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case ']', '}', ' ', '\t', '\r', '\n':
		default:
			return false
		}
	}
	return true
}

func trimTrailingComma(builder *strings.Builder) {
	value := builder.String()
	end := len(value)
	for end > 0 && unicode.IsSpace(rune(value[end-1])) {
		end--
	}
	if end == 0 || value[end-1] != ',' {
		return
	}
	suffix := value[end:]
	builder.Reset()
	builder.Grow(len(value) - 1)
	builder.WriteString(value[:end-1])
	builder.WriteString(suffix)
}

func isBareKeyStart(value byte) bool {
	return value == '_' || value == '$' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isBareKeyContinue(value byte) bool {
	return isBareKeyStart(value) || value >= '0' && value <= '9' || value == '-'
}

func stripLeakedTemplateTokens(value any) any {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(stripTemplateTokenString(typed))
	case []any:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			item = stripLeakedTemplateTokens(item)
			if item != nil && item != "" {
				result = append(result, item)
			}
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = stripLeakedTemplateTokens(item)
		}
		return result
	default:
		return value
	}
}

func stripTemplateTokenString(value string) string {
	value = strings.ReplaceAll(value, "<start_of_turn>", "")
	value = strings.ReplaceAll(value, "<end_of_turn>", "")
	for {
		start := strings.Index(value, "<|")
		if start < 0 {
			break
		}
		if end := strings.Index(value[start+2:], "|>"); end >= 0 && end <= 40 {
			value = value[:start] + value[start+2+end+2:]
			continue
		}
		end := start + 2
		for end < len(value) && end-start-2 < 10 && !isTemplateTokenWord(value[end]) && value[end] != '<' && value[end] != '>' {
			end++
		}
		if end > start+2 {
			value = value[:start] + value[end:]
			continue
		}
		break
	}
	return value
}

func isTemplateTokenWord(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}
