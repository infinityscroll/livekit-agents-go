// SPDX-License-Identifier: Apache-2.0

package tokenize

import "strings"

// HyphenateWord applies the Frank Liang English patterns used by the Python
// and TypeScript SDKs. Short and non-ASCII words are returned unchanged.
func HyphenateWord(word string) []string {
	if len(word) <= 4 || !isASCII(word) {
		return []string{word}
	}
	lower := strings.ToLower(word)
	if breaks, ok := hyphenException(lower); ok {
		return splitAtBreaks(word, breaks)
	}

	work := "." + lower + "."
	points := make([]byte, len(work)+1)
	for i := 0; i < len(work); i++ {
		node := uint16(0)
		for j := i; j < len(work); j++ {
			next, ok := hyphenChild(node, work[j])
			if !ok {
				break
			}
			node = next
			meta := hyphenNodes[node]
			for k := 0; k < int(meta.pointCount); k++ {
				at := i + k
				if at >= len(points) {
					break
				}
				value := hyphenPoints[int(meta.pointStart)+k]
				if value > points[at] {
					points[at] = value
				}
			}
		}
	}

	// TeX's default left/right minima: never split within the first or last
	// two letters.
	points[1], points[2] = 0, 0
	points[len(points)-2], points[len(points)-3] = 0, 0

	pieces := make([]string, 0, 4)
	start := 0
	for i := 0; i < len(word); i++ {
		if points[i+2]&1 != 0 {
			pieces = append(pieces, word[start:i+1])
			start = i + 1
		}
	}
	pieces = append(pieces, word[start:])
	return pieces
}

// Hyphenate is a concise compatibility alias.
func Hyphenate(word string) []string { return HyphenateWord(word) }

func hyphenChild(node uint16, ch byte) (uint16, bool) {
	meta := hyphenNodes[node]
	lo := int(meta.edgeStart)
	hi := lo + int(meta.edgeCount)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		edge := hyphenEdges[mid]
		if edge.ch < ch {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	end := int(meta.edgeStart) + int(meta.edgeCount)
	if lo < end && hyphenEdges[lo].ch == ch {
		return hyphenEdges[lo].next, true
	}
	return 0, false
}

func hyphenException(word string) (string, bool) {
	switch word {
	case "associate", "associates":
		return "\x02\x04", true
	case "declination":
		return "\x03\x05\x07", true
	case "obligatory":
		return "\x05\x06", true
	case "philanthropic":
		return "\x04\x06", true
	case "present", "presents", "project", "projects":
		return "", true
	case "reciprocity":
		return "\x04", true
	case "recognizance":
		return "\x02\x05\x07", true
	case "reformation", "retribution":
		return "\x03\x05\x07", true
	case "table":
		return "\x02", true
	default:
		return "", false
	}
}

func splitAtBreaks(word, breaks string) []string {
	if breaks == "" {
		return []string{word}
	}
	out := make([]string, 0, len(breaks)+1)
	start := 0
	for i := 0; i < len(breaks); i++ {
		at := int(breaks[i])
		out = append(out, word[start:at])
		start = at
	}
	out = append(out, word[start:])
	return out
}

func isASCII(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] >= 0x80 {
			return false
		}
	}
	return true
}

// Generated compact trie records. Static tables avoid parsing nearly five
// thousand TeX patterns during process startup.
type hyphenNode struct {
	edgeStart  uint16
	edgeCount  uint8
	pointStart uint16
	pointCount uint8
}

type hyphenEdge struct {
	ch   byte
	next uint16
}
