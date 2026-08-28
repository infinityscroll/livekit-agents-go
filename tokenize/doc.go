// SPDX-License-Identifier: Apache-2.0

// Package tokenize provides the sentence, word, paragraph, and English
// hyphenation primitives used by streaming transcription and TTS.
//
// Offsets returned by this package are UTF-8 byte offsets, matching Go's slice
// conventions. Streams are bounded and apply backpressure to PushText and Flush;
// one goroutine may send while another receives, and Close may run concurrently.
package tokenize
