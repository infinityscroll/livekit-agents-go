// SPDX-License-Identifier: Apache-2.0

// Package recorderio records the user input and agent output sides of a voice
// session into a synchronized stereo Ogg/Opus file.
//
// RecordInput and RecordOutput return ownership-neutral decorators for the
// SDK's voice.AudioInput and voice.AudioOutput contracts. The left channel is
// user input and the right channel is agent output. Output is retained until
// its authoritative playback-finished event arrives, so interrupted speech is
// truncated to the position that actually played and pause intervals become
// silence without shifting the two channels out of alignment.
//
// Recording work is lazy. NewRecorderIO starts no goroutines and resolves no
// executable; Start launches two bounded workers, and FFmpeg is resolved via
// agents.ResolveFFmpegPath only when the first non-empty PCM block is encoded.
package recorderio
