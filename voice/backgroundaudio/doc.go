// SPDX-License-Identifier: Apache-2.0

// Package backgroundaudio provides the background-audio player shipped by the
// LiveKit Agents TypeScript SDK, adapted to context-first Go APIs.
//
// The player mixes file, built-in, and caller-provided PCM16 streams at 48 kHz
// mono. Work is entirely lazy: constructing a player starts no goroutines,
// opens no files, and extracts no embedded resources. Each active source has a
// bounded 400 ms mixer-ingress queue and the mixer wakes only on its 100 ms
// playout clock while streams are active.
//
// The four byte-identical agents-js 1.7.1 Ogg assets add about 970 KiB to a
// binary that imports this package. BuiltinResolver can redirect decoding to
// caller-managed paths (it does not remove the embedded bytes); default
// extraction is lazy, private, and removed by BackgroundAudioPlayer.Close.
package backgroundaudio
