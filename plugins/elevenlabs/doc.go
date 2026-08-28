// SPDX-License-Identifier: Apache-2.0

// Package elevenlabs provides production ElevenLabs speech-to-text and
// text-to-speech clients for LiveKit Agents.
//
// The package implements Scribe batch and realtime transcription, HTTP PCM
// synthesis, and the ElevenLabs multi-context websocket protocol. Network and
// stream buffers are bounded, every blocking operation is context-aware, and
// compressed output is rejected unless it can be decoded into the PCM frames
// required by the agents TTS contract.
package elevenlabs
