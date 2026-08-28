// SPDX-License-Identifier: Apache-2.0

// Package providerformat converts LiveKit chat contexts into the wire-neutral
// request shapes consumed by OpenAI-compatible, Google Gemini, and Mistral
// APIs. It mirrors the provider_format namespace in agents-js while keeping
// provider client dependencies out of the core SDK.
//
// The returned values contain only maps, slices, strings, booleans, and
// numbers, so callers can marshal them with encoding/json or pass them to a
// provider SDK without another LiveKit-specific dependency.
package providerformat
