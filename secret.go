// SPDX-License-Identifier: Apache-2.0

package agents

import "encoding/json"

type SecretString string

func NewSecretString(value string) SecretString   { return SecretString(value) }
func (SecretString) String() string               { return "[REDACTED]" }
func (SecretString) GoString() string             { return "agents.SecretString([REDACTED])" }
func (s SecretString) Reveal() string             { return string(s) }
func (SecretString) MarshalText() ([]byte, error) { return []byte("[REDACTED]"), nil }
func (SecretString) MarshalJSON() ([]byte, error) { return json.Marshal("[REDACTED]") }
