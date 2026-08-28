// SPDX-License-Identifier: Apache-2.0

package agents

import "time"

const firstRetryInterval = 100 * time.Millisecond

// APIConnectOptions controls timeout and retry behavior for provider calls.
// Zero fields resolve to DefaultAPIConnectOptions. Set MaxRetries to a negative
// value to explicitly disable retries.
type APIConnectOptions struct {
	MaxRetries    int
	RetryInterval time.Duration
	Timeout       time.Duration
}

var DefaultAPIConnectOptions = APIConnectOptions{
	MaxRetries:    3,
	RetryInterval: 2 * time.Second,
	Timeout:       10 * time.Second,
}

// Resolve returns a validated, fully populated copy.
func (o APIConnectOptions) Resolve() APIConnectOptions {
	r := o
	if r.MaxRetries == 0 {
		r.MaxRetries = DefaultAPIConnectOptions.MaxRetries
	} else if r.MaxRetries < 0 {
		r.MaxRetries = 0
	}
	if r.RetryInterval <= 0 {
		r.RetryInterval = DefaultAPIConnectOptions.RetryInterval
	}
	if r.Timeout <= 0 {
		r.Timeout = DefaultAPIConnectOptions.Timeout
	}
	return r
}

// RetryDelay returns the delay before retry number n. The first retry is fast,
// matching the Python and TypeScript SDKs.
func (o APIConnectOptions) RetryDelay(n int) time.Duration {
	if n <= 0 {
		return firstRetryInterval
	}
	return o.Resolve().RetryInterval
}

type SessionConnectOptions struct {
	STT                    APIConnectOptions
	LLM                    APIConnectOptions
	TTS                    APIConnectOptions
	MaxUnrecoverableErrors int
}

var DefaultSessionConnectOptions = SessionConnectOptions{
	STT:                    DefaultAPIConnectOptions,
	LLM:                    DefaultAPIConnectOptions,
	TTS:                    DefaultAPIConnectOptions,
	MaxUnrecoverableErrors: 3,
}

func (o SessionConnectOptions) Resolve() SessionConnectOptions {
	r := o
	r.STT = r.STT.Resolve()
	r.LLM = r.LLM.Resolve()
	r.TTS = r.TTS.Resolve()
	if r.MaxUnrecoverableErrors <= 0 {
		r.MaxUnrecoverableErrors = DefaultSessionConnectOptions.MaxUnrecoverableErrors
	}
	return r
}
