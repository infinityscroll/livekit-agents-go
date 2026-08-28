// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"encoding/json"
	"errors"
	"fmt"
)

type UnexpectedModelBehavior struct {
	Message string
	Cause   error
}

func (e *UnexpectedModelBehavior) Error() string { return e.Message }
func (e *UnexpectedModelBehavior) Unwrap() error { return e.Cause }

type AssignmentTimeoutError struct{ Message string }

func (e *AssignmentTimeoutError) Error() string {
	if e.Message == "" {
		return "Assignment timeout occurred"
	}
	return e.Message
}

// APIError is returned by STT, LLM, TTS, VAD, and inference providers.
type APIError struct {
	Message       string
	Body          any
	RetryableFlag bool
	Cause         error
}

func NewAPIError(message string, body any, retryable bool, cause error) *APIError {
	return &APIError{Message: message, Body: body, RetryableFlag: retryable, Cause: cause}
}

func (e *APIError) Error() string {
	if e == nil {
		return "<nil>"
	}
	body, err := json.Marshal(e.Body)
	if err != nil {
		body = []byte(fmt.Sprintf("%q", fmt.Sprint(e.Body)))
	}
	return fmt.Sprintf("%s (body=%s, retryable=%t)", e.Message, body, e.RetryableFlag)
}

func (e *APIError) Unwrap() error   { return e.Cause }
func (e *APIError) Retryable() bool { return e != nil && e.RetryableFlag }

type APIStatusError struct {
	*APIError
	StatusCode int
	RequestID  string
}

func NewAPIStatusError(message string, statusCode int, requestID string, body any, retryable bool, cause error) *APIStatusError {
	if message == "" {
		message = "API error."
	}
	if statusCode == 0 {
		statusCode = -1
	}
	if statusCode >= 400 && statusCode < 500 && statusCode != 408 && statusCode != 429 && statusCode != 499 {
		retryable = false
	}
	return &APIStatusError{
		APIError:   NewAPIError(message, body, retryable, cause),
		StatusCode: statusCode,
		RequestID:  requestID,
	}
}

func (e *APIStatusError) Error() string {
	if e == nil {
		return "<nil>"
	}
	body, err := json.Marshal(e.Body)
	if err != nil {
		body = []byte(fmt.Sprintf("%q", fmt.Sprint(e.Body)))
	}
	return fmt.Sprintf("%s (statusCode=%d, requestId=%s, body=%s, retryable=%t)",
		e.Message, e.StatusCode, e.RequestID, body, e.RetryableFlag)
}

func (e *APIStatusError) Unwrap() error { return e.APIError }

type APIConnectionError struct{ *APIError }

func (e *APIConnectionError) Unwrap() error { return e.APIError }

func NewAPIConnectionError(message string, retryable bool, cause error) *APIConnectionError {
	if message == "" {
		message = "Connection error."
	}
	return &APIConnectionError{APIError: NewAPIError(message, nil, retryable, cause)}
}

type APITimeoutError struct{ *APIConnectionError }

func (e *APITimeoutError) Unwrap() error { return e.APIConnectionError }

func NewAPITimeoutError(message string, retryable bool, cause error) *APITimeoutError {
	if message == "" {
		message = "Request timed out."
	}
	return &APITimeoutError{APIConnectionError: NewAPIConnectionError(message, retryable, cause)}
}

func IsAPIError(err error) bool {
	var target *APIError
	return errors.As(err, &target)
}

type MissingCredentialsError struct{ Name string }

func (e *MissingCredentialsError) Error() string {
	if e.Name == "" {
		return "missing credentials"
	}
	return "missing credential: " + e.Name
}

type IdleTimeoutError struct{ Message string }

func (e *IdleTimeoutError) Error() string {
	if e.Message == "" {
		return "idle timeout"
	}
	return e.Message
}

type WorkerError struct {
	Message string
	Cause   error
}

func (e *WorkerError) Error() string { return e.Message }
func (e *WorkerError) Unwrap() error { return e.Cause }
