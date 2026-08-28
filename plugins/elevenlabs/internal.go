// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	agents "github.com/livekit/agents-go"
)

type ProviderError struct {
	Type          string
	Message       string
	Details       string
	RetryableFlag bool
	Body          any
}

func (e *ProviderError) Error() string {
	message := e.Message
	if message == "" {
		message = "provider error"
	}
	if e.Type != "" {
		message = e.Type + ": " + message
	}
	if e.Details != "" {
		message += " - " + e.Details
	}
	return "elevenlabs: " + message
}

func (e *ProviderError) Retryable() bool { return e != nil && e.RetryableFlag }

func endpointURL(base *url.URL, segments ...string) *url.URL {
	u := *base
	all := make([]string, 0, len(segments)+1)
	if u.Path != "" {
		all = append(all, u.Path)
	}
	all = append(all, segments...)
	u.Path = path.Join(all...)
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return &u
}

func websocketURL(base *url.URL, segments ...string) *url.URL {
	u := endpointURL(base, segments...)
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	return u
}

func requestID(header http.Header) string {
	for _, key := range []string{"request-id", "xi-request-id", "x-request-id"} {
		if value := header.Get(key); value != "" {
			return value
		}
	}
	return ""
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		limit = defaultMaxResponseBytes
	}
	limited := io.LimitReader(reader, limit+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, &ProtocolError{Message: fmt.Sprintf("response exceeds %d bytes", limit)}
	}
	return body, nil
}

func apiStatusError(response *http.Response, body []byte) error {
	var decoded any
	if len(body) != 0 && json.Unmarshal(body, &decoded) != nil {
		decoded = string(body)
	}
	message := http.StatusText(response.StatusCode)
	if record, ok := decoded.(map[string]any); ok {
		if detail, ok := record["detail"].(string); ok && detail != "" {
			message = detail
		} else if value, ok := record["message"].(string); ok && value != "" {
			message = value
		}
	}
	retry := response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
	return agents.NewAPIStatusError("ElevenLabs API error: "+message, response.StatusCode, requestID(response.Header), decoded, retry, nil)
}

func normalizeNetworkError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return agents.NewAPITimeoutError("ElevenLabs request timed out", true, err)
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	if agents.IsAPIError(err) {
		return err
	}
	var protocol *ProtocolError
	var provider *ProviderError
	if errors.As(err, &protocol) || errors.As(err, &provider) {
		return err
	}
	return agents.NewAPIConnectionError("ElevenLabs connection failed", true, err)
}

func isRetryable(err error) bool {
	type retryable interface{ Retryable() bool }
	var target retryable
	return errors.As(err, &target) && target.Retryable()
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func withAttemptTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func contentMediaType(header string) (string, error) {
	if strings.TrimSpace(header) == "" {
		return "", &ProtocolError{Message: "missing Content-Type"}
	}
	mediaType, _, err := mime.ParseMediaType(header)
	if err != nil {
		return "", &ProtocolError{Message: "invalid Content-Type", Cause: err}
	}
	return strings.ToLower(mediaType), nil
}

func requireJSONContentType(header string) error {
	mediaType, err := contentMediaType(header)
	if err != nil {
		return err
	}
	if mediaType != "application/json" && !strings.HasSuffix(mediaType, "+json") {
		return &ProtocolError{Message: "expected JSON Content-Type, got " + mediaType}
	}
	return nil
}

func requirePCMContentType(header string) error {
	mediaType, err := contentMediaType(header)
	if err != nil {
		return err
	}
	switch mediaType {
	case "audio/pcm", "audio/x-pcm":
		return nil
	default:
		return &ProtocolError{Message: "expected PCM audio Content-Type, got " + mediaType}
	}
}

func createWAV(frames []agents.AudioFrame) ([]byte, agents.AudioFrame, error) {
	merged, err := agents.MergeFrames(frames)
	if err != nil {
		return nil, agents.AudioFrame{}, err
	}
	dataBytes := len(merged.Data) * 2
	if dataBytes > int(^uint32(0))-36 {
		return nil, agents.AudioFrame{}, fmt.Errorf("elevenlabs: WAV input is too large")
	}
	buf := bytes.NewBuffer(make([]byte, 0, 44+dataBytes))
	buf.WriteString("RIFF")
	_ = binary.Write(buf, binary.LittleEndian, uint32(36+dataBytes))
	buf.WriteString("WAVEfmt ")
	_ = binary.Write(buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(buf, binary.LittleEndian, uint16(merged.Channels))
	_ = binary.Write(buf, binary.LittleEndian, uint32(merged.SampleRate))
	byteRate := merged.SampleRate * merged.Channels * 2
	_ = binary.Write(buf, binary.LittleEndian, uint32(byteRate))
	_ = binary.Write(buf, binary.LittleEndian, uint16(merged.Channels*2))
	_ = binary.Write(buf, binary.LittleEndian, uint16(16))
	buf.WriteString("data")
	_ = binary.Write(buf, binary.LittleEndian, uint32(dataBytes))
	for _, sample := range merged.Data {
		_ = binary.Write(buf, binary.LittleEndian, sample)
	}
	return buf.Bytes(), merged, nil
}

func pcmBytes(frame agents.AudioFrame) ([]byte, error) {
	if frame.SampleRate <= 0 || frame.Channels != 1 || frame.SamplesPerChannel != len(frame.Data) {
		return nil, fmt.Errorf("%w: realtime STT requires mono PCM16", agents.ErrInvalidAudioFormat)
	}
	data := make([]byte, len(frame.Data)*2)
	for i, sample := range frame.Data {
		binary.LittleEndian.PutUint16(data[i*2:], uint16(sample))
	}
	return data, nil
}

func boolString(value bool) string { return strconv.FormatBool(value) }

func mergedKeyterms(user, session []string) []string {
	result := make([]string, 0, len(user)+len(session))
	seen := make(map[string]struct{}, len(user)+len(session))
	for _, group := range [][]string{user, session} {
		for _, term := range group {
			term = strings.TrimSpace(term)
			key := strings.ToLower(term)
			if term == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			result = append(result, term)
		}
	}
	return result
}

func retryDelay(options agents.APIConnectOptions, retry int) time.Duration {
	return options.RetryDelay(retry)
}
