// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/voice"
	"github.com/livekit/protocol/auth"
	livekit "github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultRecordingUploadTimeout  = 15 * time.Minute
	defaultRecordingConnectTimeout = 30 * time.Second
	defaultMaxAudioUploadBytes     = int64(4 << 30)
	defaultRecordingMaxRetries     = 3
	severityError                  = int32(17)
)

type SessionReportUploaderConfig struct {
	AgentName         string
	CloudHostname     string
	RecordingEndpoint string
	LogEndpoint       string
	APIKey            agents.SecretString
	APISecret         agents.SecretString
	HTTPClient        *http.Client
	UploadGate        *UploadGate
	Metadata          map[string]any
	Logger            *slog.Logger
	MaxAudioBytes     int64
	MaxErrorBodyBytes int64
	// Zero selects three retries; a negative value disables retries.
	MaxRetries int
	// Sleep is injectable for deterministic tests. Nil uses a context-aware timer.
	Sleep func(context.Context, time.Duration) error
}

type SessionReportUploader struct {
	config SessionReportUploaderConfig

	tokenMu sync.Mutex
	token   string
}

func NewSessionReportUploader(config SessionReportUploaderConfig) (*SessionReportUploader, error) {
	recordingEndpoint, logEndpoint, err := resolveUploadEndpoints(config)
	if err != nil {
		return nil, err
	}
	config.RecordingEndpoint = recordingEndpoint
	config.LogEndpoint = logEndpoint
	if config.HTTPClient == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.DialContext = (&net.Dialer{
			Timeout: defaultRecordingConnectTimeout, KeepAlive: 30 * time.Second,
		}).DialContext
		config.HTTPClient = &http.Client{Transport: transport, Timeout: defaultRecordingUploadTimeout}
	}
	if config.UploadGate == nil {
		config.UploadGate = DefaultUploadGate
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.MaxAudioBytes <= 0 {
		config.MaxAudioBytes = defaultMaxAudioUploadBytes
	}
	if config.MaxErrorBodyBytes <= 0 {
		config.MaxErrorBodyBytes = defaultMaxErrorBodyBytes
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = defaultRecordingMaxRetries
	} else if config.MaxRetries < 0 {
		config.MaxRetries = 0
	}
	if config.Sleep == nil {
		config.Sleep = sleepContext
	}
	config.Metadata = cloneAttributes(config.Metadata)
	// Validate the log exporter configuration eagerly as well.
	if _, err := NewSimpleOTLPHTTPLogExporter(SimpleOTLPHTTPLogExporterConfig{
		Endpoint: logEndpoint, ScopeName: "chat_history",
		APIKey: config.APIKey, APISecret: config.APISecret,
		HTTPClient: config.HTTPClient, UploadGate: config.UploadGate,
		MaxErrorBodyBytes: config.MaxErrorBodyBytes,
	}); err != nil {
		return nil, err
	}
	return &SessionReportUploader{config: config}, nil
}

func resolveUploadEndpoints(config SessionReportUploaderConfig) (string, string, error) {
	derive := func(path string) (string, error) {
		if config.CloudHostname == "" {
			return "", errors.New("telemetry: cloud hostname is required when upload endpoints are omitted")
		}
		if strings.ContainsAny(config.CloudHostname, "/?#") {
			return "", errors.New("telemetry: cloud hostname must not contain a path, query, or fragment")
		}
		return "https://" + config.CloudHostname + path, nil
	}
	recording := config.RecordingEndpoint
	if recording == "" {
		var err error
		recording, err = derive("/observability/recordings/v0")
		if err != nil {
			return "", "", err
		}
	}
	logs := config.LogEndpoint
	if logs == "" {
		var err error
		logs, err = derive("/observability/logs/otlp/v0")
		if err != nil {
			return "", "", err
		}
	}
	for label, endpoint := range map[string]string{"recording": recording, "log": logs} {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
			return "", "", fmt.Errorf("telemetry: invalid %s endpoint %q", label, endpoint)
		}
		if parsed.User != nil {
			return "", "", fmt.Errorf("telemetry: %s endpoint credentials in URL are not allowed", label)
		}
	}
	return recording, logs, nil
}

type SessionUploadError struct {
	StatusCode int
	Status     string
	Body       string
	Attempts   int
}

func (e *SessionUploadError) Error() string {
	if e == nil {
		return "session report upload failed"
	}
	if e.Body == "" {
		return fmt.Sprintf("session report upload failed after %d attempt(s): %s", e.Attempts, e.Status)
	}
	return fmt.Sprintf("session report upload failed after %d attempt(s): %s - %s", e.Attempts, e.Status, e.Body)
}

func (u *SessionReportUploader) Upload(ctx context.Context, report *voice.SessionReport) error {
	if u == nil {
		return errors.New("telemetry: session report uploader is nil")
	}
	if report == nil {
		return errors.New("telemetry: session report is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	recording := report.Options.Recording
	if !recording.Enabled() || u.config.UploadGate.Disabled() {
		return nil
	}
	if recording.Redaction && recording.Audio && !recording.Transcript {
		return errors.New("telemetry: audio upload requires transcript upload when redaction is enabled")
	}
	reportWire, err := voice.SessionReportToJSON(report)
	if err != nil {
		return fmt.Errorf("telemetry: serialize session report: %w", err)
	}
	if err := u.exportChatLogs(ctx, report, reportWire); err != nil {
		return err
	}
	if u.config.UploadGate.Disabled() {
		return nil
	}
	audio, err := u.resolveAudio(report)
	if err != nil {
		return err
	}
	if !recording.Transcript && audio == nil {
		return nil
	}
	chatHistory, err := marshalReportChatHistory(reportWire, recording.Transcript)
	if err != nil {
		return err
	}
	header, err := u.recordingHeader(report)
	if err != nil {
		return err
	}
	return u.uploadMultipart(ctx, header, chatHistory, audio)
}

func (u *SessionReportUploader) exportChatLogs(
	ctx context.Context,
	report *voice.SessionReport,
	reportWire map[string]any,
) error {
	common := map[string]any{
		"room_id": report.RoomID, "job_id": report.JobID, "logger.name": "chat_history",
	}
	for key, value := range u.config.Metadata {
		common[key] = value
	}
	options, _ := reportWire["options"].(map[string]any)
	sessionAttributes := cloneAttributes(common)
	sessionAttributes["session.options"] = options
	sessionAttributes["session.report_timestamp"] = report.Timestamp.UnixMilli()
	sessionAttributes["agent_name"] = u.config.AgentName
	sessionAttributes["sdk_version"] = agents.Version
	sessionAttributes["usage"] = reportWire["usage"]
	started := report.StartedAt
	if started.IsZero() {
		started = report.Timestamp
	}
	records := []SimpleLogRecord{{
		Body: "session report", Timestamp: started, Attributes: sessionAttributes,
	}}
	if report.Options.Recording.Transcript && report.ChatHistory != nil {
		var last time.Time
		for index, item := range report.ChatHistory.Items() {
			if item == nil {
				continue
			}
			encoded, err := llm.ChatItemToJSON(item, false)
			if err != nil {
				return fmt.Errorf("telemetry: serialize chat item %d: %w", index, err)
			}
			var native any
			decoder := json.NewDecoder(bytes.NewReader(encoded))
			decoder.UseNumber()
			if err := decoder.Decode(&native); err != nil {
				return fmt.Errorf("telemetry: decode chat item %d: %w", index, err)
			}
			wire, err := voice.ToSnakeCaseDeep(native)
			if err != nil {
				return fmt.Errorf("telemetry: convert chat item %d: %w", index, err)
			}
			timestamp := item.ItemCreatedAt()
			if timestamp.IsZero() {
				timestamp = time.Now()
			}
			if !last.IsZero() && !timestamp.After(last) {
				timestamp = last.Add(time.Microsecond)
			}
			last = timestamp
			attributes := cloneAttributes(common)
			attributes["chat.item"] = wire
			record := SimpleLogRecord{
				Body: "chat item", Timestamp: timestamp, Attributes: attributes,
			}
			if output, ok := item.(*llm.FunctionCallOutput); ok && output.IsError {
				record.SeverityNumber = severityError
				record.SeverityText = "error"
			}
			records = append(records, record)
		}
	}
	scopeAttributes := map[string]any{
		"room_id": report.RoomID, "job_id": report.JobID, "room": report.Room,
	}
	for key, value := range u.config.Metadata {
		scopeAttributes[key] = value
	}
	exporter, err := NewSimpleOTLPHTTPLogExporter(SimpleOTLPHTTPLogExporterConfig{
		Endpoint:           u.config.LogEndpoint,
		ResourceAttributes: map[string]any{"room_id": report.RoomID, "job_id": report.JobID},
		ScopeName:          "chat_history", ScopeAttributes: scopeAttributes,
		APIKey: u.config.APIKey, APISecret: u.config.APISecret,
		HTTPClient: u.config.HTTPClient, UploadGate: u.config.UploadGate,
		MaxErrorBodyBytes: u.config.MaxErrorBodyBytes,
	})
	if err != nil {
		return err
	}
	if err := exporter.Export(ctx, records); err != nil {
		return fmt.Errorf("telemetry: export session report logs: %w", err)
	}
	return nil
}

type audioUpload struct {
	path string
	size int64
}

func (u *SessionReportUploader) resolveAudio(report *voice.SessionReport) (*audioUpload, error) {
	if !report.Options.Recording.Audio || report.AudioRecordingPath == "" || report.AudioRecordingStartedAt == nil {
		return nil, nil
	}
	info, err := os.Stat(report.AudioRecordingPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			u.config.Logger.Warn(
				"audio recording is unavailable; uploading transcript only",
				"path", report.AudioRecordingPath, "error", err,
			)
			return nil, nil
		}
		return nil, fmt.Errorf("telemetry: stat audio recording: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("telemetry: audio recording must be a regular file")
	}
	if info.Size() <= 0 {
		return nil, nil
	}
	if info.Size() > u.config.MaxAudioBytes {
		return nil, fmt.Errorf(
			"telemetry: audio recording is %d bytes, limit is %d",
			info.Size(), u.config.MaxAudioBytes,
		)
	}
	return &audioUpload{path: report.AudioRecordingPath, size: info.Size()}, nil
}

func marshalReportChatHistory(reportWire map[string]any, enabled bool) ([]byte, error) {
	if !enabled {
		return nil, nil
	}
	encoded, err := json.Marshal(reportWire["chat_history"])
	if err != nil {
		return nil, fmt.Errorf("telemetry: encode chat history: %w", err)
	}
	return encoded, nil
}

func (u *SessionReportUploader) recordingHeader(report *voice.SessionReport) ([]byte, error) {
	start := time.Time{}
	if report.AudioRecordingStartedAt != nil {
		start = *report.AudioRecordingStartedAt
	}
	duration := report.Duration
	if duration < 0 {
		duration = 0
	}
	header := &livekit.MetricsRecordingHeader{
		RoomId:    report.RoomID,
		JobId:     report.JobID,
		Duration:  uint64(duration / time.Millisecond),
		Simulated: metadataBool(u.config.Metadata, agents.AttributeSimulationEnabled),
		RedactionEnabled: report.Options.Recording.Redaction ||
			metadataBool(u.config.Metadata, agents.AttributeRedactionEnabled),
	}
	if !start.IsZero() {
		header.StartTime = timestamppb.New(start)
		if err := header.StartTime.CheckValid(); err != nil {
			return nil, fmt.Errorf("telemetry: invalid recording start time: %w", err)
		}
	}
	encoded, err := proto.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("telemetry: encode recording header: %w", err)
	}
	return encoded, nil
}

func metadataBool(metadata map[string]any, key string) bool {
	value, _ := metadata[key].(bool)
	return value
}

type multipartUpload struct {
	boundary string
	prefix   []byte
	tail     []byte
	audio    *audioUpload
	size     int64
}

func buildRecordingMultipart(header, chatHistory []byte, audio *audioUpload) (*multipartUpload, error) {
	random := make([]byte, 18)
	if _, err := rand.Read(random); err != nil {
		return nil, fmt.Errorf("telemetry: create multipart boundary: %w", err)
	}
	boundary := "livekit-agents-" + hex.EncodeToString(random)
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	if err := writer.SetBoundary(boundary); err != nil {
		return nil, err
	}
	writePart := func(name, filename, contentType string, payload []byte) error {
		header := make(textproto.MIMEHeader)
		header.Set("Content-Disposition", fmt.Sprintf("form-data; name=%q; filename=%q", name, filename))
		header.Set("Content-Type", contentType)
		header.Set("Content-Length", strconv.Itoa(len(payload)))
		part, err := writer.CreatePart(header)
		if err != nil {
			return err
		}
		_, err = part.Write(payload)
		return err
	}
	if err := writePart("header", "header.binpb", "application/protobuf", header); err != nil {
		return nil, err
	}
	if chatHistory != nil {
		if err := writePart("chat_history", "chat_history.json", "application/json", chatHistory); err != nil {
			return nil, err
		}
	}
	prefixEnd := buffer.Len()
	if audio != nil {
		header := make(textproto.MIMEHeader)
		header.Set("Content-Disposition", "form-data; name=\"audio\"; filename=\"recording.ogg\"")
		header.Set("Content-Type", "audio/ogg")
		header.Set("Content-Length", strconv.FormatInt(audio.size, 10))
		if _, err := writer.CreatePart(header); err != nil {
			return nil, err
		}
		prefixEnd = buffer.Len()
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	all := append([]byte(nil), buffer.Bytes()...)
	result := &multipartUpload{
		boundary: boundary, prefix: all, audio: audio, size: int64(len(all)),
	}
	if audio != nil {
		result.prefix = all[:prefixEnd]
		result.tail = all[prefixEnd:]
		result.size += audio.size
	}
	return result, nil
}

func (m *multipartUpload) reader() (io.ReadCloser, error) {
	if m.audio == nil {
		return io.NopCloser(bytes.NewReader(m.prefix)), nil
	}
	file, err := os.Open(m.audio.path)
	if err != nil {
		return nil, err
	}
	return &multipartReadCloser{
		Reader: io.MultiReader(
			bytes.NewReader(m.prefix),
			io.LimitReader(file, m.audio.size),
			bytes.NewReader(m.tail),
		),
		file: file,
	}, nil
}

type multipartReadCloser struct {
	io.Reader
	file *os.File
}

func (r *multipartReadCloser) Close() error { return r.file.Close() }

func (u *SessionReportUploader) uploadMultipart(
	ctx context.Context,
	header, chatHistory []byte,
	audio *audioUpload,
) error {
	body, err := buildRecordingMultipart(header, chatHistory, audio)
	if err != nil {
		return fmt.Errorf("telemetry: build recording multipart: %w", err)
	}
	token, err := u.ensureToken()
	if err != nil {
		return err
	}
	generation := u.config.UploadGate.Generation()
	for attempt := 0; attempt <= u.config.MaxRetries; attempt++ {
		reader, err := body.reader()
		if err != nil {
			return fmt.Errorf("telemetry: open recording multipart: %w", err)
		}
		request, err := http.NewRequestWithContext(
			ctx, http.MethodPost, u.config.RecordingEndpoint, reader,
		)
		if err != nil {
			_ = reader.Close()
			return fmt.Errorf("telemetry: build recording request: %w", err)
		}
		request.ContentLength = body.size
		request.Header.Set("Content-Type", "multipart/form-data; boundary="+body.boundary)
		request.Header.Set("Authorization", "Bearer "+token)
		response, requestErr := u.config.HTTPClient.Do(request)
		_ = reader.Close()
		if requestErr != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			if attempt == u.config.MaxRetries {
				return fmt.Errorf(
					"telemetry: recording upload request after %d attempt(s): %w",
					attempt+1, requestErr,
				)
			}
			if err := u.config.Sleep(ctx, fallbackRetryDelay(attempt)); err != nil {
				return err
			}
			continue
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(
			response.Body, u.config.MaxErrorBodyBytes+1,
		))
		_ = response.Body.Close()
		if readErr != nil {
			return fmt.Errorf("telemetry: read recording upload response: %w", readErr)
		}
		if u.config.UploadGate.IsDisabledResponse(response.StatusCode, responseBody) {
			u.config.UploadGate.Disable(generation)
			return nil
		}
		if response.StatusCode >= 200 && response.StatusCode < 400 {
			return nil
		}
		if attempt < u.config.MaxRetries && retryableRecordingStatus(response.StatusCode) {
			delay := recordingRetryDelay(response, responseBody, attempt)
			if err := u.config.Sleep(ctx, delay); err != nil {
				return err
			}
			continue
		}
		truncated := int64(len(responseBody)) > u.config.MaxErrorBodyBytes
		if truncated {
			responseBody = responseBody[:u.config.MaxErrorBodyBytes]
		}
		message := strings.TrimSpace(string(responseBody))
		if truncated {
			message += "…"
		}
		return &SessionUploadError{
			StatusCode: response.StatusCode, Status: response.Status,
			Body: message, Attempts: attempt + 1,
		}
	}
	return errors.New("telemetry: recording upload exhausted without response")
}

func (u *SessionReportUploader) ensureToken() (string, error) {
	u.tokenMu.Lock()
	defer u.tokenMu.Unlock()
	if u.token != "" {
		return u.token, nil
	}
	apiKey := u.config.APIKey.Reveal()
	if apiKey == "" {
		apiKey = os.Getenv("LIVEKIT_API_KEY")
	}
	apiSecret := u.config.APISecret.Reveal()
	if apiSecret == "" {
		apiSecret = os.Getenv("LIVEKIT_API_SECRET")
	}
	if apiKey == "" || apiSecret == "" {
		return "", errors.New(
			"telemetry: LIVEKIT_API_KEY and LIVEKIT_API_SECRET must be set for session upload",
		)
	}
	token, err := auth.NewAccessToken(apiKey, apiSecret).
		SetIdentity("livekit-agents-telemetry").
		SetValidFor(6 * time.Hour).
		SetObservabilityGrant(&auth.ObservabilityGrant{Write: true}).
		ToJWT()
	if err != nil {
		return "", fmt.Errorf("telemetry: create session upload token: %w", err)
	}
	u.token = token
	return token, nil
}

func retryableRecordingStatus(status int) bool {
	return status == http.StatusTooManyRequests ||
		status == http.StatusRequestTimeout ||
		status == http.StatusConflict ||
		status >= 500
}

func recordingRetryDelay(response *http.Response, body []byte, attempt int) time.Duration {
	if response != nil {
		if value := strings.TrimSpace(response.Header.Get("Retry-After")); value != "" {
			if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds >= 0 {
				return time.Duration(seconds * float64(time.Second))
			}
			if when, err := http.ParseTime(value); err == nil {
				return max(0, time.Until(when))
			}
		}
	}
	if delay := parseGoogleRetryInfo(body); delay >= 0 {
		return delay
	}
	return fallbackRetryDelay(attempt)
}

func fallbackRetryDelay(attempt int) time.Duration {
	if attempt > 6 {
		attempt = 6
	}
	return (100 * time.Millisecond) << attempt
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
