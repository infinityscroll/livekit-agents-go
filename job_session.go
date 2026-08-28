// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

var (
	// ErrPrimarySessionRecording is returned when a secondary session
	// explicitly enables recording. Exactly one session owns recording and the
	// automatic shutdown/report lifecycle for a job.
	ErrPrimarySessionRecording = errors.New("agents: only the primary session may enable recording")
	ErrJobSessionFinishing     = errors.New("agents: job session lifecycle is already finishing")
)

// JobSessionLifecycle is the import-cycle-free bridge between JobContext and
// a high-level agent session. CloseSession runs before the room disconnects;
// FinalizeSession then persists/uploads its report. A finalization failure is
// logged and does not turn an otherwise successful agent job into a failed
// assignment, matching the TypeScript/Python lifecycle.
type JobSessionLifecycle interface {
	CloseSession(context.Context) error
	FinalizeSession(context.Context) error
}

// JobSessionLifecycleFuncs adapts functions without requiring a wrapper type.
type JobSessionLifecycleFuncs struct {
	Close    func(context.Context) error
	Finalize func(context.Context) error
}

func (f JobSessionLifecycleFuncs) CloseSession(ctx context.Context) error {
	if f.Close == nil {
		return nil
	}
	return f.Close(ctx)
}

func (f JobSessionLifecycleFuncs) FinalizeSession(ctx context.Context) error {
	if f.Finalize == nil {
		return nil
	}
	return f.Finalize(ctx)
}

// JobSessionRegistrationOptions controls the primary-session claim. Recording
// inherits livekit.Job.enable_recording when Recording is zero-valued. Use
// Use(false) or Disable[bool]() to explicitly disable it.
type JobSessionRegistrationOptions struct {
	Lifecycle JobSessionLifecycle
	Recording Override[bool]
}

// JobSessionRegistration is the resolved claim returned to a session. Release
// rolls back a primary claim when Start fails; it is idempotent.
type JobSessionRegistration struct {
	Primary          bool
	RecordingEnabled bool
	RedactionEnabled bool

	owner jobSessionRegistrationOwner
	id    uint64
	once  sync.Once
}

type jobSessionRegistrationOwner interface{ releaseJobSession(uint64) }

// Release rolls back this registration. Successful primary sessions normally
// remain registered until JobContext shutdown so the job runner can close and
// finalize them in the correct order.
func (r *JobSessionRegistration) Release() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.owner != nil && r.id != 0 {
			r.owner.releaseJobSession(r.id)
		}
	})
}

type registeredJobSession struct {
	id        uint64
	lifecycle JobSessionLifecycle
}

// RegisterSession claims the primary session slot atomically. An omitted
// recording choice inherits the dispatch setting. A secondary omitted choice
// is silently demoted to recording-off; an explicit recording-on choice fails.
func (j *JobContext[T]) RegisterSession(options JobSessionRegistrationOptions) (*JobSessionRegistration, error) {
	if j == nil {
		return nil, errors.New("agents: cannot register a session on a nil JobContext")
	}
	if options.Lifecycle == nil || isNilJobSessionLifecycle(options.Lifecycle) {
		return nil, errors.New("agents: job session lifecycle is required")
	}

	recording, explicit := options.Recording.Value()
	if options.Recording.IsDisabled() {
		recording, explicit = false, true
	} else if options.Recording.IsInherited() {
		recording = j.info.Job.GetEnableRecording()
	}
	redaction := j.info.Job.GetEnableRedaction()

	j.sessionMu.Lock()
	defer j.sessionMu.Unlock()
	if j.sessionFinishing || j.ctx.Err() != nil {
		return nil, ErrJobSessionFinishing
	}
	if j.primarySession.lifecycle != nil {
		if recording && explicit {
			return nil, ErrPrimarySessionRecording
		}
		return &JobSessionRegistration{
			Primary: false, RecordingEnabled: false, RedactionEnabled: redaction,
		}, nil
	}
	j.nextSessionID++
	if j.nextSessionID == 0 {
		j.nextSessionID++
	}
	id := j.nextSessionID
	j.primarySession = registeredJobSession{id: id, lifecycle: options.Lifecycle}
	return &JobSessionRegistration{
		Primary: true, RecordingEnabled: recording, RedactionEnabled: redaction,
		owner: j, id: id,
	}, nil
}

func isNilJobSessionLifecycle(value JobSessionLifecycle) bool {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (j *JobContext[T]) releaseJobSession(id uint64) {
	if j == nil || id == 0 {
		return
	}
	j.sessionMu.Lock()
	if !j.sessionFinishing && j.primarySession.id == id {
		j.primarySession = registeredJobSession{}
	}
	j.sessionMu.Unlock()
}

func (j *JobContext[T]) finishPrimarySession(ctx context.Context) error {
	j.sessionMu.Lock()
	j.sessionFinishing = true
	registered := j.primarySession
	j.sessionMu.Unlock()
	if registered.lifecycle == nil {
		return nil
	}
	closeErr := invokeJobSessionStage(ctx, "close", registered.lifecycle.CloseSession)
	if finalErr := invokeJobSessionStage(ctx, "finalize", registered.lifecycle.FinalizeSession); finalErr != nil {
		jobID := ""
		if j.info.Job != nil {
			jobID = j.info.Job.GetId()
		}
		slog.Error("failed to finalize agent session", "job_id", jobID, "error", finalErr)
	}
	return closeErr
}

func invokeJobSessionStage(ctx context.Context, stage string, fn func(context.Context) error) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("agents: job session %s panicked: %v", stage, recovered)
		}
	}()
	if err := fn(ctx); err != nil {
		return fmt.Errorf("agents: job session %s: %w", stage, err)
	}
	return nil
}

func resolveSessionDirectory(info RunningJobInfo) string {
	if info.SessionDirectory != "" {
		return filepath.Clean(info.SessionDirectory)
	}
	jobID := "unknown"
	if info.Job != nil && info.Job.GetId() != "" {
		jobID = sanitizeSessionPathComponent(info.Job.GetId())
	}
	return filepath.Join(os.TempDir(), "livekit-agents", "job-"+jobID)
}

func sanitizeSessionPathComponent(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	var result strings.Builder
	result.Grow(min(len(value), 128))
	for _, r := range value {
		if result.Len() >= 128 {
			break
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
			result.WriteRune(r)
		} else {
			result.WriteByte('_')
		}
	}
	clean := result.String()
	if clean == "" {
		return "unknown"
	}
	return clean
}
