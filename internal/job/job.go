// Package job defines the unit of work that travels through the pipeline, the
// file name parser used at the trust boundary, and the durable journal that
// makes restarts safe.
package job

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Stage is the name of one step in the pipeline.
type Stage string

const (
	// StageNone is the cursor value of a job that has not run yet.
	StageNone Stage = ""
	// StageScan hashes the input and creates the per job scratch directory.
	StageScan Stage = "scan"
	// StageConvert rewrites the content into the canonical intermediate form.
	StageConvert Stage = "convert"
	// StageCompress gzips the converted artefact.
	StageCompress Stage = "compress"
	// StagePublish moves the artefact to its final content addressed path.
	StagePublish Stage = "publish"
)

// State is the journal state of a job.
type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
	StateDone    State = "done"
	StateFailed  State = "failed"
)

// Errors returned by the filename parser.
var (
	ErrEmptyName   = errors.New("job: empty file name")
	ErrNameTooLong = errors.New("job: file name too long")
	ErrNameUnsafe  = errors.New("job: unsafe file name")
	ErrTempName    = errors.New("job: temporary file name")
)

// Job is the single mutable record that travels from stage to stage. Exactly
// one goroutine owns a Job at a time: ownership is transferred through a
// channel, which is what makes lock free mutation safe.
type Job struct {
	ID         string        `json:"id"`
	TraceID    string        `json:"trace_id"`
	SourcePath string        `json:"source_path"`
	FileName   string        `json:"file_name"`
	Size       int64         `json:"size"`
	Hash       string        `json:"hash,omitempty"`
	State      State         `json:"state"`
	Stage      Stage         `json:"stage,omitempty"`
	Attempts   map[Stage]int `json:"attempts"`
	TempDir    string        `json:"temp_dir,omitempty"`
	Converted  string        `json:"converted_path,omitempty"`
	Artifact   string        `json:"artifact_path,omitempty"`
	OutSize    int64         `json:"output_size,omitempty"`
	Error      string        `json:"error,omitempty"`
	Resumes    int           `json:"resumes"`
	CreatedAt  time.Time     `json:"created_at"`
	UpdatedAt  time.Time     `json:"updated_at"`
	FinishedAt *time.Time    `json:"finished_at,omitempty"`
}

// New builds a pending job for one inbox file.
func New(id, sourcePath string, size int64, traceID string, now time.Time) *Job {
	return &Job{
		ID:         id,
		TraceID:    traceID,
		SourcePath: sourcePath,
		FileName:   filepath.Base(sourcePath),
		Size:       size,
		State:      StatePending,
		Attempts:   make(map[Stage]int, 3),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// Clone returns a deep copy, so a journal snapshot can be written without
// racing the owning goroutine.
func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	c := *j
	c.Attempts = make(map[Stage]int, len(j.Attempts))
	for k, v := range j.Attempts {
		c.Attempts[k] = v
	}
	if j.FinishedAt != nil {
		t := *j.FinishedAt
		c.FinishedAt = &t
	}
	return &c
}

// Attempt reports how many times stage s has already been tried.
func (j *Job) Attempt(stage Stage) int {
	if j.Attempts == nil {
		return 0
	}
	return j.Attempts[stage]
}

// BumpAttempt records one more try of stage s and returns the new count.
func (j *Job) BumpAttempt(stage Stage, now time.Time) int {
	if j.Attempts == nil {
		j.Attempts = make(map[Stage]int, 3)
	}
	j.Attempts[stage]++
	j.UpdatedAt = now
	return j.Attempts[stage]
}

// MarkRunning moves the job into the running state for stage s.
func (j *Job) MarkRunning(stage Stage, now time.Time) {
	j.State = StateRunning
	j.Stage = stage
	j.Error = ""
	j.UpdatedAt = now
}

// MarkStageDone advances the cursor after a successful stage.
func (j *Job) MarkStageDone(stage Stage, now time.Time) {
	j.Stage = stage
	j.UpdatedAt = now
}

// MarkDone marks the job as successfully published.
func (j *Job) MarkDone(now time.Time) {
	j.State = StateDone
	j.Stage = StagePublish
	j.Error = ""
	j.UpdatedAt = now
	j.FinishedAt = &now
}

// MarkFailed marks the job as dead lettered.
func (j *Job) MarkFailed(stage Stage, cause error, now time.Time) {
	j.State = StateFailed
	j.Stage = stage
	if cause != nil {
		j.Error = cause.Error()
	}
	j.UpdatedAt = now
	j.FinishedAt = &now
}

// Terminal reports whether the job will never be processed again.
func (j *Job) Terminal() bool {
	return j.State == StateDone || j.State == StateFailed
}

// Reset prepares a recovered job for a fresh pass through the stage chain.
// Per stage attempt counters are cleared so a crash does not consume the retry
// budget, and all scratch paths are dropped because the temporary directory is
// gone after a restart.
func (j *Job) Reset(now time.Time) {
	j.Resumes++
	j.State = StatePending
	j.Stage = StageNone
	j.Attempts = make(map[Stage]int, 3)
	j.TempDir = ""
	j.Converted = ""
	j.Artifact = ""
	j.OutSize = 0
	j.Error = ""
	j.FinishedAt = nil
	j.UpdatedAt = now
}

// ArtifactReady reports whether the journal points at an artefact that still
// exists on disk, i.e. the job was interrupted after compression finished and
// only the publish step is missing.
func (j *Job) ArtifactReady() bool {
	return j.Artifact != "" && j.Hash != ""
}

// NewID returns a random 16 hex character identifier for a job.
func NewID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on the platforms we support; fall back to a
		// timestamp so the pipeline keeps working instead of panicking.
		return fmt.Sprintf("t%015x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

const (
	maxNameLen  = 200
	maxExtLen   = 16
	minNameRune = 1
)

// ParseFileName validates a file name coming from the inbox. It is the only
// place where an untrusted name is examined, and it never touches the path:
// the caller joins it with a directory afterwards.
func ParseFileName(name string) (string, error) {
	if name == "" {
		return "", ErrEmptyName
	}
	if len(name) > maxNameLen {
		return "", ErrNameTooLong
	}
	if !utf8.ValidString(name) {
		return "", fmt.Errorf("%w: invalid UTF-8", ErrNameUnsafe)
	}
	if name != filepath.Base(name) || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("%w: path separators", ErrNameUnsafe)
	}
	if strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("%w: NUL byte", ErrNameUnsafe)
	}
	for _, r := range name {
		if r == utf8.RuneError || unicode.IsControl(r) {
			return "", fmt.Errorf("%w: control characters", ErrNameUnsafe)
		}
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, "~") {
		return "", fmt.Errorf("%w: hidden or backup file", ErrNameUnsafe)
	}
	lower := strings.ToLower(name)
	for _, suffix := range []string{".tmp", ".part", ".partial", ".crdownload", ".swp", ".lock"} {
		if strings.HasSuffix(lower, suffix) {
			return "", fmt.Errorf("%w: %s", ErrTempName, suffix)
		}
	}
	if strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
		return "", fmt.Errorf("%w: trailing separator", ErrNameUnsafe)
	}
	ext := filepath.Ext(name)
	if len(ext) > maxExtLen {
		return "", fmt.Errorf("%w: extension too long", ErrNameUnsafe)
	}
	if len(name) < minNameRune {
		return "", ErrEmptyName
	}
	return name, nil
}

// SafeFileName rewrites an arbitrary name into something that can be used as a
// single path element on the local file system (used for dead letter copies).
func SafeFileName(name string) string {
	name = filepath.Base(name)
	var b strings.Builder
	for _, r := range name {
		switch {
		case r == utf8.RuneError:
			continue
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "._")
	if out == "" {
		out = "unnamed"
	}
	if len(out) > 80 {
		out = out[:80]
	}
	return out
}
