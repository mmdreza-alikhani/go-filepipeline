package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
	"example.com/filepipeline/internal/stage"
)

// DeadLetterOptions configures the failure sink.
type DeadLetterOptions struct {
	Dir     string
	Log     *slog.Logger
	Metrics *obs.Metrics
	Clock   job.Clock
}

// DeadLetter writes failed/<job>.json and moves the offending input next to it,
// so an operator has the data and the diagnosis in one place.
type DeadLetter struct {
	o      DeadLetterOptions
	tmpDir string
}

// NewDeadLetter prepares the failed directory.
func NewDeadLetter(o DeadLetterOptions) (*DeadLetter, error) {
	if o.Dir == "" {
		return nil, errors.New("sink: failed directory must not be empty")
	}
	if o.Clock == nil {
		o.Clock = job.RealClock{}
	}
	if o.Log == nil {
		o.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.Metrics == nil {
		o.Metrics = obs.NewMetrics()
	}
	tmp := filepath.Join(o.Dir, ".tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, fmt.Errorf("sink: create failed dir: %w", err)
	}
	return &DeadLetter{o: o, tmpDir: tmp}, nil
}

// Failure is the JSON document written for every dead lettered job.
type Failure struct {
	JobID           string            `json:"job_id"`
	TraceID         string            `json:"trace_id"`
	FileName        string            `json:"file_name"`
	SourcePath      string            `json:"source_path"`
	Size            int64             `json:"size"`
	Hash            string            `json:"hash,omitempty"`
	Stage           job.Stage         `json:"stage"`
	Attempts        int               `json:"attempts"`
	Permanent       bool              `json:"permanent"`
	Error           string            `json:"error"`
	At              time.Time         `json:"at"`
	AttemptsByStage map[job.Stage]int `json:"attempts_by_stage,omitempty"`
}

// Fail records the job. It is deliberately context insensitive once started:
// the dead letter is the last chance to keep the evidence, so a cancelled
// pipeline still finishes the record.
func (d *DeadLetter) Fail(ctx context.Context, j *job.Job, st job.Stage, cause error) error {
	if j == nil {
		return errors.New("sink: nil job in dead letter")
	}
	now := d.o.Clock.Now()
	rec := Failure{
		JobID:           j.ID,
		TraceID:         j.TraceID,
		FileName:        j.FileName,
		SourcePath:      j.SourcePath,
		Size:            j.Size,
		Hash:            j.Hash,
		Stage:           st,
		Attempts:        j.Attempt(st),
		Permanent:       stage.IsPermanent(cause),
		Error:           errText(cause),
		At:              now,
		AttemptsByStage: cloneAttempts(j.Attempts),
	}
	if err := d.writeRecord(j, rec); err != nil {
		return err
	}
	d.moveSource(j)
	d.cleanup(j)

	d.o.Metrics.IncCounter("pipeline_jobs_failed_total", "Jobs that ended in the dead letter queue.",
		map[string]string{"stage": string(st)}, 1)
	if ctx.Err() != nil {
		d.o.Log.Warn("dead letter recorded during cancellation", "job", j.ID, "stage", st)
	}
	return nil
}

func (d *DeadLetter) writeRecord(j *job.Job, rec Failure) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("sink: marshal failure record: %w", err)
	}
	tmp := filepath.Join(d.tmpDir, j.ID+".json.part")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("sink: create failure record: %w", err)
	}
	_, werr := f.Write(data)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("sink: write failure record: %w", werr)
	}
	final := filepath.Join(d.o.Dir, j.ID+".json")
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("sink: publish failure record: %w", err)
	}
	syncDir(d.o.Dir)
	return nil
}

// moveSource relocates the input file. A failure here is logged but does not
// mask the record: the operator still gets the JSON describing the job.
func (d *DeadLetter) moveSource(j *job.Job) {
	if j.SourcePath == "" {
		return
	}
	if _, err := os.Stat(j.SourcePath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return
		}
		d.o.Log.Warn("could not stat failed input", "job", j.ID, "err", err)
		return
	}
	dest := filepath.Join(d.o.Dir, j.ID+"__"+job.SafeFileName(j.FileName))
	if err := os.Rename(j.SourcePath, dest); err != nil {
		d.o.Log.Warn("could not move failed input", "job", j.ID, "err", err)
	}
}

func (d *DeadLetter) cleanup(j *job.Job) {
	if j.TempDir == "" {
		return
	}
	if err := os.RemoveAll(j.TempDir); err != nil {
		d.o.Log.Warn("could not remove scratch directory", "job", j.ID, "err", err)
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func cloneAttempts(in map[job.Stage]int) map[job.Stage]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[job.Stage]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
