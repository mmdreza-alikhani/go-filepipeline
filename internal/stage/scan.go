package stage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"example.com/filepipeline/internal/config"
	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
)

// ScanOptions configures the scan stage.
type ScanOptions struct {
	MaxFileSize int64
	TempRoot    string
	Probe       OutputProbe
	Log         *slog.Logger
	Metrics     *obs.Metrics
	Clock       job.Clock
}

// Scan opens the input, validates the name, enforces the size ceiling, hashes
// the content and creates the per job scratch directory.
//
// It is IO bound, so its pool is sized for latency hiding rather than for cores.
type Scan struct {
	o ScanOptions
}

func newScanStage(d Deps, _ config.StageConfig) (Stage, error) {
	if d.Probe == nil {
		return nil, errors.New("scan: an output probe is required for idempotency")
	}
	if d.Config.MaxFileSize <= 0 {
		return nil, errors.New("scan: max_file_size must be positive")
	}
	return &Scan{o: ScanOptions{
		MaxFileSize: d.Config.MaxFileSize,
		TempRoot:    d.Config.TempRoot(),
		Probe:       d.Probe,
		Log:         d.Log,
		Metrics:     d.Metrics,
		Clock:       d.Clock,
	}}, nil
}

// Name implements Stage.
func (s *Scan) Name() job.Stage { return job.StageScan }

// Kind implements Stage.
func (s *Scan) Kind() Kind { return KindIO }

// Process implements Stage.
func (s *Scan) Process(ctx context.Context, j *job.Job) (*job.Job, error) {
	if j == nil {
		return nil, errors.New("scan: nil job")
	}
	if _, err := job.ParseFileName(j.FileName); err != nil {
		// The source already filters names, so this only fires for jobs injected
		// by another path (replay of a corrupt journal). It is permanent.
		return nil, Permanent(fmt.Errorf("scan: %s: %w", j.FileName, err))
	}

	f, err := os.Open(j.SourcePath)
	if err != nil {
		switch {
		case errors.Is(err, fs.ErrNotExist):
			// The file disappeared between listing and scanning. Dropping it is
			// the only correct action; retrying would never succeed.
			j.MarkDone(s.o.Clock.Now())
			j.Error = "source vanished before scan"
			s.o.Log.WarnContext(ctx, "source vanished before scan", "job", j.ID, "path", j.SourcePath)
			return j, ErrSkip
		case IsDiskError(err):
			return nil, Fatal(err)
		default:
			return nil, fmt.Errorf("scan: open %q: %w", j.FileName, err)
		}
	}
	defer func() { _ = f.Close() }()

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("scan: stat %q: %w", j.FileName, err)
	}
	if !st.Mode().IsRegular() {
		j.MarkDone(s.o.Clock.Now())
		j.Error = "not a regular file"
		return j, ErrSkip
	}
	if st.Size() > s.o.MaxFileSize {
		return nil, Permanent(fmt.Errorf("scan: %q: %w: %d > %d", j.FileName, ErrFileTooLarge, st.Size(), s.o.MaxFileSize))
	}

	// Hash at most max+1 bytes: one byte past the ceiling is enough to prove the
	// file grew after stat, without ever buffering the content.
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(ctxReader{ctx: ctx, r: f}, s.o.MaxFileSize+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if IsDiskError(err) {
			return nil, Fatal(err)
		}
		return nil, fmt.Errorf("scan: read %q: %w", j.FileName, err)
	}
	if n > s.o.MaxFileSize {
		return nil, Permanent(fmt.Errorf("scan: %q: %w: grew past %d bytes", j.FileName, ErrFileTooLarge, s.o.MaxFileSize))
	}

	j.Hash = hex.EncodeToString(h.Sum(nil))
	j.Size = n

	published, err := s.o.Probe.Published(j.Hash)
	if err != nil {
		return nil, Fatal(fmt.Errorf("scan: probe output for %s: %w", j.Hash, err))
	}
	if published {
		// Content addressed output already exists: this is the idempotency hit.
		j.MarkDone(s.o.Clock.Now())
		s.o.Metrics.IncCounter("pipeline_jobs_skipped_total", "Jobs skipped because their output already existed.",
			map[string]string{"stage": string(job.StageScan)}, 1)
		s.o.Log.InfoContext(ctx, "skipping already published content", "job", j.ID, "hash", j.Hash)
		return j, ErrSkip
	}

	if err := os.MkdirAll(filepath.Join(s.o.TempRoot, j.ID), 0o755); err != nil {
		if IsDiskError(err) {
			return nil, Fatal(err)
		}
		return nil, fmt.Errorf("scan: create scratch dir: %w", err)
	}
	j.TempDir = filepath.Join(s.o.TempRoot, j.ID)
	j.MarkStageDone(job.StageScan, s.o.Clock.Now())

	s.o.Metrics.IncCounter("pipeline_bytes_processed_total", "Bytes read by a stage.",
		map[string]string{"stage": string(job.StageScan)}, float64(n))
	return j, nil
}
