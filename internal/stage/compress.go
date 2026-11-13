package stage

import (
	"compress/gzip"
	"context"
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

// CompressOptions configures the compress stage.
type CompressOptions struct {
	Level   int
	Log     *slog.Logger
	Metrics *obs.Metrics
	Clock   job.Clock
}

// Compress gzips the converted artefact into the job's scratch directory. The
// result is not visible to consumers until the sink renames it into out/.
type Compress struct {
	o CompressOptions
}

func newCompressStage(d Deps, _ config.StageConfig) (Stage, error) {
	level := d.Config.Compress.GzipLevel()
	if level < -1 || level > 9 {
		return nil, fmt.Errorf("compress: invalid gzip level %d", level)
	}
	return &Compress{o: CompressOptions{
		Level:   level,
		Log:     d.Log,
		Metrics: d.Metrics,
		Clock:   d.Clock,
	}}, nil
}

// Name implements Stage.
func (c *Compress) Name() job.Stage { return job.StageCompress }

// Kind implements Stage: deflate is CPU bound.
func (c *Compress) Kind() Kind { return KindCPU }

// Process implements Stage.
func (c *Compress) Process(ctx context.Context, j *job.Job) (*job.Job, error) {
	if j == nil {
		return nil, errors.New("compress: nil job")
	}
	if j.Converted == "" {
		return nil, Fatal(errors.New("compress: job has no converted artefact (convert must run first)"))
	}

	in, err := os.Open(j.Converted)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, Permanent(fmt.Errorf("compress: converted artefact missing: %w", err))
		}
		if IsDiskError(err) {
			return nil, Fatal(err)
		}
		return nil, fmt.Errorf("compress: open converted artefact: %w", err)
	}
	defer func() { _ = in.Close() }()

	dir := filepath.Join(j.TempDir, "compress")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		if IsDiskError(err) {
			return nil, Fatal(err)
		}
		return nil, fmt.Errorf("compress: create scratch dir: %w", err)
	}
	out := filepath.Join(dir, "artifact.gz")

	written, err := c.compress(ctx, in, out)
	if err != nil {
		_ = os.Remove(out)
		if IsDiskError(err) {
			return nil, Fatal(err)
		}
		return nil, err
	}

	j.Artifact = out
	j.OutSize = written
	j.MarkStageDone(job.StageCompress, c.o.Clock.Now())
	c.o.Metrics.IncCounter("pipeline_bytes_processed_total", "Bytes read by a stage.",
		map[string]string{"stage": string(job.StageCompress)}, float64(j.Size))
	c.o.Metrics.IncCounter("pipeline_bytes_written_total", "Bytes written to the scratch area.",
		map[string]string{"stage": string(job.StageCompress)}, float64(written))
	return j, nil
}

func (c *Compress) compress(ctx context.Context, in io.Reader, out string) (int64, error) {
	f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, fmt.Errorf("compress: create artefact: %w", err)
	}
	defer func() { _ = f.Close() }()

	gz, err := gzip.NewWriterLevel(f, c.o.Level)
	if err != nil {
		return 0, fmt.Errorf("compress: gzip writer: %w", err)
	}
	// gzip.NewWriterLevel leaves the header time stamp zero, so the same input
	// and level always produce byte identical output: a useful property for a
	// content addressed sink and for reproducible tests.
	if _, err := io.Copy(gz, ctxReader{ctx: ctx, r: in}); err != nil {
		_ = gz.Close()
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, fmt.Errorf("compress: deflate: %w", err)
	}
	if err := gz.Close(); err != nil {
		return 0, fmt.Errorf("compress: close gzip: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("compress: sync artefact: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("compress: stat artefact: %w", err)
	}
	return fi.Size(), nil
}
