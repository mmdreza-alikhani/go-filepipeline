// Package sink is the exit of the pipeline: it publishes artefacts atomically
// into out/ and records unrecoverable jobs in failed/.
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
)

// PublisherOptions configures the publisher.
type PublisherOptions struct {
	OutDir  string
	Log     *slog.Logger
	Metrics *obs.Metrics
	Clock   job.Clock
}

// Publisher places artefacts at out/<hash[0:2]>/<hash>.gz plus a JSON manifest.
//
// Every write happens in out/.tmp first (same file system), then a rename makes
// the file visible. A reader of out/ therefore never observes a partial file,
// which is also what makes re-publishing the same content a no-op.
type Publisher struct {
	o      PublisherOptions
	tmpDir string
}

// NewPublisher prepares the output directory and its staging area.
func NewPublisher(o PublisherOptions) (*Publisher, error) {
	if o.OutDir == "" {
		return nil, errors.New("sink: output directory must not be empty")
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
	tmp := filepath.Join(o.OutDir, ".tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, fmt.Errorf("sink: create staging dir: %w", err)
	}
	return &Publisher{o: o, tmpDir: tmp}, nil
}

// FinalPath is the content addressed location of one artefact.
func FinalPath(outDir, hash string) string {
	return filepath.Join(outDir, shard(hash), hash+".gz")
}

// ManifestPath is the metadata document that sits next to an artefact.
func ManifestPath(outDir, hash string) string {
	return filepath.Join(outDir, shard(hash), hash+".json")
}

func shard(hash string) string {
	if len(hash) < 2 {
		return "__"
	}
	return hash[:2]
}

func validHash(hash string) error {
	if len(hash) < 4 || len(hash) > 128 {
		return fmt.Errorf("sink: invalid content hash %q", hash)
	}
	for _, r := range hash {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f', r >= 'A' && r <= 'F':
		default:
			return fmt.Errorf("sink: non hexadecimal content hash %q", hash)
		}
	}
	return nil
}

// Published implements stage.OutputProbe: both the artefact and its manifest
// must exist for the content to count as published.
func (p *Publisher) Published(hash string) (bool, error) {
	if err := validHash(hash); err != nil {
		return false, err
	}
	for _, path := range []string{FinalPath(p.o.OutDir, hash), ManifestPath(p.o.OutDir, hash)} {
		fi, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return false, nil
			}
			return false, fmt.Errorf("sink: stat %s: %w", path, err)
		}
		if !fi.Mode().IsRegular() || fi.Size() == 0 {
			return false, nil
		}
	}
	return true, nil
}

// Manifest is the metadata written next to every artefact.
type Manifest struct {
	JobID      string            `json:"job_id"`
	TraceID    string            `json:"trace_id"`
	Hash       string            `json:"hash"`
	FileName   string            `json:"file_name"`
	SourcePath string            `json:"source_path"`
	InputSize  int64             `json:"input_size"`
	OutputSize int64             `json:"output_size"`
	Attempts   map[job.Stage]int `json:"attempts"`
	CreatedAt  time.Time         `json:"created_at"`
	FinishedAt time.Time         `json:"finished_at"`
}

// Publish copies the job's artefact into out/ atomically. It does not touch the
// journal: the pipeline marks the job done afterwards, which is why an
// interrupted publish is safely repeated on the next start.
func (p *Publisher) Publish(ctx context.Context, j *job.Job) error {
	if j == nil {
		return errors.New("sink: nil job")
	}
	if err := validHash(j.Hash); err != nil {
		return err
	}
	if j.Artifact == "" {
		return fmt.Errorf("sink: job %s has no artefact to publish", j.ID)
	}

	published, err := p.Published(j.Hash)
	if err != nil {
		return err
	}
	if !published {
		if err := p.copyInto(ctx, j); err != nil {
			return err
		}
	}

	now := p.o.Clock.Now()
	m := Manifest{
		JobID:      j.ID,
		TraceID:    j.TraceID,
		Hash:       j.Hash,
		FileName:   j.FileName,
		SourcePath: j.SourcePath,
		InputSize:  j.Size,
		OutputSize: j.OutSize,
		Attempts:   j.Attempts,
		CreatedAt:  j.CreatedAt,
		FinishedAt: now,
	}
	if err := p.writeManifest(m); err != nil {
		return err
	}

	p.cleanup(j)
	p.o.Metrics.IncCounter("pipeline_bytes_written_total", "Bytes written to the scratch area.",
		map[string]string{"stage": string(job.StagePublish)}, float64(j.OutSize))
	return nil
}

func (p *Publisher) copyInto(ctx context.Context, j *job.Job) error {
	dir := filepath.Join(p.o.OutDir, shard(j.Hash))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("sink: create shard dir: %w", err)
	}
	final := FinalPath(p.o.OutDir, j.Hash)
	tmp := filepath.Join(p.tmpDir, j.Hash+".gz.part")

	src, err := os.Open(j.Artifact)
	if err != nil {
		return fmt.Errorf("sink: open artefact: %w", err)
	}
	defer func() { _ = src.Close() }()

	dst, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("sink: create staging file: %w", err)
	}
	// Any exit that is not the successful rename must not leave a staging file
	// behind: the pipeline promises that out/ holds only complete artefacts.
	committed := false
	defer func() {
		_ = dst.Close()
		if !committed {
			_ = os.Remove(tmp)
		}
	}()

	if _, err := io.Copy(dst, ctxReader{ctx: ctx, r: src}); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("sink: copy artefact: %w", err)
	}
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("sink: sync staging file: %w", err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("sink: close staging file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("sink: publish %s: %w", j.Hash, err)
	}
	committed = true
	syncDir(dir)
	return nil
}

func (p *Publisher) writeManifest(m Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("sink: marshal manifest: %w", err)
	}
	dir := filepath.Join(p.o.OutDir, shard(m.Hash))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("sink: create shard dir: %w", err)
	}
	tmp := filepath.Join(p.tmpDir, m.Hash+".json.part")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("sink: create manifest: %w", err)
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
		return fmt.Errorf("sink: write manifest: %w", werr)
	}
	if err := os.Rename(tmp, ManifestPath(p.o.OutDir, m.Hash)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("sink: publish manifest: %w", err)
	}
	syncDir(dir)
	return nil
}

// cleanup removes the job scratch directory. Failing to remove it must not fail
// the publish: the artefact is already durable, and startup sweeps leftovers.
func (p *Publisher) cleanup(j *job.Job) {
	if j.TempDir == "" {
		return
	}
	if err := os.RemoveAll(j.TempDir); err != nil {
		p.o.Log.Warn("could not remove scratch directory", "job", j.ID, "dir", j.TempDir, "err", err)
	}
}

// ctxReader stops a copy as soon as the context is cancelled.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(b []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(b)
}

// syncDir flushes a directory entry after a rename. Best effort: some platforms
// refuse to open directories for writing.
func syncDir(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = f.Close()
}
