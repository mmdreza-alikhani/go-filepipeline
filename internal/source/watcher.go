// Package source discovers work. The pipeline depends on the Source interface,
// never on polling, so replacing the ticker based watcher with fsnotify later is
// a constructor change in main and nothing else.
package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
)

// Source produces jobs. emit blocks until the pipeline accepts the job or the
// context is cancelled; a returned error aborts the run.
type Source interface {
	Run(ctx context.Context, emit func(ctx context.Context, j *job.Job) error) error
}

// Options configures the polling watcher.
type Options struct {
	InboxDir  string
	Interval  time.Duration
	StableAge time.Duration
	Log       *slog.Logger
	Clock     job.Clock
	Metrics   *obs.Metrics
}

type fileSig struct {
	size int64
	mod  time.Time
}

// Polling watches the inbox with a ticker.
//
// Two kinds of noise are filtered before a job is ever created: names that look
// temporary (.tmp, .part, hidden) and files whose size/mtime changed since the
// previous tick (still being written). A (path, size, mtime) signature is then
// remembered so the same file is emitted exactly once per process lifetime.
type Polling struct {
	o    Options
	seen map[string]fileSig
	// live is read by the metrics endpoint and written by the poll goroutine.
	live atomic.Int64
}

// NewPolling validates the options and creates the inbox if needed.
func NewPolling(o Options) (*Polling, error) {
	if o.InboxDir == "" {
		return nil, errors.New("source: inbox directory must not be empty")
	}
	if o.Interval <= 0 {
		o.Interval = 200 * time.Millisecond
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
	if err := os.MkdirAll(o.InboxDir, 0o755); err != nil {
		return nil, fmt.Errorf("source: create inbox: %w", err)
	}
	p := &Polling{o: o, seen: make(map[string]fileSig)}
	o.Metrics.RegisterGaugeFunc("pipeline_inbox_files",
		"Regular files currently waiting in the inbox.", nil,
		func() float64 { return float64(p.live.Load()) })
	return p, nil
}

// Run implements Source.
func (p *Polling) Run(ctx context.Context, emit func(context.Context, *job.Job) error) error {
	ticker := time.NewTicker(p.o.Interval)
	defer ticker.Stop()

	if err := p.poll(ctx, emit); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.poll(ctx, emit); err != nil {
				return err
			}
		}
	}
}

func (p *Polling) poll(ctx context.Context, emit func(context.Context, *job.Job) error) error {
	entries, err := os.ReadDir(p.o.InboxDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Somebody removed the inbox at runtime; recreate instead of dying.
			if err := os.MkdirAll(p.o.InboxDir, 0o755); err != nil {
				return fmt.Errorf("source: recreate inbox: %w", err)
			}
			return nil
		}
		return fmt.Errorf("source: read inbox: %w", err)
	}

	now := p.o.Clock.Now()
	live := make(map[string]struct{}, len(entries))
	regular := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		path := filepath.Join(p.o.InboxDir, name)
		live[path] = struct{}{}

		if _, err := job.ParseFileName(name); err != nil {
			// Temporary, hidden or unsafe names are not work.
			p.o.Metrics.IncCounter("pipeline_source_ignored_total",
				"Inbox entries the watcher deliberately ignored.",
				map[string]string{"reason": "name"}, 1)
			p.o.Log.DebugContext(ctx, "ignoring inbox entry", "name", name, "reason", err)
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // vanished between ReadDir and Info
		}
		if !info.Mode().IsRegular() {
			continue
		}
		regular++
		sig := fileSig{size: info.Size(), mod: info.ModTime()}
		if p.o.StableAge > 0 && now.Sub(sig.mod) < p.o.StableAge {
			continue // still being written by the producer
		}
		if prev, ok := p.seen[path]; ok && prev == sig {
			continue // duplicate event for an identical file
		}

		j := job.New(job.NewID(), path, sig.size, "", now)
		j.TraceID = j.ID
		if err := emit(obs.WithTraceID(ctx, j.TraceID), j); err != nil {
			return err
		}
		p.seen[path] = sig
	}

	// Forget signatures of files that left the inbox so the map stays bounded.
	for path := range p.seen {
		if _, ok := live[path]; !ok {
			delete(p.seen, path)
		}
	}
	p.live.Store(int64(regular))
	return nil
}

// Replay emits recovered jobs before delegating to the wrapped source. The
// pipeline uses it to resume work found in the journal at startup.
type Replay struct {
	inner Source
	jobs  []*job.Job
	log   *slog.Logger
}

// NewReplay wraps inner. A nil inner means "replay only", which the recovery
// path uses when it wants no new discovery at all.
func NewReplay(inner Source, jobs []*job.Job, log *slog.Logger) *Replay {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Replay{inner: inner, jobs: jobs, log: log}
}

// Run implements Source.
func (r *Replay) Run(ctx context.Context, emit func(context.Context, *job.Job) error) error {
	for _, j := range r.jobs {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.log.InfoContext(ctx, "replaying journal entry", "job", j.ID, "stage", j.Stage, "resumes", j.Resumes)
		if err := emit(obs.WithTraceID(ctx, j.TraceID), j); err != nil {
			return err
		}
	}
	if r.inner == nil {
		return nil
	}
	return r.inner.Run(ctx, emit)
}

var errLimitReached = errors.New("source: job limit reached")

// Limit stops the source after n emitted jobs, which is what makes a batch run
// ("process exactly these files, then drain and exit") scriptable.
type Limit struct {
	inner Source
	n     int
	log   *slog.Logger
}

// NewLimit wraps inner with a hard job budget.
func NewLimit(inner Source, n int, log *slog.Logger) *Limit {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Limit{inner: inner, n: n, log: log}
}

// Run implements Source.
func (l *Limit) Run(ctx context.Context, emit func(context.Context, *job.Job) error) error {
	if l.n <= 0 {
		return l.inner.Run(ctx, emit)
	}
	count := 0
	err := l.inner.Run(ctx, func(c context.Context, j *job.Job) error {
		if count >= l.n {
			return errLimitReached
		}
		if err := emit(c, j); err != nil {
			return err
		}
		count++
		if count >= l.n {
			l.log.InfoContext(c, "job limit reached, stopping the source", "limit", l.n)
			return errLimitReached
		}
		return nil
	})
	if errors.Is(err, errLimitReached) {
		return nil
	}
	return err
}
