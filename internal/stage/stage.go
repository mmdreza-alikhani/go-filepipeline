// Package stage holds the pipeline steps. A stage knows nothing about the
// stages around it: it receives a job, returns a job, and signals "skip" with
// ErrSkip. That is what keeps the chain reorderable and independently testable.
package stage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"syscall"

	"example.com/filepipeline/internal/config"
	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
)

// Kind documents whether a stage is CPU bound or IO bound. The pipeline builds
// one worker pool per stage, so the two kinds never share a concurrency budget,
// but the kind is also reported in logs and used for sizing advice.
type Kind uint8

const (
	// KindCPU marks a stage that spends its time in user space (normalising,
	// compressing). Those workers can saturate every core.
	KindCPU Kind = iota
	// KindIO marks a stage that waits on the file system.
	KindIO
)

func (k Kind) String() string {
	switch k {
	case KindCPU:
		return "cpu"
	case KindIO:
		return "io"
	default:
		return "unknown"
	}
}

// Stage is one processing step.
type Stage interface {
	// Name is the identifier used in the journal, metrics and logs.
	Name() job.Stage
	// Kind reports the resource profile of Process.
	Kind() Kind
	// Process handles one job. Returning ErrSkip means "nothing to do", which
	// is not an error and must not be retried or dead lettered.
	Process(ctx context.Context, j *job.Job) (*job.Job, error)
}

// ErrSkip tells the pipeline that the job needs no further work. Stages return
// it together with the job so the caller can journal the final state.
var ErrSkip = errors.New("stage: skip job")

// Domain errors that are permanent: retrying cannot help, so the pipeline sends
// the job straight to the dead letter queue.
var (
	ErrFileTooLarge = errors.New("stage: file exceeds the configured maximum size")
	ErrNotText      = errors.New("stage: input is not valid UTF-8 text")
)

type fatalError struct{ err error }

func (f *fatalError) Error() string { return "fatal: " + f.err.Error() }
func (f *fatalError) Unwrap() error { return f.err }

// Fatal marks err as a pipeline fatal condition (full disk, broken journal,
// unusable configuration). The whole pipeline stops instead of dead lettering a
// single job, because every other job would fail the same way.
func Fatal(err error) error {
	if err == nil {
		return nil
	}
	return &fatalError{err: err}
}

// IsFatal reports whether err was wrapped by Fatal.
func IsFatal(err error) bool {
	var f *fatalError
	return errors.As(err, &f)
}

type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent marks err as a job level, non retryable failure (oversized file,
// invalid content, unsafe name). The job goes to the dead letter queue on the
// first attempt, while the rest of the pipeline keeps running.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was wrapped by Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// IsDiskError reports whether err is a "the device is out of space" condition,
// which the pipeline treats as fatal.
func IsDiskError(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}

// OutputProbe answers "is this content already published?". The scan stage uses
// it for idempotency: a re-run of an already processed file costs one hash and
// nothing else.
type OutputProbe interface {
	Published(hash string) (bool, error)
}

// ProbeFunc adapts a function to OutputProbe.
type ProbeFunc func(hash string) (bool, error)

// Published implements OutputProbe.
func (f ProbeFunc) Published(hash string) (bool, error) { return f(hash) }

// Deps are the collaborators a stage may use.
type Deps struct {
	Config  config.Config
	Log     *slog.Logger
	Metrics *obs.Metrics
	Clock   job.Clock
	Probe   OutputProbe
}

// Factory builds one stage from its configuration block.
type Factory func(Deps, config.StageConfig) (Stage, error)

// factories is the registry. Adding a stage is: write the type, add one entry
// here, add a block to the config. Nothing else in the pipeline changes.
var factories = map[string]Factory{
	"scan":     newScanStage,
	"convert":  newConvertStage,
	"compress": newCompressStage,
}

// Known lists the registered stage names, sorted.
func Known() []string {
	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Build instantiates the configured stages in configuration order.
func Build(d Deps) ([]Stage, error) {
	if d.Clock == nil {
		d.Clock = job.RealClock{}
	}
	if d.Metrics == nil {
		d.Metrics = obs.NewMetrics()
	}
	if d.Log == nil {
		d.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if len(d.Config.Stages) == 0 {
		return nil, errors.New("stage: no stages configured")
	}
	out := make([]Stage, 0, len(d.Config.Stages))
	for _, sc := range d.Config.Stages {
		factory, ok := factories[sc.Name]
		if !ok {
			return nil, fmt.Errorf("stage: unknown stage %q (registered: %s)", sc.Name, strings.Join(Known(), ", "))
		}
		s, err := factory(d, sc)
		if err != nil {
			return nil, fmt.Errorf("stage: build %q: %w", sc.Name, err)
		}
		if got := string(s.Name()); got != sc.Name {
			return nil, fmt.Errorf("stage: factory %q produced stage %q", sc.Name, got)
		}
		out = append(out, s)
	}
	return out, nil
}

// ctxReader aborts a long copy as soon as the context is cancelled. Without it
// a cancelled pipeline would keep streaming a multi gigabyte file to the end.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
