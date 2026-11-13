// Package pipeline wires the stages together and owns every goroutine, channel
// and shutdown deadline in the service.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"example.com/filepipeline/internal/config"
	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
	"example.com/filepipeline/internal/source"
	"example.com/filepipeline/internal/stage"
)

// Sink is the exit of the pipeline: publish an artefact, or record a failure.
type Sink interface {
	// Publish must be atomic: after an error nothing partial may be visible.
	Publish(ctx context.Context, j *job.Job) error
	// Fail records the job in the dead letter queue.
	Fail(ctx context.Context, j *job.Job, st job.Stage, cause error) error
}

// Options are the collaborators of a Pipeline.
type Options struct {
	Config  config.Config
	Log     *slog.Logger
	Metrics *obs.Metrics
	Store   *job.Store
	Clock   job.Clock
	Rand    job.Rand
	Source  source.Source
	Stages  []stage.Stage
	Sink    Sink
}

// Pipeline owns n+1 channels for n stages: queue[i] feeds stage i, queue[n]
// feeds the sink. Every queue is bounded, so a slow stage pushes back on the
// source instead of growing an in-memory backlog.
type Pipeline struct {
	o         Options
	queues    []chan *job.Job
	stageCfgs []config.StageConfig
	limiter   *weightedLimiter

	mu            sync.Mutex
	cancelSource  context.CancelFunc
	cancelAll     context.CancelFunc
	stopRequested bool
	forceDone     bool
	fatal         error
}

// New validates the wiring and allocates the bounded queues.
func New(o Options) (*Pipeline, error) {
	if o.Source == nil {
		return nil, errors.New("pipeline: source is required")
	}
	if o.Sink == nil {
		return nil, errors.New("pipeline: sink is required")
	}
	if o.Store == nil {
		return nil, errors.New("pipeline: journal store is required")
	}
	if len(o.Stages) == 0 {
		return nil, errors.New("pipeline: at least one stage is required")
	}
	if len(o.Config.Stages) != len(o.Stages) {
		return nil, fmt.Errorf("pipeline: %d stage configs for %d stages", len(o.Config.Stages), len(o.Stages))
	}
	if o.Log == nil {
		o.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.Metrics == nil {
		o.Metrics = obs.NewMetrics()
	}
	if o.Clock == nil {
		o.Clock = job.RealClock{}
	}
	if o.Rand == nil {
		o.Rand = job.NewRand(time.Now().UnixNano())
	}

	p := &Pipeline{o: o, stageCfgs: make([]config.StageConfig, len(o.Stages))}
	n := len(o.Stages)
	p.queues = make([]chan *job.Job, n+1)
	for i, st := range o.Stages {
		sc := o.Config.Stages[i]
		if string(st.Name()) != sc.Name {
			return nil, fmt.Errorf("pipeline: stage %d is %q but config says %q", i, st.Name(), sc.Name)
		}
		if sc.QueueSize < config.MinQueueSize || sc.QueueSize > config.MaxQueueSize {
			return nil, fmt.Errorf("pipeline: stage %q queue_size %d outside [%d,%d]",
				sc.Name, sc.QueueSize, config.MinQueueSize, config.MaxQueueSize)
		}
		p.stageCfgs[i] = sc
		p.queues[i] = make(chan *job.Job, sc.QueueSize)
	}
	p.queues[n] = make(chan *job.Job, o.Config.QueueSize)
	p.limiter = newWeightedLimiter(o.Config.MaxInflightBytes)

	for i := 0; i <= n; i++ {
		label := string(job.StagePublish)
		if i < n {
			label = string(o.Stages[i].Name())
		}
		ch := p.queues[i]
		o.Metrics.RegisterGaugeFunc("pipeline_queue_depth",
			"Jobs waiting in the channel that feeds a stage.",
			map[string]string{"stage": label}, func() float64 { return float64(len(ch)) })
	}
	o.Metrics.RegisterGaugeFunc("pipeline_inflight_bytes",
		"Bytes currently held inside a stage, bounded by max_inflight_bytes.",
		nil, func() float64 { return float64(p.limiter.Used()) })
	return p, nil
}

// Queues exposes the channel capacities, which is how tests assert that the
// pipeline is bounded rather than buffered without limit.
func (p *Pipeline) Queues() []chan *job.Job { return p.queues }

// run is the single owner of the errgroup. Every long lived goroutine is either
// started by the group or waited for by a closer that the group runs, so
// g.Wait() cannot return while a worker is still alive.
func (p *Pipeline) run(parent context.Context) error {
	ctx, cancelAll := context.WithCancel(parent)
	p.setCancelAll(cancelAll)
	defer cancelAll()

	srcCtx, stopSource := context.WithCancel(ctx)
	p.setCancelSource(stopSource)
	defer stopSource()

	g, gctx := errgroup.WithContext(ctx)
	n := len(p.o.Stages)

	g.Go(func() error {
		// The source is the sender of queue[0], so the source closes it.
		defer close(p.queues[0])
		err := p.o.Source.Run(srcCtx, func(_ context.Context, j *job.Job) error {
			// Journal at admission: from this moment the job exists for the
			// rest of the system, so a job that is still buffered in a channel
			// when a forced cancel fires is recovered, not lost.
			if j.State == job.StatePending {
				if serr := p.o.Store.Save(srcCtx, j.Clone()); serr != nil {
					if srcCtx.Err() != nil {
						// Stopping: the job was never admitted, so it stays in the
						// inbox and the next start will pick it up again.
						return context.Canceled
					}
					p.fail(fmt.Errorf("pipeline: journal admitted job %s: %w", j.ID, serr))
					return serr
				}
				// Counting at admission keeps the metric and the journal in
				// agreement: every job the pipeline knows about is counted once.
				p.o.Metrics.IncCounter("pipeline_jobs_enqueued_total", "Jobs admitted into the pipeline.",
					map[string]string{"stage": "source"}, 1)
			}
			if !p.send(srcCtx, p.queues[0], j) {
				return context.Canceled
			}
			return nil
		})
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		if errors.Is(err, context.DeadlineExceeded) && parent.Err() != nil {
			return nil
		}
		p.fail(fmt.Errorf("pipeline: source: %w", err))
		return err
	})

	for i, st := range p.o.Stages {
		p.startStage(g, gctx, st, p.queues[i], p.queues[i+1], p.stageCfgs[i])
	}
	g.Go(func() error { return p.consumeSink(gctx, p.queues[n]) })

	err := g.Wait()
	if fatal := p.fatalErr(); fatal != nil {
		return fatal
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return err
	}
	return nil
}

// startStage launches the worker pool and a closer goroutine. Workers are plain
// goroutines; the closer is the errgroup member that waits for them, which keeps
// the "no goroutine without a join" rule.
func (p *Pipeline) startStage(g *errgroup.Group, ctx context.Context, st stage.Stage, in <-chan *job.Job, out chan<- *job.Job, sc config.StageConfig) {
	var wg sync.WaitGroup
	for i := 0; i < sc.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.worker(ctx, st, in, out, sc)
		}()
	}
	g.Go(func() error {
		wg.Wait()
		// Sender side close: this stage produced everything it ever will.
		close(out)
		return nil
	})
}

func (p *Pipeline) worker(ctx context.Context, st stage.Stage, in <-chan *job.Job, out chan<- *job.Job, sc config.StageConfig) {
	for {
		var (
			j  *job.Job
			ok bool
		)
		select {
		case <-ctx.Done():
			return
		case j, ok = <-in:
			if !ok {
				return
			}
		}
		if j == nil {
			continue
		}
		if !p.handle(ctx, st, j, out, sc) {
			return
		}
	}
}

// handle runs one job through one stage, including retries. It returns false
// when the worker must stop (cancellation or a fatal pipeline error).
func (p *Pipeline) handle(ctx context.Context, st stage.Stage, j *job.Job, out chan<- *job.Job, sc config.StageConfig) bool {
	j.MarkRunning(st.Name(), p.o.Clock.Now())
	if !p.journal(ctx, j) {
		return false
	}

	backoff := job.Backoff{
		Base:        sc.Backoff.Base.D(),
		Max:         sc.Backoff.Max.D(),
		Factor:      sc.Backoff.Factor,
		Jitter:      sc.Backoff.Jitter,
		MaxAttempts: sc.Backoff.MaxAttempts,
	}.Normalize()

	// Byte weighted admission: a 40 MiB file occupies 40 MiB of the budget, so
	// four of them never sit in memory at once even with 32 workers.
	weight := j.Size
	if weight < 0 {
		weight = 0
	}
	if err := p.limiter.Acquire(ctx, weight); err != nil {
		return false
	}
	defer p.limiter.Release(weight)

	for {
		attempt := j.BumpAttempt(st.Name(), p.o.Clock.Now())
		actx, cancel := context.WithTimeout(ctx, sc.Timeout.D())
		started := p.o.Clock.Now()
		res, err := safeProcess(actx, st, j)
		cancel()
		p.observe(st.Name(), started)

		if err == nil {
			if res == nil {
				res = j
			}
			if !p.journal(ctx, res) {
				return false
			}
			if !p.send(ctx, out, res) {
				return false
			}
			return true
		}

		switch {
		case errors.Is(err, stage.ErrSkip):
			skipped := res
			if skipped == nil {
				skipped = j
			}
			if !skipped.Terminal() {
				skipped.MarkDone(p.o.Clock.Now())
			}
			if !p.journal(ctx, skipped) {
				return false
			}
			return true

		case ctx.Err() != nil:
			// Global stop, not a job failure: leave the journal entry in the
			// running state so the next start replays this job.
			p.o.Log.WarnContext(ctx, "stage interrupted by shutdown, job left in the journal",
				"job", j.ID, "stage", st.Name())
			return false

		case stage.IsFatal(err) || stage.IsDiskError(err):
			p.fail(fmt.Errorf("pipeline: stage %s: %w", st.Name(), err))
			return false

		case stage.IsPermanent(err) || attempt >= backoff.MaxAttempts:
			p.deadLetter(j, st.Name(), err, attempt)
			return true
		}

		delay := backoff.Delay(attempt, p.o.Rand.Float64())
		p.o.Metrics.IncCounter("pipeline_retries_total", "Stage attempts that were retried.",
			map[string]string{"stage": string(st.Name())}, 1)
		p.o.Log.WarnContext(ctx, "stage attempt failed, retrying",
			"job", j.ID, "stage", st.Name(), "attempt", attempt,
			"max_attempts", backoff.MaxAttempts, "delay", delay.String(), "err", err)
		if serr := p.o.Clock.Sleep(ctx, delay); serr != nil {
			return false
		}
	}
}

// consumeSink publishes finished artefacts. Publishing is not retried: a failed
// rename is either a disk level problem (fatal) or a job specific one (dead
// letter), and the content addressed target makes a later rerun harmless.
func (p *Pipeline) consumeSink(ctx context.Context, in <-chan *job.Job) error {
	for {
		var (
			j  *job.Job
			ok bool
		)
		select {
		case <-ctx.Done():
			return nil
		case j, ok = <-in:
			if !ok {
				return nil
			}
		}
		if j == nil {
			continue
		}
		started := p.o.Clock.Now()
		err := p.o.Sink.Publish(ctx, j)
		p.observe(job.StagePublish, started)
		if err == nil {
			j.MarkDone(p.o.Clock.Now())
			if !p.journal(ctx, j) {
				return nil
			}
			p.o.Metrics.IncCounter("pipeline_jobs_completed_total", "Jobs published into out/.",
				map[string]string{"stage": string(job.StagePublish)}, 1)
			continue
		}

		switch {
		case ctx.Err() != nil:
			// Cancelled mid publish: the staging file is already gone, the job
			// stays "running" and is replayed on the next start.
			return nil
		case stage.IsFatal(err) || stage.IsDiskError(err):
			p.fail(err)
			return err
		default:
			p.deadLetter(j, job.StagePublish, err, j.Attempt(job.StagePublish))
		}
	}
}

func (p *Pipeline) deadLetter(j *job.Job, st job.Stage, cause error, attempts int) {
	now := p.o.Clock.Now()
	j.MarkFailed(st, cause, now)
	// The dead letter is the only remaining evidence for this job, so it runs on
	// a fresh context even when the pipeline is being cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := p.o.Sink.Fail(ctx, j, st, cause); err != nil {
		p.o.Log.Error("could not record dead letter", "job", j.ID, "stage", st, "err", err)
		p.fail(fmt.Errorf("pipeline: dead letter job %s: %w", j.ID, err))
		return
	}
	if err := p.o.Store.Save(ctx, j.Clone()); err != nil {
		p.o.Log.Error("could not journal failed job", "job", j.ID, "err", err)
		p.fail(fmt.Errorf("pipeline: journal failed job %s: %w", j.ID, err))
		return
	}
	p.o.Log.Error("job moved to the dead letter queue",
		"job", j.ID, "stage", st, "attempts", attempts, "err", cause)
}

// journal writes a job snapshot. A write that fails because the pipeline is
// shutting down is not fatal: the job keeps its previous journal entry and is
// replayed by the next start. Any other failure is fatal, because the journal is
// the only thing that makes restarts safe.
func (p *Pipeline) journal(ctx context.Context, j *job.Job) bool {
	err := p.o.Store.Save(ctx, j.Clone())
	if err == nil {
		return true
	}
	if ctx.Err() != nil {
		p.o.Log.WarnContext(ctx, "journal write skipped during shutdown", "job", j.ID, "err", err)
		return false
	}
	p.fail(fmt.Errorf("pipeline: journal job %s: %w", j.ID, err))
	return false
}

// observe records one stage attempt in the latency histogram.
func (p *Pipeline) observe(name job.Stage, started time.Time) {
	d := p.o.Clock.Now().Sub(started).Seconds()
	if d < 0 {
		d = 0
	}
	p.o.Metrics.ObserveHistogram("pipeline_stage_duration_seconds",
		"Wall clock time of one stage attempt.", obs.LatencyBuckets,
		map[string]string{"stage": string(name)}, d)
}

// send is the only way the pipeline writes to a channel: every send can be
// abandoned by cancelling the context.
func (p *Pipeline) send(ctx context.Context, ch chan<- *job.Job, j *job.Job) bool {
	select {
	case ch <- j:
		return true
	case <-ctx.Done():
		return false
	}
}

// safeProcess turns a panicking stage into a permanent job error instead of a
// crashed process. One bad file must not take the pipeline down.
func safeProcess(ctx context.Context, st stage.Stage, j *job.Job) (out *job.Job, err error) {
	defer func() {
		if r := recover(); r != nil {
			out = nil
			err = stage.Permanent(fmt.Errorf("stage %s panicked: %v", st.Name(), r))
		}
	}()
	return st.Process(ctx, j)
}

func (p *Pipeline) setCancelAll(fn context.CancelFunc) {
	p.mu.Lock()
	p.cancelAll = fn
	force := p.forceDone
	p.mu.Unlock()
	if force {
		fn()
	}
}

func (p *Pipeline) setCancelSource(fn context.CancelFunc) {
	p.mu.Lock()
	p.cancelSource = fn
	stop := p.stopRequested
	p.mu.Unlock()
	if stop {
		fn()
	}
}

// fail records the first fatal error and cancels everything.
func (p *Pipeline) fail(err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	if p.fatal == nil {
		p.fatal = err
	}
	ca := p.cancelAll
	p.mu.Unlock()
	if ca != nil {
		ca()
	}
}

func (p *Pipeline) fatalErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fatal
}

// weightedLimiter is a context aware counting semaphore keyed on bytes. It
// exists because worker counts bound concurrency in jobs, not in bytes.
type weightedLimiter struct {
	capacity int64

	mu      sync.Mutex
	used    int64
	waiters []chan struct{}
}

func newWeightedLimiter(capacity int64) *weightedLimiter {
	if capacity < 0 {
		capacity = 0
	}
	return &weightedLimiter{capacity: capacity}
}

// Acquire reserves n bytes, blocking until the budget allows it.
func (l *weightedLimiter) Acquire(ctx context.Context, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.capacity <= 0 {
		return nil
	}
	if n > l.capacity {
		// A single job larger than the whole budget would deadlock; let it pass
		// alone and document the exception.
		n = l.capacity
	}
	if n < 0 {
		n = 0
	}
	for {
		l.mu.Lock()
		if l.used+n <= l.capacity {
			l.used += n
			l.mu.Unlock()
			return nil
		}
		ch := make(chan struct{})
		l.waiters = append(l.waiters, ch)
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			l.removeWaiter(ch)
			return ctx.Err()
		case <-ch:
		}
	}
}

// Release returns bytes to the budget and wakes every waiter.
func (l *weightedLimiter) Release(n int64) {
	if l.capacity <= 0 {
		return
	}
	if n > l.capacity {
		n = l.capacity
	}
	if n < 0 {
		n = 0
	}
	l.mu.Lock()
	l.used -= n
	if l.used < 0 {
		l.used = 0
	}
	waiters := l.waiters
	l.waiters = nil
	l.mu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

// Used reports the reserved budget, exposed as a gauge.
func (l *weightedLimiter) Used() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used
}

func (l *weightedLimiter) removeWaiter(target chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, ch := range l.waiters {
		if ch == target {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			return
		}
	}
}
