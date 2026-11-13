package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"example.com/filepipeline/internal/config"
	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
	"example.com/filepipeline/internal/pipeline"
	"example.com/filepipeline/internal/source"
	"example.com/filepipeline/internal/stage"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// The standard library keeps a few runtime owned goroutines around; none
		// of them belong to this package.
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

type fakeSource struct {
	jobs  []*job.Job
	hold  bool
	delay time.Duration
	err   error

	emitted atomic.Int64
}

func (s *fakeSource) Run(ctx context.Context, emit func(context.Context, *job.Job) error) error {
	for _, j := range s.jobs {
		if s.delay > 0 {
			timer := time.NewTimer(s.delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
			timer.Stop()
		}
		if err := emit(ctx, j); err != nil {
			return err
		}
		s.emitted.Add(1)
	}
	if s.err != nil {
		return s.err
	}
	if s.hold {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

type fakeStage struct {
	name    job.Stage
	kind    stage.Kind
	clock   job.Clock
	process func(ctx context.Context, j *job.Job) (*job.Job, error)

	calls atomic.Int64
}

func (f *fakeStage) Name() job.Stage { return f.name }

func (f *fakeStage) Kind() stage.Kind { return f.kind }

func (f *fakeStage) Process(ctx context.Context, j *job.Job) (*job.Job, error) {
	f.calls.Add(1)
	if f.process != nil {
		return f.process(ctx, j)
	}
	j.MarkStageDone(f.name, f.clock.Now())
	return j, nil
}

type fakeSink struct {
	mu        sync.Mutex
	published []*job.Job
	failed    map[string]string
	failIDs   map[string]error
	failErr   error
}

func newFakeSink() *fakeSink {
	return &fakeSink{failed: make(map[string]string), failIDs: make(map[string]error)}
}

func (f *fakeSink) Publish(_ context.Context, j *job.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.failIDs[j.ID]; ok {
		return err
	}
	if f.failErr != nil {
		return f.failErr
	}
	f.published = append(f.published, j)
	return nil
}

func (f *fakeSink) Fail(_ context.Context, j *job.Job, st job.Stage, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed[j.ID] = fmt.Sprintf("%s: %v", st, cause)
	return nil
}

func (f *fakeSink) publishedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.published))
	for i, j := range f.published {
		out[i] = j.ID
	}
	return out
}

func (f *fakeSink) failedStages() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(f.failed))
	for k, v := range f.failed {
		out[k] = v
	}
	return out
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func testConfig(names ...string) config.Config {
	cfg := *config.Default()
	cfg.QueueSize = 8
	cfg.MaxInflightBytes = 0
	cfg.Stages = nil
	for _, n := range names {
		cfg.Stages = append(cfg.Stages, config.StageConfig{
			Name:      n,
			Workers:   2,
			Timeout:   config.Duration(5 * time.Second),
			QueueSize: 8,
			Backoff: config.BackoffConfig{
				Base:        config.Duration(time.Millisecond),
				Max:         config.Duration(5 * time.Millisecond),
				Factor:      2,
				Jitter:      0,
				MaxAttempts: 3,
			},
		})
	}
	return cfg
}

func newStore(t *testing.T) *job.Store {
	t.Helper()
	s, err := job.NewStore(filepath.Join(t.TempDir(), "jobs"), nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func makeJobs(n int, size int64) []*job.Job {
	now := time.Now()
	out := make([]*job.Job, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, job.New(fmt.Sprintf("job%03d", i), fmt.Sprintf("inbox/file%03d.txt", i), size, "", now))
	}
	return out
}

func buildPipeline(t *testing.T, cfg config.Config, src source.Source, stages []stage.Stage, snk pipeline.Sink, clk job.Clock) (*pipeline.Pipeline, *job.Store) {
	t.Helper()
	store := newStore(t)
	p, err := pipeline.New(pipeline.Options{
		Config:  cfg,
		Log:     discardLogger(),
		Metrics: obs.NewMetrics(),
		Store:   store,
		Clock:   clk,
		Rand:    job.FixedRand{V: 0.5},
		Source:  src,
		Stages:  stages,
		Sink:    snk,
	})
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}
	return p, store
}

func supervise(t *testing.T, p *pipeline.Pipeline, ctx context.Context) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- p.Supervise(ctx, nil) }()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("pipeline did not stop within 30s")
		return nil
	}
}

func journalState(t *testing.T, store *job.Store) map[string]job.State {
	t.Helper()
	jobs, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("journal list: %v", err)
	}
	out := make(map[string]job.State, len(jobs))
	for _, j := range jobs {
		out[j.ID] = j.State
	}
	return out
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestPipelineProcessesEveryJob(t *testing.T) {
	cfg := testConfig("alpha", "beta")
	clk := job.NewFakeClock(time.Now())
	src := &fakeSource{jobs: makeJobs(25, 10)}
	stages := []stage.Stage{
		&fakeStage{name: "alpha", kind: stage.KindCPU, clock: clk},
		&fakeStage{name: "beta", kind: stage.KindIO, clock: clk},
	}
	sink := newFakeSink()
	p, store := buildPipeline(t, cfg, src, stages, sink, clk)

	if err := supervise(t, p, context.Background()); err != nil {
		t.Fatalf("Supervise: %v", err)
	}
	if got := len(sink.publishedIDs()); got != 25 {
		t.Fatalf("published %d jobs, want 25", got)
	}
	if got := src.emitted.Load(); got != 25 {
		t.Fatalf("source emitted %d, want 25", got)
	}
	for id, st := range journalState(t, store) {
		if st != job.StateDone {
			t.Errorf("job %s state = %s, want done", id, st)
		}
	}
}

func TestQueuesAreBounded(t *testing.T) {
	cfg := testConfig("alpha")
	clk := job.NewFakeClock(time.Now())
	p, _ := buildPipeline(t, cfg, &fakeSource{}, []stage.Stage{
		&fakeStage{name: "alpha", clock: clk},
	}, newFakeSink(), clk)

	queues := p.Queues()
	if len(queues) != 2 {
		t.Fatalf("pipeline has %d queues, want 2", len(queues))
	}
	for i, q := range queues {
		if cap(q) < config.MinQueueSize || cap(q) > config.MaxQueueSize {
			t.Fatalf("queue %d capacity = %d, outside [%d,%d]", i, cap(q), config.MinQueueSize, config.MaxQueueSize)
		}
	}
}

func TestRetryThenSuccessUsesInjectedClock(t *testing.T) {
	cfg := testConfig("alpha")
	cfg.Stages[0].Backoff.MaxAttempts = 5
	cfg.Stages[0].Backoff.Base = config.Duration(10 * time.Millisecond)
	cfg.Stages[0].Backoff.Max = config.Duration(200 * time.Millisecond)
	cfg.Stages[0].Backoff.Factor = 2

	clk := job.NewFakeClock(time.Now())
	src := &fakeSource{jobs: makeJobs(1, 1)}
	var mu sync.Mutex
	remaining := 2
	stages := []stage.Stage{&fakeStage{
		name: "alpha", clock: clk,
		process: func(_ context.Context, j *job.Job) (*job.Job, error) {
			mu.Lock()
			defer mu.Unlock()
			if remaining > 0 {
				remaining--
				return nil, errors.New("transient")
			}
			j.MarkStageDone("alpha", clk.Now())
			return j, nil
		},
	}}
	sink := newFakeSink()
	p, store := buildPipeline(t, cfg, src, stages, sink, clk)

	if err := supervise(t, p, context.Background()); err != nil {
		t.Fatalf("Supervise: %v", err)
	}
	if got := len(sink.publishedIDs()); got != 1 {
		t.Fatalf("published %d, want 1", got)
	}
	sleeps := clk.Sleeps()
	if len(sleeps) != 2 {
		t.Fatalf("retry sleeps = %v, want 2", sleeps)
	}
	if sleeps[0] != 10*time.Millisecond || sleeps[1] != 20*time.Millisecond {
		t.Fatalf("retry sleeps = %v, want [10ms 20ms]", sleeps)
	}
	state := journalState(t, store)
	if state["job000"] != job.StateDone {
		t.Fatalf("job state = %s, want done", state["job000"])
	}
}

func TestRetriesExhaustIntoDeadLetter(t *testing.T) {
	cfg := testConfig("alpha")
	cfg.Stages[0].Backoff.MaxAttempts = 2
	clk := job.NewFakeClock(time.Now())
	src := &fakeSource{jobs: makeJobs(3, 1)}
	stages := []stage.Stage{&fakeStage{
		name: "alpha", clock: clk,
		process: func(context.Context, *job.Job) (*job.Job, error) {
			return nil, errors.New("always broken")
		},
	}}
	sink := newFakeSink()
	p, store := buildPipeline(t, cfg, src, stages, sink, clk)

	if err := supervise(t, p, context.Background()); err != nil {
		t.Fatalf("Supervise: %v", err)
	}
	failed := sink.failedStages()
	if len(failed) != 3 {
		t.Fatalf("dead lettered %d jobs, want 3 (%v)", len(failed), failed)
	}
	if !strings.Contains(failed["job000"], "alpha") {
		t.Fatalf("dead letter stage = %q, want alpha", failed["job000"])
	}
	if got := stages[0].(*fakeStage).calls.Load(); got != 6 {
		t.Fatalf("stage calls = %d, want 6 (3 jobs x 2 attempts)", got)
	}
	for id, st := range journalState(t, store) {
		if st != job.StateFailed {
			t.Errorf("job %s state = %s, want failed", id, st)
		}
		j, err := store.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Attempt("alpha") != 2 {
			t.Errorf("job %s attempts = %d, want 2", id, j.Attempt("alpha"))
		}
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	cfg := testConfig("alpha")
	cfg.Stages[0].Backoff.MaxAttempts = 5
	clk := job.NewFakeClock(time.Now())
	src := &fakeSource{jobs: makeJobs(2, 1)}
	stages := []stage.Stage{&fakeStage{
		name: "alpha", clock: clk,
		process: func(context.Context, *job.Job) (*job.Job, error) {
			return nil, stage.Permanent(errors.New("bad content"))
		},
	}}
	sink := newFakeSink()
	p, _ := buildPipeline(t, cfg, src, stages, sink, clk)

	if err := supervise(t, p, context.Background()); err != nil {
		t.Fatalf("Supervise: %v", err)
	}
	if got := stages[0].(*fakeStage).calls.Load(); got != 2 {
		t.Fatalf("stage calls = %d, want 2 (one per job)", got)
	}
	if got := len(sink.failedStages()); got != 2 {
		t.Fatalf("dead lettered %d, want 2", got)
	}
	if sleeps := clk.Sleeps(); len(sleeps) != 0 {
		t.Fatalf("permanent errors slept instead of failing fast: %v", sleeps)
	}
}

func TestSkipMarksJobDone(t *testing.T) {
	cfg := testConfig("alpha")
	clk := job.NewFakeClock(time.Now())
	src := &fakeSource{jobs: makeJobs(1, 1)}
	stages := []stage.Stage{&fakeStage{
		name: "alpha", clock: clk,
		process: func(_ context.Context, j *job.Job) (*job.Job, error) {
			return j, stage.ErrSkip
		},
	}}
	sink := newFakeSink()
	p, store := buildPipeline(t, cfg, src, stages, sink, clk)

	if err := supervise(t, p, context.Background()); err != nil {
		t.Fatalf("Supervise: %v", err)
	}
	if got := len(sink.publishedIDs()); got != 0 {
		t.Fatalf("skipped job was published %d times", got)
	}
	if st := journalState(t, store)["job000"]; st != job.StateDone {
		t.Fatalf("skipped job state = %s, want done", st)
	}
}

func TestFatalErrorStopsThePipeline(t *testing.T) {
	cfg := testConfig("alpha", "beta")
	clk := job.NewFakeClock(time.Now())
	jobs := makeJobs(40, 1)
	src := &fakeSource{jobs: jobs}
	diskFull := fmt.Errorf("write failed: %w", errors.New("no space left on device"))
	alpha := &fakeStage{
		name: "alpha", clock: clk,
		process: func(_ context.Context, j *job.Job) (*job.Job, error) {
			if j.ID == "job000" {
				return nil, stage.Fatal(diskFull)
			}
			j.MarkStageDone("alpha", clk.Now())
			return j, nil
		},
	}
	beta := &fakeStage{name: "beta", clock: clk}
	sink := newFakeSink()
	p, _ := buildPipeline(t, cfg, src, []stage.Stage{alpha, beta}, sink, clk)

	err := supervise(t, p, context.Background())
	if err == nil {
		t.Fatal("Supervise returned nil, want a fatal error")
	}
	if !strings.Contains(err.Error(), "no space left on device") {
		t.Fatalf("error = %v, want it to mention the disk", err)
	}
	if got := len(sink.publishedIDs()); got == len(jobs) {
		t.Fatalf("fatal error did not stop the pipeline: all %d jobs were published", got)
	}
	if got := len(sink.failedStages()); got != 0 {
		t.Fatalf("fatal error dead lettered %d jobs, want 0", got)
	}
}

func TestSourceErrorIsReturned(t *testing.T) {
	cfg := testConfig("alpha")
	clk := job.NewFakeClock(time.Now())
	boom := errors.New("inbox unreadable")
	src := &fakeSource{jobs: makeJobs(1, 1), err: boom}
	p, _ := buildPipeline(t, cfg, src, []stage.Stage{&fakeStage{name: "alpha", clock: clk}}, newFakeSink(), clk)

	err := supervise(t, p, context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Supervise = %v, want the source error", err)
	}
}

func TestPanicBecomesDeadLetter(t *testing.T) {
	cfg := testConfig("alpha")
	clk := job.NewFakeClock(time.Now())
	src := &fakeSource{jobs: makeJobs(2, 1)}
	stages := []stage.Stage{&fakeStage{
		name: "alpha", clock: clk,
		process: func(_ context.Context, j *job.Job) (*job.Job, error) {
			if j.ID == "job000" {
				panic("kaboom")
			}
			j.MarkStageDone("alpha", clk.Now())
			return j, nil
		},
	}}
	sink := newFakeSink()
	p, _ := buildPipeline(t, cfg, src, stages, sink, clk)

	if err := supervise(t, p, context.Background()); err != nil {
		t.Fatalf("Supervise: %v", err)
	}
	if _, ok := sink.failedStages()["job000"]; !ok {
		t.Fatal("panicking job was not dead lettered")
	}
	if got := len(sink.publishedIDs()); got != 1 {
		t.Fatalf("published %d jobs, want the one healthy job", got)
	}
}

func TestSinkFailureDeadLettersOnlyThatJob(t *testing.T) {
	cfg := testConfig("alpha")
	clk := job.NewFakeClock(time.Now())
	src := &fakeSource{jobs: makeJobs(3, 1)}
	sink := newFakeSink()
	sink.failIDs["job001"] = errors.New("rename denied")
	p, store := buildPipeline(t, cfg, src, []stage.Stage{&fakeStage{name: "alpha", clock: clk}}, sink, clk)

	if err := supervise(t, p, context.Background()); err != nil {
		t.Fatalf("Supervise: %v", err)
	}
	if got := len(sink.publishedIDs()); got != 2 {
		t.Fatalf("published %d, want 2", got)
	}
	state := journalState(t, store)
	if state["job001"] != job.StateFailed {
		t.Fatalf("job001 state = %s, want failed", state["job001"])
	}
	if _, ok := sink.failedStages()["job001"]; !ok {
		t.Fatal("sink failure was not dead lettered")
	}
}

func TestByteBudgetLimitsConcurrency(t *testing.T) {
	cfg := testConfig("alpha")
	cfg.Stages[0].Workers = 4
	cfg.MaxInflightBytes = 100

	clk := job.NewFakeClock(time.Now())
	src := &fakeSource{jobs: makeJobs(8, 100)}
	var inflight, maxSeen atomic.Int64
	stages := []stage.Stage{&fakeStage{
		name: "alpha", clock: clk,
		process: func(_ context.Context, j *job.Job) (*job.Job, error) {
			cur := inflight.Add(1)
			for {
				prev := maxSeen.Load()
				if cur <= prev || maxSeen.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			inflight.Add(-1)
			j.MarkStageDone("alpha", clk.Now())
			return j, nil
		},
	}}
	sink := newFakeSink()
	p, _ := buildPipeline(t, cfg, src, stages, sink, clk)

	if err := supervise(t, p, context.Background()); err != nil {
		t.Fatalf("Supervise: %v", err)
	}
	if got := len(sink.publishedIDs()); got != 8 {
		t.Fatalf("published %d, want 8", got)
	}
	if got := maxSeen.Load(); got != 1 {
		t.Fatalf("max concurrent jobs = %d, want 1 with a 100 byte budget and 100 byte jobs", got)
	}
}

func TestCancelLeavesJobRunningInJournal(t *testing.T) {
	cfg := testConfig("alpha")
	clk := job.NewFakeClock(time.Now())
	jobs := makeJobs(4, 1)
	src := &fakeSource{jobs: jobs}
	started := make(chan struct{}, len(jobs))
	stages := []stage.Stage{&fakeStage{
		name: "alpha", clock: clk,
		process: func(ctx context.Context, j *job.Job) (*job.Job, error) {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}}
	sink := newFakeSink()
	p, store := buildPipeline(t, cfg, src, stages, sink, clk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Supervise(ctx, nil) }()

	for i := 0; i < len(jobs); i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("stage never started")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Supervise after cancel = %v, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("pipeline did not stop after cancel")
	}

	if got := len(sink.failedStages()); got != 0 {
		t.Fatalf("cancellation dead lettered %d jobs, want 0", got)
	}
	var running int
	for _, st := range journalState(t, store) {
		if st == job.StateRunning {
			running++
		}
	}
	if running == 0 {
		t.Fatal("no job was left in the running state after cancellation")
	}
}

func TestJournalSurvivesRestartWithNewPipeline(t *testing.T) {
	cfg := testConfig("alpha")
	clk := job.NewFakeClock(time.Now())
	src := &fakeSource{jobs: makeJobs(3, 1)}
	sink := newFakeSink()
	store := newStore(t)
	stages := func() []stage.Stage {
		return []stage.Stage{&fakeStage{name: "alpha", clock: clk}}
	}
	p, err := pipeline.New(pipeline.Options{
		Config: cfg, Log: discardLogger(), Metrics: obs.NewMetrics(), Store: store,
		Clock: clk, Rand: job.FixedRand{V: 0.5}, Source: src, Stages: stages(), Sink: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervise(t, p, context.Background()); err != nil {
		t.Fatal(err)
	}

	// A second pipeline over the same journal must be able to read every record.
	jobs, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 3 {
		t.Fatalf("journal has %d jobs, want 3", len(jobs))
	}
	for _, j := range jobs {
		if j.State != job.StateDone {
			t.Fatalf("job %s state = %s", j.ID, j.State)
		}
	}
}

func TestNewValidatesWiring(t *testing.T) {
	cfg := testConfig("alpha")
	clk := job.NewFakeClock(time.Now())
	base := pipeline.Options{
		Config: cfg, Log: discardLogger(), Metrics: obs.NewMetrics(), Store: newStore(t),
		Clock: clk, Rand: job.FixedRand{V: 0.5}, Source: &fakeSource{},
		Stages: []stage.Stage{&fakeStage{name: "alpha", clock: clk}}, Sink: newFakeSink(),
	}

	mutations := map[string]func(*pipeline.Options){
		"no source":       func(o *pipeline.Options) { o.Source = nil },
		"no sink":         func(o *pipeline.Options) { o.Sink = nil },
		"no store":        func(o *pipeline.Options) { o.Store = nil },
		"no stages":       func(o *pipeline.Options) { o.Stages = nil },
		"name mismatch":   func(o *pipeline.Options) { o.Stages = []stage.Stage{&fakeStage{name: "beta", clock: clk}} },
		"config mismatch": func(o *pipeline.Options) { o.Config.Stages = cfg.Stages[:0] },
	}
	for name, mutate := range mutations {
		opts := base
		mutate(&opts)
		if _, err := pipeline.New(opts); err == nil {
			t.Errorf("%s: New accepted an invalid wiring", name)
		}
	}
}
