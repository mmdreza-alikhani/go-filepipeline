package pipeline_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"example.com/filepipeline/internal/config"
	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
	"example.com/filepipeline/internal/pipeline"
	"example.com/filepipeline/internal/sink"
	"example.com/filepipeline/internal/source"
	"example.com/filepipeline/internal/stage"
)

// harness wires the real source, stages, publisher and dead letter queue against
// a throwaway directory tree, exactly like cmd/pipeline does.
type harness struct {
	cfg     config.Config
	store   *job.Store
	pub     *sink.Publisher
	dlq     *sink.DeadLetter
	metrics *obs.Metrics
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	root := t.TempDir()
	cfg := *config.Default()
	cfg.InboxDir = filepath.Join(root, "inbox")
	cfg.OutDir = filepath.Join(root, "out")
	cfg.WorkDir = filepath.Join(root, "work")
	cfg.FailedDir = filepath.Join(root, "failed")
	cfg.QueueSize = 8
	cfg.MaxFileSize = 1 << 20
	cfg.MaxInflightBytes = 4 << 20
	cfg.StableAge = 0
	cfg.PollInterval = config.Duration(10 * time.Millisecond)
	cfg.Shutdown = config.ShutdownConfig{
		Grace: config.Duration(2 * time.Second),
		Hard:  config.Duration(5 * time.Second),
	}
	for i := range cfg.Stages {
		cfg.Stages[i].QueueSize = 8
		cfg.Stages[i].Workers = 2
		cfg.Stages[i].Backoff = config.BackoffConfig{
			Base:        config.Duration(time.Millisecond),
			Max:         config.Duration(10 * time.Millisecond),
			Factor:      2,
			Jitter:      0,
			MaxAttempts: 2,
		}
	}
	for _, dir := range []string{cfg.InboxDir, cfg.OutDir, cfg.WorkDir, cfg.FailedDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	store, err := job.NewStore(cfg.JournalDir(), discardLogger())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	metrics := obs.NewMetrics()
	pub, err := sink.NewPublisher(sink.PublisherOptions{
		OutDir: cfg.OutDir, Log: discardLogger(), Metrics: metrics, Clock: job.RealClock{},
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	dlq, err := sink.NewDeadLetter(sink.DeadLetterOptions{
		Dir: cfg.FailedDir, Log: discardLogger(), Metrics: metrics, Clock: job.RealClock{},
	})
	if err != nil {
		t.Fatalf("NewDeadLetter: %v", err)
	}
	return &harness{cfg: cfg, store: store, pub: pub, dlq: dlq, metrics: metrics}
}

func (h *harness) write(t *testing.T, name, content string) {
	t.Helper()
	path := filepath.Join(h.cfg.InboxDir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

type fanoutSink struct {
	pub *sink.Publisher
	dlq *sink.DeadLetter
}

func (f fanoutSink) Publish(ctx context.Context, j *job.Job) error { return f.pub.Publish(ctx, j) }

func (f fanoutSink) Fail(ctx context.Context, j *job.Job, st job.Stage, cause error) error {
	return f.dlq.Fail(ctx, j, st, cause)
}

func (h *harness) pipeline(t *testing.T, replay []*job.Job, limit int) *pipeline.Pipeline {
	t.Helper()
	stages, err := stage.Build(stage.Deps{
		Config: h.cfg, Log: discardLogger(), Metrics: h.metrics,
		Clock: job.RealClock{}, Probe: h.pub,
	})
	if err != nil {
		t.Fatalf("stage.Build: %v", err)
	}
	watcher, err := source.NewPolling(source.Options{
		InboxDir:  h.cfg.InboxDir,
		Interval:  h.cfg.PollInterval.D(),
		StableAge: h.cfg.StableAge.D(),
		Log:       discardLogger(),
		Clock:     job.RealClock{},
		Metrics:   h.metrics,
	})
	if err != nil {
		t.Fatalf("NewPolling: %v", err)
	}
	var src source.Source = watcher
	if len(replay) > 0 {
		src = source.NewReplay(src, replay, discardLogger())
	}
	if limit > 0 {
		src = source.NewLimit(src, limit, discardLogger())
	}
	p, err := pipeline.New(pipeline.Options{
		Config: h.cfg, Log: discardLogger(), Metrics: h.metrics, Store: h.store,
		Clock: job.RealClock{}, Rand: job.NewRand(1), Source: src, Stages: stages,
		Sink: fanoutSink{pub: h.pub, dlq: h.dlq},
	})
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}
	return p
}

// runToCompletion runs a pipeline that stops by itself (a bounded source).
func (h *harness) runToCompletion(t *testing.T, p *pipeline.Pipeline) error {
	t.Helper()
	return supervise(t, p, context.Background())
}

func (h *harness) gzFiles(t *testing.T) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(h.cfg.OutDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".tmp" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".gz") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk out dir: %v", err)
	}
	return out
}

func readGzip(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("%s is not a valid gzip member: %v", path, err)
	}
	data, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("%s is truncated: %v", path, err)
	}
	return string(data)
}

// assertOutDirIsClean checks the two promises the sink makes: no partial files
// anywhere in out/, and every artefact decompresses to canonical text.
func (h *harness) assertOutDirIsClean(t *testing.T) int {
	t.Helper()
	count := 0
	err := filepath.Walk(h.cfg.OutDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		name := info.Name()
		if strings.HasSuffix(name, ".part") || strings.HasSuffix(name, ".tmp") {
			t.Errorf("partial file in out/: %s", path)
		}
		if !strings.HasSuffix(name, ".gz") {
			return nil
		}
		count++
		content := readGzip(t, path)
		if strings.Contains(content, "\r") {
			t.Errorf("%s contains CR characters", path)
		}
		for _, line := range strings.Split(content, "\n") {
			if line != strings.TrimRight(line, " \t") {
				t.Errorf("%s has trailing whitespace on %q", path, line)
			}
		}
		if content != "" && !strings.HasSuffix(content, "\n") {
			t.Errorf("%s does not end with a newline", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk out dir: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(h.cfg.OutDir, ".tmp"))
	if err != nil {
		t.Fatalf("read staging dir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("staging directory still holds %d files", len(entries))
	}
	return count
}

func TestIdempotentReprocessingProducesOneOutput(t *testing.T) {
	h := newHarness(t)
	h.write(t, "a.txt", "hello  \r\nworld\r\n")

	if err := h.runToCompletion(t, h.pipeline(t, nil, 1)); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first := h.gzFiles(t)
	if len(first) != 1 {
		t.Fatalf("first run produced %d artefacts, want 1", len(first))
	}
	if got := readGzip(t, first[0]); got != "hello\nworld\n" {
		t.Fatalf("content = %q", got)
	}

	// Running again over the same inbox must not create a second artefact and
	// must not rewrite the existing one.
	before, err := os.Stat(first[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := h.runToCompletion(t, h.pipeline(t, nil, 1)); err != nil {
		t.Fatalf("second run: %v", err)
	}
	second := h.gzFiles(t)
	if len(second) != 1 {
		t.Fatalf("second run produced %d artefacts, want 1", len(second))
	}
	after, err := os.Stat(second[0])
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("the second run rewrote an already published artefact")
	}
	if count := h.assertOutDirIsClean(t); count != 1 {
		t.Fatalf("clean check counted %d artefacts, want 1", count)
	}

	jobs, err := h.store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("journal has %d jobs, want 2 (the second one skipped)", len(jobs))
	}
	for _, j := range jobs {
		if j.State != job.StateDone {
			t.Fatalf("job %s state = %s, want done", j.ID, j.State)
		}
	}
	if skipped := h.metrics.CounterValue("pipeline_jobs_skipped_total", map[string]string{"stage": "scan"}); skipped != 1 {
		t.Fatalf("skipped counter = %v, want 1", skipped)
	}
}

func TestCorruptInputGoesToDeadLetter(t *testing.T) {
	h := newHarness(t)
	h.write(t, "good.txt", "fine\n")
	h.write(t, "broken.bin", string([]byte{0xff, 0xfe, 0x00, 0x41}))

	if err := h.runToCompletion(t, h.pipeline(t, nil, 2)); err != nil {
		t.Fatalf("run: %v", err)
	}

	entries, err := os.ReadDir(h.cfg.FailedDir)
	if err != nil {
		t.Fatal(err)
	}
	var records []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			records = append(records, e.Name())
		}
	}
	if len(records) != 1 {
		t.Fatalf("failed/ holds %d records (%v), want exactly 1", len(records), records)
	}
	data, err := os.ReadFile(filepath.Join(h.cfg.FailedDir, records[0]))
	if err != nil {
		t.Fatal(err)
	}
	var rec sink.Failure
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("dead letter record is not valid JSON: %v", err)
	}
	if rec.Stage != job.StageConvert {
		t.Errorf("stage = %q, want convert", rec.Stage)
	}
	if rec.Attempts < 1 || rec.AttemptsByStage[job.StageConvert] < 1 {
		t.Errorf("attempts = %d (%v), want at least 1", rec.Attempts, rec.AttemptsByStage)
	}
	if !strings.Contains(rec.Error, "UTF-8") {
		t.Errorf("error = %q, want the UTF-8 diagnosis", rec.Error)
	}
	if rec.At.IsZero() || rec.TraceID == "" || rec.JobID == "" {
		t.Errorf("record is missing timestamp or identity: %+v", rec)
	}
	if _, err := os.Stat(filepath.Join(h.cfg.FailedDir, rec.JobID+"__broken.bin")); err != nil {
		t.Errorf("the failed input was not moved next to its record: %v", err)
	}

	// The healthy file must still have been published.
	if files := h.gzFiles(t); len(files) != 1 {
		t.Fatalf("published %d artefacts, want the one good file", len(files))
	}

	jobs, err := h.store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var failed, done int
	for _, j := range jobs {
		switch j.State {
		case job.StateFailed:
			failed++
		case job.StateDone:
			done++
		}
	}
	if failed != 1 || done != 1 {
		t.Fatalf("journal: %d failed, %d done; want 1/1", failed, done)
	}
}

func TestShutdownAccountingAndRecovery(t *testing.T) {
	const total = 1000
	h := newHarness(t)
	for i := 0; i < total; i++ {
		h.write(t, fmt.Sprintf("f%04d.txt", i), fmt.Sprintf("record %d\n", i))
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := h.pipeline(t, nil, 0)
	done := make(chan error, 1)
	go func() { done <- p.Supervise(ctx, nil) }()

	// Cancel in the middle of the batch: enough work is in flight to exercise
	// the drain, the journal and the staging cleanup at the same time.
	waitForCond(t, "50 published jobs", func() bool {
		return h.metrics.CounterValue("pipeline_jobs_completed_total", map[string]string{"stage": "publish"}) >= 50
	}, 180*time.Second)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Supervise after cancel = %v, want nil", err)
		}
	case <-time.After(120 * time.Second):
		t.Fatal("pipeline did not stop after cancellation")
	}

	jobs, err := h.store.List(context.Background())
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	var doneCount, failedCount, runningCount int
	for _, j := range jobs {
		switch j.State {
		case job.StateDone:
			doneCount++
		case job.StateFailed:
			failedCount++
		case job.StateRunning, job.StatePending:
			runningCount++
			if _, err := os.Stat(j.SourcePath); err != nil {
				t.Errorf("job %s is unfinished but its input is gone: %v", j.ID, err)
			}
		}
	}
	enqueued := int(h.metrics.CounterValue("pipeline_jobs_enqueued_total", map[string]string{"stage": "source"}))
	if doneCount+failedCount+runningCount != enqueued {
		t.Fatalf("accounting broken: done=%d failed=%d running=%d, enqueued=%d",
			doneCount, failedCount, runningCount, enqueued)
	}
	if enqueued == 0 || enqueued > total {
		t.Fatalf("enqueued = %d, want 1..%d", enqueued, total)
	}
	if failedCount != 0 {
		t.Fatalf("a clean batch produced %d dead letters", failedCount)
	}
	if runningCount == 0 {
		t.Log("note: cancellation landed after the source was already drained")
	}

	// out/ must only ever contain complete artefacts. A job marked done always
	// has a visible artefact; the reverse can lag by exactly one job when the
	// cancellation lands between the rename and the journal write (the documented
	// at-least-once window).
	published := h.assertOutDirIsClean(t)
	if published < doneCount {
		t.Fatalf("out/ holds %d artefacts but %d jobs are marked done", published, doneCount)
	}
	if published > doneCount+1 {
		t.Fatalf("out/ holds %d artefacts but only %d jobs are done", published, doneCount)
	}
	if published == doneCount+1 {
		t.Logf("note: one artefact landed just before its journal write (at-least-once window)")
	}

	// Recovery: requeue every unfinished job and let the watcher pick up the
	// rest of the inbox, exactly like a restart.
	var replay []*job.Job
	for _, j := range jobs {
		if j.Terminal() {
			continue
		}
		j.Reset(time.Now())
		if err := h.store.Save(context.Background(), j.Clone()); err != nil {
			t.Fatalf("requeue %s: %v", j.ID, err)
		}
		replay = append(replay, j)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	p2 := h.pipeline(t, replay, 0)
	done2 := make(chan error, 1)
	go func() { done2 <- p2.Supervise(ctx2, nil) }()

	waitForCond(t, fmt.Sprintf("%d artefacts", total), func() bool {
		return len(h.gzFiles(t)) >= total
	}, 300*time.Second)
	cancel2()
	select {
	case err := <-done2:
		if err != nil {
			t.Fatalf("recovery run: %v", err)
		}
	case <-time.After(120 * time.Second):
		t.Fatal("recovery run did not stop")
	}

	if got := h.assertOutDirIsClean(t); got != total {
		t.Fatalf("after recovery out/ holds %d artefacts, want %d", got, total)
	}
	entries, err := os.ReadDir(h.cfg.FailedDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			t.Fatalf("unexpected dead letter after recovery: %s", e.Name())
		}
	}
}

func waitForCond(t *testing.T, what string, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
