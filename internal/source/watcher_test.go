package source_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
	"example.com/filepipeline/internal/source"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestPollingEmitsEachFileOnce(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "a.txt", "a")
	write(t, dir, "skip.tmp", "temp")
	write(t, dir, "skip.part", "partial")
	write(t, dir, ".hidden", "hidden")
	if err := os.MkdirAll(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}

	p, err := source.NewPolling(source.Options{
		InboxDir: dir,
		Interval: 10 * time.Millisecond,
		Metrics:  obs.NewMetrics(),
	})
	if err != nil {
		t.Fatalf("NewPolling: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var names []string
	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx, func(_ context.Context, j *job.Job) error {
			mu.Lock()
			names = append(names, j.FileName)
			mu.Unlock()
			return nil
		})
	}()

	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(names)
	}
	waitFor(t, "the first job", func() bool { return count() >= 1 }, 5*time.Second)

	write(t, dir, "b.txt", "b")
	waitFor(t, "the second job", func() bool { return count() >= 2 }, 5*time.Second)

	// Several more ticks must not re-emit the same files.
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(names) != 2 {
		t.Fatalf("emitted %d jobs (%v), want 2", len(names), names)
	}
	if names[0] != "a.txt" || names[1] != "b.txt" {
		t.Fatalf("emitted %v, want [a.txt b.txt]", names)
	}
}

func TestPollingIgnoresUnstableFiles(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "still-writing.txt", "x")

	p, err := source.NewPolling(source.Options{
		InboxDir:  dir,
		Interval:  10 * time.Millisecond,
		StableAge: time.Hour,
		Metrics:   obs.NewMetrics(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	emitted := 0
	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx, func(context.Context, *job.Job) error {
			mu.Lock()
			emitted++
			mu.Unlock()
			return nil
		})
	}()
	time.Sleep(120 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if emitted != 0 {
		t.Fatalf("emitted %d unstable files, want 0", emitted)
	}
}

func TestPollingRecreatesMissingInbox(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "inbox")
	p, err := source.NewPolling(source.Options{InboxDir: dir, Interval: 10 * time.Millisecond, Metrics: obs.NewMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, func(context.Context, *job.Job) error { return nil }) }()
	waitFor(t, "the inbox to be recreated", func() bool {
		_, err := os.Stat(dir)
		return err == nil
	}, 5*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestReplayEmitsRecoveredJobsFirst(t *testing.T) {
	now := time.Now()
	jobs := []*job.Job{
		job.New("r1", "inbox/a.txt", 1, "t1", now),
		job.New("r2", "inbox/b.txt", 1, "t2", now),
	}
	dir := t.TempDir()
	write(t, dir, "live.txt", "live")
	poll, err := source.NewPolling(source.Options{InboxDir: dir, Interval: 10 * time.Millisecond, Metrics: obs.NewMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	replay := source.NewReplay(poll, jobs, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var ids []string
	done := make(chan error, 1)
	go func() {
		done <- replay.Run(ctx, func(_ context.Context, j *job.Job) error {
			mu.Lock()
			ids = append(ids, j.ID)
			mu.Unlock()
			return nil
		})
	}()
	waitFor(t, "replayed and live jobs", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ids) >= 3
	}, 5*time.Second)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 3 || ids[0] != "r1" || ids[1] != "r2" {
		t.Fatalf("emitted %v, want the replay first", ids)
	}
}

func TestLimitStopsTheSource(t *testing.T) {
	now := time.Now()
	jobs := make([]*job.Job, 0, 5)
	for i := 0; i < 5; i++ {
		jobs = append(jobs, job.New(string(rune('a'+i)), "inbox/x.txt", 1, "t", now))
	}
	limited := source.NewLimit(source.NewReplay(nil, jobs, nil), 2, nil)
	var count int
	if err := limited.Run(context.Background(), func(context.Context, *job.Job) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if count != 2 {
		t.Fatalf("emitted %d jobs, want 2", count)
	}

	unlimited := source.NewLimit(source.NewReplay(nil, jobs, nil), 0, nil)
	count = 0
	if err := unlimited.Run(context.Background(), func(context.Context, *job.Job) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if count != 5 {
		t.Fatalf("emitted %d jobs, want 5", count)
	}
}

func TestPollingRequiresInbox(t *testing.T) {
	if _, err := source.NewPolling(source.Options{}); err == nil {
		t.Fatal("NewPolling accepted an empty inbox path")
	}
}
