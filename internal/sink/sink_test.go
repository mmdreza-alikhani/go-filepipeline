package sink_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
	"example.com/filepipeline/internal/sink"
)

const testHash = "ab3f9c1d2e4b5a69788796a5b4c3d2e1f00112233445566778899aabbccddeeff"

func gzipBytes(t *testing.T, content string) []byte {
	t.Helper()
	var buf strings.Builder
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return []byte(buf.String())
}

func newPublisher(t *testing.T, outDir string) *sink.Publisher {
	t.Helper()
	p, err := sink.NewPublisher(sink.PublisherOptions{OutDir: outDir, Metrics: obs.NewMetrics()})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return p
}

func jobFor(t *testing.T, dir string, artifact []byte) *job.Job {
	t.Helper()
	scratch := filepath.Join(t.TempDir(), "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(scratch, "artifact.gz")
	if err := os.WriteFile(path, artifact, 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	j := job.New("job1", filepath.Join(dir, "inbox", "a.txt"), 12, "trace", now)
	j.Hash = testHash
	j.Artifact = path
	j.OutSize = int64(len(artifact))
	j.TempDir = scratch
	return j
}

func TestPublisherWritesAtomically(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "out")
	pub := newPublisher(t, out)
	j := jobFor(t, root, gzipBytes(t, "hello\n"))

	if err := pub.Publish(context.Background(), j); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	final := sink.FinalPath(out, testHash)
	data, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("final artefact missing: %v", err)
	}
	if string(data) != string(gzipBytes(t, "hello\n")) {
		t.Fatal("published bytes differ from the artefact")
	}
	manifestData, err := os.ReadFile(sink.ManifestPath(out, testHash))
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	var m sink.Manifest
	if err := json.Unmarshal(manifestData, &m); err != nil {
		t.Fatalf("manifest is not valid JSON: %v", err)
	}
	if m.Hash != testHash || m.JobID != "job1" || m.TraceID != "trace" || m.FileName != "a.txt" {
		t.Fatalf("manifest = %+v", m)
	}
	if ok, err := pub.Published(testHash); err != nil || !ok {
		t.Fatalf("Published = %v, %v", ok, err)
	}
	if _, err := os.Stat(j.TempDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scratch dir was not removed: %v", err)
	}

	// The output tree must contain only complete artefacts and manifests.
	entries, err := os.ReadDir(filepath.Join(out, testHash[:2]))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".part") || strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("staging file leaked into out/: %s", e.Name())
		}
	}
	tmpEntries, err := os.ReadDir(filepath.Join(out, ".tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tmpEntries) != 0 {
		t.Fatalf("staging directory is not empty: %v", tmpEntries)
	}
}

func TestPublisherIsIdempotent(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "out")
	pub := newPublisher(t, out)
	j := jobFor(t, root, gzipBytes(t, "hello\n"))

	if err := pub.Publish(context.Background(), j); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	before, err := os.Stat(sink.FinalPath(out, testHash))
	if err != nil {
		t.Fatal(err)
	}

	// The first publish removed the scratch directory, so the artefact is gone.
	// The second publish must still succeed: the content is already published and
	// only the manifest is rewritten.
	if _, err := os.Stat(j.Artifact); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected the scratch artefact to be gone: %v", err)
	}
	if err := pub.Publish(context.Background(), j); err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	after, err := os.Stat(sink.FinalPath(out, testHash))
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Fatal("the second publish rewrote an already published artefact")
	}
}

func TestPublisherCancelledLeavesNothing(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "out")
	pub := newPublisher(t, out)
	j := jobFor(t, root, gzipBytes(t, strings.Repeat("x", 1000)))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pub.Publish(ctx, j); err == nil {
		t.Fatal("Publish with a cancelled context reported success")
	}
	if ok, _ := pub.Published(testHash); ok {
		t.Fatal("a cancelled publish produced a visible artefact")
	}
	entries, err := os.ReadDir(filepath.Join(out, ".tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cancelled publish left %d staging files", len(entries))
	}
}

func TestPublisherRejectsInvalidJobs(t *testing.T) {
	root := t.TempDir()
	pub := newPublisher(t, filepath.Join(root, "out"))

	j := jobFor(t, root, gzipBytes(t, "x"))
	j.Hash = "not-hex"
	if err := pub.Publish(context.Background(), j); err == nil {
		t.Fatal("Publish accepted a non hexadecimal hash")
	}

	j = jobFor(t, root, gzipBytes(t, "x"))
	j.Artifact = ""
	if err := pub.Publish(context.Background(), j); err == nil {
		t.Fatal("Publish accepted a job without an artefact")
	}

	if err := pub.Publish(context.Background(), nil); err == nil {
		t.Fatal("Publish accepted a nil job")
	}
	if _, err := pub.Published("zz"); err == nil {
		t.Fatal("Published accepted an invalid hash")
	}
}

func TestDeadLetterRecordsAndMovesInput(t *testing.T) {
	root := t.TempDir()
	inbox := filepath.Join(root, "inbox")
	failed := filepath.Join(root, "failed")
	if err := os.MkdirAll(inbox, 0o755); err != nil {
		t.Fatal(err)
	}
	dlq, err := sink.NewDeadLetter(sink.DeadLetterOptions{Dir: failed, Metrics: obs.NewMetrics()})
	if err != nil {
		t.Fatal(err)
	}

	src := filepath.Join(inbox, "broken.txt")
	if err := os.WriteFile(src, []byte("\xffbroken"), 0o644); err != nil {
		t.Fatal(err)
	}
	scratch := filepath.Join(t.TempDir(), "scratch")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}

	j := job.New("bad1", src, 7, "trace-bad", time.Now())
	j.Hash = testHash
	j.TempDir = scratch
	j.BumpAttempt(job.StageConvert, time.Now())
	j.BumpAttempt(job.StageConvert, time.Now())

	cause := errors.New("stage: input is not valid UTF-8 text")
	if err := dlq.Fail(context.Background(), j, job.StageConvert, cause); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(failed, "bad1.json"))
	if err != nil {
		t.Fatalf("failure record missing: %v", err)
	}
	var rec sink.Failure
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("failure record is not valid JSON: %v", err)
	}
	if rec.Stage != job.StageConvert {
		t.Errorf("stage = %q, want convert", rec.Stage)
	}
	if rec.Attempts != 2 || rec.AttemptsByStage[job.StageConvert] != 2 {
		t.Errorf("attempts = %d (%v), want 2", rec.Attempts, rec.AttemptsByStage)
	}
	if rec.Error == "" || !strings.Contains(rec.Error, "UTF-8") {
		t.Errorf("error = %q", rec.Error)
	}
	if rec.At.IsZero() {
		t.Error("timestamp is missing")
	}
	if rec.TraceID != "trace-bad" || rec.JobID != "bad1" {
		t.Errorf("record identity = %+v", rec)
	}

	if _, err := os.Stat(src); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the failed input was not moved: %v", err)
	}
	moved, err := os.ReadFile(filepath.Join(failed, "bad1__broken.txt"))
	if err != nil || string(moved) != "\xffbroken" {
		t.Fatalf("moved input = %q, %v", moved, err)
	}
	if _, err := os.Stat(scratch); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scratch dir was not removed: %v", err)
	}
}

func TestDeadLetterToleratesMissingInput(t *testing.T) {
	root := t.TempDir()
	failed := filepath.Join(root, "failed")
	dlq, err := sink.NewDeadLetter(sink.DeadLetterOptions{Dir: failed, Metrics: obs.NewMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	j := job.New("gone", filepath.Join(root, "inbox", "missing.txt"), 0, "t", time.Now())
	if err := dlq.Fail(context.Background(), j, job.StageScan, errors.New("boom")); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if _, err := os.Stat(filepath.Join(failed, "gone.json")); err != nil {
		t.Fatalf("failure record missing for a vanished input: %v", err)
	}
	if _, err := os.Stat(filepath.Join(failed, ".tmp")); err != nil {
		t.Fatalf("staging dir missing: %v", err)
	}
}

var _ = io.Discard
