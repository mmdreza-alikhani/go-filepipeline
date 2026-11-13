package job_test

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"example.com/filepipeline/internal/job"
)

func newTestStore(t *testing.T) *job.Store {
	t.Helper()
	s, err := job.NewStore(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestStoreSaveLoadList(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)

	j := job.New("abc123", filepath.Join("inbox", "a.txt"), 42, "trace", now)
	j.BumpAttempt(job.StageScan, now)
	if err := s.Save(ctx, j); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Load("abc123")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.ID != j.ID || got.Size != j.Size || got.Attempt(job.StageScan) != 1 {
		t.Fatalf("round trip mismatch: %+v", got)
	}

	other := job.New("def456", filepath.Join("inbox", "b.txt"), 7, "trace2", now.Add(time.Second))
	if err := s.Save(ctx, other); err != nil {
		t.Fatalf("Save: %v", err)
	}
	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("List returned %d jobs, want 2", len(list))
	}
	if list[0].ID != "abc123" || list[1].ID != "def456" {
		t.Fatalf("List order = %s,%s", list[0].ID, list[1].ID)
	}

	// The overwrite must be atomic and leave no temporary files behind.
	if err := s.Save(ctx, got); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(s.Dir(), "tmp"))
	if err != nil {
		t.Fatalf("ReadDir tmp: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("journal tmp dir has %d leftovers", len(entries))
	}
}

func TestStoreRejectsTraversal(t *testing.T) {
	s := newTestStore(t)
	bad := job.New("../escape", "inbox/a.txt", 1, "t", time.Now())
	if err := s.Save(context.Background(), bad); !errors.Is(err, job.ErrInvalidID) {
		t.Fatalf("Save with a traversing id = %v, want ErrInvalidID", err)
	}
	if _, err := s.Load("../../etc/passwd"); !errors.Is(err, job.ErrInvalidID) {
		t.Fatalf("Load with a traversing id = %v, want ErrInvalidID", err)
	}
}

func TestStoreCanceledContext(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	j := job.New("abc", "inbox/a.txt", 1, "t", time.Now())
	if err := s.Save(ctx, j); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save on a cancelled context = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(s.Path("abc")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("cancelled Save created a document: %v", err)
	}
}

func TestStoreQuarantinesCorruptDocument(t *testing.T) {
	s := newTestStore(t)
	if err := os.WriteFile(s.Path("broken"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	good := job.New("good", "inbox/a.txt", 1, "t", time.Now())
	if err := s.Save(context.Background(), good); err != nil {
		t.Fatal(err)
	}

	list, err := s.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].ID != "good" {
		t.Fatalf("List = %+v, want only the good job", list)
	}
	if _, err := os.Stat(s.Path("broken") + ".corrupt"); err != nil {
		t.Fatalf("corrupt document was not quarantined: %v", err)
	}
}

func TestStoreDelete(t *testing.T) {
	s := newTestStore(t)
	j := job.New("abc", "inbox/a.txt", 1, "t", time.Now())
	if err := s.Save(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("abc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete("abc"); err != nil {
		t.Fatalf("Delete of a missing document = %v, want nil", err)
	}
	if _, err := s.Load("abc"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Load after Delete = %v", err)
	}
}

func TestStoreDocumentIsStableJSON(t *testing.T) {
	s := newTestStore(t)
	j := job.New("abc", "inbox/a.txt", 1, "trace", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := s.Save(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(s.Path("abc"))
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("journal document is not valid JSON: %v", err)
	}
	for _, field := range []string{"id", "state", "source_path", "attempts", "trace_id"} {
		if _, ok := decoded[field]; !ok {
			t.Errorf("journal document is missing %q", field)
		}
	}
}
