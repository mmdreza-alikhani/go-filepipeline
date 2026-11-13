package job_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"example.com/filepipeline/internal/job"
)

func TestParseFileNameAccepts(t *testing.T) {
	for _, name := range []string{
		"report.txt",
		"a",
		"2024-01-05.log",
		"x_y-z.csv",
		"report.final.txt",
		"данные.txt",
		"数据.csv",
	} {
		got, err := job.ParseFileName(name)
		if err != nil {
			t.Errorf("ParseFileName(%q) = error %v, want nil", name, err)
			continue
		}
		if got != name {
			t.Errorf("ParseFileName(%q) = %q, want unchanged", name, got)
		}
	}
}

func TestParseFileNameRejects(t *testing.T) {
	for _, name := range []string{
		"",
		".hidden",
		"a.tmp",
		"a.TMP",
		"a.part",
		"a.crdownload",
		"a.swp",
		"dir/x.txt",
		`dir\x.txt`,
		"..",
		"a\x00b",
		"a\nb",
		"a\rb",
		"a\x1bb",
		"a.",
		"trailing ",
		"backup~",
		strings.Repeat("x", 300),
	} {
		if got, err := job.ParseFileName(name); err == nil {
			t.Errorf("ParseFileName(%q) = %q, want error", name, got)
		}
	}
}

func TestValidateID(t *testing.T) {
	for _, id := range []string{"abc", "ABC-123_x", job.NewID()} {
		if err := job.ValidateID(id); err != nil {
			t.Errorf("ValidateID(%q) = %v, want nil", id, err)
		}
	}
	for _, id := range []string{"", "..", "a/b", `a\b`, "a b", "a.json", strings.Repeat("a", 65), "a\x00"} {
		if err := job.ValidateID(id); err == nil {
			t.Errorf("ValidateID(%q) = nil, want error", id)
		}
	}
}

func TestNewIDIsUniqueAndValid(t *testing.T) {
	seen := make(map[string]bool, 500)
	for i := 0; i < 500; i++ {
		id := job.NewID()
		if err := job.ValidateID(id); err != nil {
			t.Fatalf("NewID produced an invalid id %q: %v", id, err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestSafeFileName(t *testing.T) {
	cases := map[string]string{
		"report.txt":     "report.txt",
		"a b:c*.txt":     "a_b_c_.txt",
		"../../etc/pass": "pass",
		"":               "unnamed",
	}
	for in, want := range cases {
		if got := job.SafeFileName(in); got != want {
			t.Errorf("SafeFileName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJobLifecycle(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	j := job.New("id1", filepath.Join("inbox", "a.txt"), 12, "trace1", now)
	if j.State != job.StatePending || j.Terminal() {
		t.Fatalf("new job state = %s, want pending and not terminal", j.State)
	}
	j.MarkRunning(job.StageScan, now)
	if j.State != job.StateRunning || j.Stage != job.StageScan {
		t.Fatalf("running job = %s/%s", j.State, j.Stage)
	}
	if got := j.BumpAttempt(job.StageScan, now); got != 1 {
		t.Fatalf("BumpAttempt = %d, want 1", got)
	}
	clone := j.Clone()
	clone.BumpAttempt(job.StageScan, now)
	if j.Attempt(job.StageScan) != 1 {
		t.Fatal("Clone shares the attempts map with the original")
	}
	j.MarkDone(now)
	if !j.Terminal() || j.FinishedAt == nil || j.State != job.StateDone {
		t.Fatalf("done job = %+v", j)
	}
}

func TestJobResetForReplay(t *testing.T) {
	now := time.Now()
	j := job.New("id1", "inbox/a.txt", 10, "t", now)
	j.BumpAttempt(job.StageScan, now)
	j.TempDir = "/tmp/x"
	j.Artifact = "/tmp/x/a.gz"
	j.Converted = "/tmp/x/a.txt"
	j.MarkRunning(job.StageConvert, now)
	j.Reset(now)
	if j.State != job.StatePending || j.Stage != job.StageNone {
		t.Fatalf("reset state = %s/%s", j.State, j.Stage)
	}
	if len(j.Attempts) != 0 || j.TempDir != "" || j.Artifact != "" || j.Converted != "" {
		t.Fatalf("reset left stale fields: %+v", j)
	}
	if j.Resumes != 1 {
		t.Fatalf("Resumes = %d, want 1", j.Resumes)
	}
}

func TestBackoffDelay(t *testing.T) {
	b := job.Backoff{Base: 100 * time.Millisecond, Max: time.Second, Factor: 2, Jitter: 0, MaxAttempts: 5}
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second,
		time.Second,
	}
	for i, w := range want {
		if got := b.Delay(i+1, 0.5); got != w {
			t.Errorf("Delay(attempt=%d) = %v, want %v", i+1, got, w)
		}
	}

	jittered := b
	jittered.Jitter = 0.5
	if got := jittered.Delay(1, 0); got != 50*time.Millisecond {
		t.Errorf("Delay with rnd=0 = %v, want 50ms", got)
	}
	if got := jittered.Delay(1, 1); got != 150*time.Millisecond {
		t.Errorf("Delay with rnd=1 = %v, want 150ms", got)
	}
	if got := jittered.Delay(6, 1); got != time.Second {
		t.Errorf("Delay above the cap = %v, want 1s", got)
	}

	zero := job.Backoff{}
	got := zero.Normalize()
	if got.MaxAttempts < 1 || got.Base <= 0 || got.Factor < 1 {
		t.Fatalf("Normalize produced a dangerous policy: %+v", got)
	}
}

func TestFakeClock(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := job.NewFakeClock(base)
	if !c.Now().Equal(base) {
		t.Fatal("fake clock does not start at the given time")
	}
	if err := c.Sleep(context.Background(), time.Second); err != nil {
		t.Fatalf("Sleep(nil) = %v", err)
	}
	if got := c.Now().Sub(base); got != time.Second {
		t.Fatalf("clock advanced by %v, want 1s", got)
	}
	if sleeps := c.Sleeps(); len(sleeps) != 1 || sleeps[0] != time.Second {
		t.Fatalf("Sleeps = %v", sleeps)
	}
}

func FuzzParseFileName(f *testing.F) {
	for _, seed := range []string{
		"a.txt", "", ".", "..", "a/b", `a\b`, "a\x00", "a.tmp", "данные.csv",
		strings.Repeat("z", 256), "\xff\xfe", "a\r\nb", "con.txt",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		got, err := job.ParseFileName(name)
		if err != nil {
			if got != "" {
				t.Fatalf("ParseFileName(%q) returned %q together with an error", name, got)
			}
			return
		}
		if got != name {
			t.Fatalf("ParseFileName(%q) = %q, want unchanged", name, got)
		}
		if strings.ContainsAny(got, `/\`) {
			t.Fatalf("accepted a name with a separator: %q", got)
		}
		if got != filepath.Base(got) {
			t.Fatalf("accepted a name that is not its own base: %q", got)
		}
		if !utf8.ValidString(got) {
			t.Fatalf("accepted invalid UTF-8: %q", got)
		}
		if !filepath.IsLocal(got) {
			t.Fatalf("accepted a non local name: %q", got)
		}
	})
}

func FuzzValidateID(f *testing.F) {
	for _, seed := range []string{"", "a", "abc-123", "..", "a/b", "a\x00", strings.Repeat("a", 80)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, id string) {
		if err := job.ValidateID(id); err != nil {
			return
		}
		if strings.ContainsAny(id, `/\`) || strings.Contains(id, "..") {
			t.Fatalf("ValidateID accepted a traversing id %q", id)
		}
		if id != filepath.Base(id) {
			t.Fatalf("ValidateID accepted %q which is not its own base", id)
		}
	})
}
