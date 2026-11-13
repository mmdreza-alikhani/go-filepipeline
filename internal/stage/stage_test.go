package stage_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"example.com/filepipeline/internal/config"
	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
	"example.com/filepipeline/internal/stage"
)

func newDeps(t *testing.T) stage.Deps {
	t.Helper()
	root := t.TempDir()
	cfg := *config.Default()
	cfg.WorkDir = root
	cfg.InboxDir = filepath.Join(root, "inbox")
	cfg.OutDir = filepath.Join(root, "out")
	cfg.FailedDir = filepath.Join(root, "failed")
	cfg.MaxFileSize = 1 << 20
	for _, dir := range []string{cfg.InboxDir, cfg.TempRoot(), cfg.OutDir, cfg.FailedDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	return stage.Deps{
		Config:  cfg,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: obs.NewMetrics(),
		Clock:   job.NewFakeClock(time.Now()),
		Probe:   stage.ProbeFunc(func(string) (bool, error) { return false, nil }),
	}
}

func buildStages(t *testing.T, d stage.Deps) map[job.Stage]stage.Stage {
	t.Helper()
	stages, err := stage.Build(d)
	if err != nil {
		t.Fatalf("stage.Build: %v", err)
	}
	out := make(map[job.Stage]stage.Stage, len(stages))
	for _, s := range stages {
		out[s.Name()] = s
	}
	return out
}

func writeInbox(t *testing.T, d stage.Deps, name, content string) *job.Job {
	t.Helper()
	path := filepath.Join(d.Config.InboxDir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return job.New("job-"+job.SafeFileName(name), path, info.Size(), "trace", time.Now())
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func TestBuildUnknownStage(t *testing.T) {
	d := newDeps(t)
	d.Config.Stages = []config.StageConfig{{Name: "teleport"}}
	_, err := stage.Build(d)
	if err == nil {
		t.Fatal("Build accepted an unknown stage")
	}
	if !strings.Contains(err.Error(), "scan") {
		t.Fatalf("error does not list the registered stages: %v", err)
	}
}

func TestScanHashesAndCreatesScratch(t *testing.T) {
	d := newDeps(t)
	stages := buildStages(t, d)
	j := writeInbox(t, d, "a.txt", "hello\n")

	out, err := stages[job.StageScan].Process(context.Background(), j)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	sum := sha256.Sum256([]byte("hello\n"))
	if out.Hash != hex.EncodeToString(sum[:]) {
		t.Fatalf("hash = %s, want %x", out.Hash, sum)
	}
	if out.Size != 6 {
		t.Fatalf("size = %d, want 6", out.Size)
	}
	if out.Stage != job.StageScan {
		t.Fatalf("stage cursor = %s, want scan", out.Stage)
	}
	if _, err := os.Stat(out.TempDir); err != nil {
		t.Fatalf("scratch dir was not created: %v", err)
	}
}

func TestScanRejectsOversizedFile(t *testing.T) {
	d := newDeps(t)
	d.Config.MaxFileSize = 4
	stages := buildStages(t, d)
	j := writeInbox(t, d, "big.txt", "0123456789")

	_, err := stages[job.StageScan].Process(context.Background(), j)
	if !stage.IsPermanent(err) {
		t.Fatalf("err = %v, want a permanent job error", err)
	}
	if !errors.Is(err, stage.ErrFileTooLarge) {
		t.Fatalf("err = %v, want ErrFileTooLarge", err)
	}
	if stage.IsFatal(err) {
		t.Fatal("an oversized file must not be treated as a pipeline fatal error")
	}
}

func TestScanSkipsPublishedContent(t *testing.T) {
	d := newDeps(t)
	d.Probe = stage.ProbeFunc(func(string) (bool, error) { return true, nil })
	stages := buildStages(t, d)
	j := writeInbox(t, d, "a.txt", "hello\n")

	out, err := stages[job.StageScan].Process(context.Background(), j)
	if !errors.Is(err, stage.ErrSkip) {
		t.Fatalf("err = %v, want ErrSkip", err)
	}
	if out == nil || !out.Terminal() {
		t.Fatalf("skipped job = %+v, want a terminal state", out)
	}
}

func TestScanVanishedFileIsSkip(t *testing.T) {
	d := newDeps(t)
	stages := buildStages(t, d)
	j := job.New("gone", filepath.Join(d.Config.InboxDir, "gone.txt"), 3, "t", time.Now())
	if _, err := stages[job.StageScan].Process(context.Background(), j); !errors.Is(err, stage.ErrSkip) {
		t.Fatalf("err = %v, want ErrSkip", err)
	}
}

func TestScanRejectsUnsafeName(t *testing.T) {
	d := newDeps(t)
	stages := buildStages(t, d)
	j := job.New("bad", filepath.Join(d.Config.InboxDir, "a.txt"), 3, "t", time.Now())
	j.FileName = "../etc/passwd"
	if _, err := stages[job.StageScan].Process(context.Background(), j); !stage.IsPermanent(err) {
		t.Fatalf("err = %v, want a permanent error", err)
	}
}

func TestScanAbortsWhenCancelled(t *testing.T) {
	d := newDeps(t)
	stages := buildStages(t, d)
	j := writeInbox(t, d, "a.txt", strings.Repeat("x", 4096))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := stages[job.StageScan].Process(ctx, j); err == nil {
		t.Fatal("scan ignored a cancelled context")
	}
}

func TestConvertNormalisation(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "hello\n", "hello\n"},
		{"no trailing newline", "hello", "hello\n"},
		{"crlf", "a\r\nb\r\n", "a\nb\n"},
		{"lone cr", "a\rb", "ab\n"},
		{"trailing spaces", "a   \nb\t\t\n", "a\nb\n"},
		{"interior spaces kept", "a  b\n", "a  b\n"},
		{"blank lines kept", "a\n\n\nb", "a\n\n\nb\n"},
		{"empty", "", ""},
		{"unicode", "سلام دنیا\n", "سلام دنیا\n"},
		{"long line", strings.Repeat("y", 200_000), strings.Repeat("y", 200_000) + "\n"},
		{"multibyte boundary", strings.Repeat("é", 100_000), strings.Repeat("é", 100_000) + "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newDeps(t)
			stages := buildStages(t, d)
			j := writeInbox(t, d, "a.txt", tc.in)
			if _, err := stages[job.StageScan].Process(context.Background(), j); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out, err := stages[job.StageConvert].Process(context.Background(), j)
			if err != nil {
				t.Fatalf("convert: %v", err)
			}
			if got := readFile(t, out.Converted); got != tc.want {
				t.Fatalf("converted = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestConvertRejectsBinaryInput(t *testing.T) {
	d := newDeps(t)
	stages := buildStages(t, d)
	j := writeInbox(t, d, "a.bin", "\xff\xfe\x00binary")
	if _, err := stages[job.StageScan].Process(context.Background(), j); err != nil {
		t.Fatalf("scan: %v", err)
	}
	_, err := stages[job.StageConvert].Process(context.Background(), j)
	if !stage.IsPermanent(err) || !errors.Is(err, stage.ErrNotText) {
		t.Fatalf("err = %v, want a permanent ErrNotText", err)
	}
	if _, statErr := os.Stat(j.Converted); statErr == nil {
		t.Fatal("a failed conversion left its output behind")
	}
}

func TestConvertEnforcesOutputBudget(t *testing.T) {
	d := newDeps(t)
	d.Config.MaxFileSize = 8
	stages := buildStages(t, d)

	j := job.New("budget", filepath.Join(d.Config.InboxDir, "a.txt"), 0, "t", time.Now())
	j.TempDir = t.TempDir()
	path := filepath.Join(d.Config.InboxDir, "a.txt")
	if err := os.WriteFile(path, []byte("0123456789abcdef\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	j.SourcePath = path
	j.FileName = "a.txt"

	_, err := stages[job.StageConvert].Process(context.Background(), j)
	if !stage.IsPermanent(err) || !errors.Is(err, stage.ErrFileTooLarge) {
		t.Fatalf("err = %v, want a permanent ErrFileTooLarge", err)
	}
}

func TestConvertRequiresScratchDir(t *testing.T) {
	d := newDeps(t)
	stages := buildStages(t, d)
	j := writeInbox(t, d, "a.txt", "x")
	if _, err := stages[job.StageConvert].Process(context.Background(), j); !stage.IsFatal(err) {
		t.Fatalf("err = %v, want a fatal error about the missing scratch dir", err)
	}
}

// argsAfterSeparator returns the arguments that follow a bare "--".
func argsAfterSeparator(args []string) []string {
	for i, a := range args {
		if a == "--" {
			return args[i+1:]
		}
	}
	return nil
}

// TestHelperProcess is the external converter used by the exec tests. Running it
// normally is a no-op; when the pipeline invokes the test binary with
// "-test.run=TestHelperProcess -- <mode> ..." it behaves like a small tool.
func TestHelperProcess(t *testing.T) {
	args := argsAfterSeparator(os.Args)
	if len(args) == 0 {
		return
	}
	switch args[0] {
	case "copy":
		if len(args) < 3 {
			os.Exit(2)
		}
		data, err := os.ReadFile(args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, "helper:", err)
			os.Exit(2)
		}
		if err := os.WriteFile(args[2], data, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "helper:", err)
			os.Exit(2)
		}
		os.Exit(0)
	case "fail":
		fmt.Fprintln(os.Stderr, "helper: deliberately failing")
		os.Exit(3)
	case "sleep":
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
	os.Exit(2)
}

func newExecDeps(t *testing.T, mode ...string) stage.Deps {
	t.Helper()
	d := newDeps(t)
	cmd := append([]string{os.Args[0], "-test.run=TestHelperProcess", "--"}, mode...)
	d.Config.Convert.Command = cmd
	return d
}

func TestConvertExternalCommand(t *testing.T) {
	d := newExecDeps(t, "copy", "{in}", "{out}")
	stages := buildStages(t, d)
	j := writeInbox(t, d, "a.txt", "external\n")

	if _, err := stages[job.StageScan].Process(context.Background(), j); err != nil {
		t.Fatalf("scan: %v", err)
	}
	out, err := stages[job.StageConvert].Process(context.Background(), j)
	if err != nil {
		t.Fatalf("convert with an external tool: %v", err)
	}
	if got := readFile(t, out.Converted); got != "external\n" {
		t.Fatalf("converted = %q", got)
	}
}

func TestConvertExternalCommandFailure(t *testing.T) {
	d := newExecDeps(t, "fail")
	stages := buildStages(t, d)
	j := writeInbox(t, d, "a.txt", "external\n")
	if _, err := stages[job.StageScan].Process(context.Background(), j); err != nil {
		t.Fatalf("scan: %v", err)
	}
	_, err := stages[job.StageConvert].Process(context.Background(), j)
	if err == nil {
		t.Fatal("a failing external tool was reported as success")
	}
	if stage.IsPermanent(err) || stage.IsFatal(err) {
		t.Fatalf("a tool failure must stay retryable, got %v", err)
	}
	if !strings.Contains(err.Error(), "deliberately failing") {
		t.Fatalf("error does not include the tool's stderr: %v", err)
	}
}

func TestConvertExternalCommandIsKilledOnCancel(t *testing.T) {
	d := newExecDeps(t, "sleep")
	stages := buildStages(t, d)
	j := writeInbox(t, d, "a.txt", "external\n")
	if _, err := stages[job.StageScan].Process(context.Background(), j); err != nil {
		t.Fatalf("scan: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := stages[job.StageConvert].Process(ctx, j)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a cancelled conversion reported success")
	}
	if elapsed > 10*time.Second {
		t.Fatalf("cancellation took %v, the child was not killed", elapsed)
	}
}

func TestCompressProducesDeterministicGzip(t *testing.T) {
	d := newDeps(t)
	stages := buildStages(t, d)
	j := writeInbox(t, d, "a.txt", "compress me\n")
	if _, err := stages[job.StageScan].Process(context.Background(), j); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if _, err := stages[job.StageConvert].Process(context.Background(), j); err != nil {
		t.Fatalf("convert: %v", err)
	}
	out, err := stages[job.StageCompress].Process(context.Background(), j)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if out.OutSize <= 0 {
		t.Fatalf("artefact size = %d", out.OutSize)
	}
	data, err := os.ReadFile(out.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("artefact is not gzip: %v", err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if string(plain) != "compress me\n" {
		t.Fatalf("decompressed = %q", plain)
	}

	// A second run over the same content must produce identical bytes.
	j2 := writeInbox(t, d, "b.txt", "compress me\n")
	j2.ID = "second"
	if _, err := stages[job.StageScan].Process(context.Background(), j2); err != nil {
		t.Fatal(err)
	}
	if _, err := stages[job.StageConvert].Process(context.Background(), j2); err != nil {
		t.Fatal(err)
	}
	out2, err := stages[job.StageCompress].Process(context.Background(), j2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, []byte(readFile(t, out2.Artifact))) {
		t.Fatal("two runs of the same content produced different gzip bytes")
	}
	if out2.Hash != out.Hash {
		t.Fatalf("hashes differ: %s vs %s", out.Hash, out2.Hash)
	}
}

func TestCompressRequiresConvertedArtefact(t *testing.T) {
	d := newDeps(t)
	stages := buildStages(t, d)
	j := job.New("x", "inbox/a.txt", 1, "t", time.Now())
	j.TempDir = t.TempDir()
	if _, err := stages[job.StageCompress].Process(context.Background(), j); !stage.IsFatal(err) {
		t.Fatalf("err = %v, want a fatal error", err)
	}
}

func TestStageKinds(t *testing.T) {
	d := newDeps(t)
	stages := buildStages(t, d)
	if got := stages[job.StageScan].Kind(); got != stage.KindIO {
		t.Errorf("scan kind = %v, want io", got)
	}
	if got := stages[job.StageConvert].Kind(); got != stage.KindCPU {
		t.Errorf("convert kind = %v, want cpu", got)
	}
	if got := stages[job.StageCompress].Kind(); got != stage.KindCPU {
		t.Errorf("compress kind = %v, want cpu", got)
	}
}
