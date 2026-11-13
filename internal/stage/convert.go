package stage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"example.com/filepipeline/internal/config"
	"example.com/filepipeline/internal/job"
	"example.com/filepipeline/internal/obs"
)

// ConvertOptions configures the convert stage.
type ConvertOptions struct {
	MaxFileSize int64
	Command     []string
	Log         *slog.Logger
	Metrics     *obs.Metrics
	Clock       job.Clock
}

// Convert rewrites the input into the canonical intermediate artefact: UTF-8
// text, LF line endings, no trailing whitespace, exactly one final newline.
//
// The built in normaliser streams byte by byte, so memory use is independent of
// file size. An external tool can replace it (see ConvertOptions.Command), in
// which case it runs under CommandContext + WaitDelay inside the per job
// scratch directory.
type Convert struct {
	o ConvertOptions
}

func newConvertStage(d Deps, _ config.StageConfig) (Stage, error) {
	return &Convert{o: ConvertOptions{
		MaxFileSize: d.Config.MaxFileSize,
		Command:     append([]string(nil), d.Config.Convert.Command...),
		Log:         d.Log,
		Metrics:     d.Metrics,
		Clock:       d.Clock,
	}}, nil
}

// Name implements Stage.
func (c *Convert) Name() job.Stage { return job.StageConvert }

// Kind implements Stage: normalising is pure user space work.
func (c *Convert) Kind() Kind { return KindCPU }

// Process implements Stage.
func (c *Convert) Process(ctx context.Context, j *job.Job) (*job.Job, error) {
	if j == nil {
		return nil, errors.New("convert: nil job")
	}
	if j.TempDir == "" {
		return nil, Fatal(errors.New("convert: job has no scratch directory (scan must run first)"))
	}

	src, err := os.Open(j.SourcePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, Permanent(fmt.Errorf("convert: source %q disappeared: %w", j.FileName, err))
		}
		if IsDiskError(err) {
			return nil, Fatal(err)
		}
		return nil, fmt.Errorf("convert: open %q: %w", j.FileName, err)
	}
	defer func() { _ = src.Close() }()

	dir := filepath.Join(j.TempDir, "convert")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		if IsDiskError(err) {
			return nil, Fatal(err)
		}
		return nil, fmt.Errorf("convert: create scratch dir: %w", err)
	}
	out := filepath.Join(dir, "converted.txt")

	var written int64
	if len(c.o.Command) > 0 {
		if err := c.runCommand(ctx, j, out); err != nil {
			_ = os.Remove(out)
			return nil, err
		}
		fi, err := os.Stat(out)
		if err != nil {
			return nil, fmt.Errorf("convert: external tool produced no output: %w", err)
		}
		written = fi.Size()
	} else {
		limited := io.LimitReader(ctxReader{ctx: ctx, r: src}, c.o.MaxFileSize+1)
		written, err = normalizeInto(ctx, limited, out, c.o.MaxFileSize)
		if err != nil {
			_ = os.Remove(out)
			if IsDiskError(err) {
				return nil, Fatal(err)
			}
			if errors.Is(err, ErrFileTooLarge) || errors.Is(err, ErrNotText) {
				return nil, Permanent(err)
			}
			return nil, err
		}
	}

	j.Converted = out
	j.OutSize = written
	j.MarkStageDone(job.StageConvert, c.o.Clock.Now())
	c.o.Metrics.IncCounter("pipeline_bytes_processed_total", "Bytes read by a stage.",
		map[string]string{"stage": string(job.StageConvert)}, float64(written))
	return j, nil
}

// runCommand executes the configured external converter.
//
// CommandContext kills the direct child when ctx is done. WaitDelay bounds the
// wait for the child's pipes afterwards, so a tool that leaks a descriptor or
// spawns a helper cannot pin the worker forever. A grandchild process may still
// survive on platforms without process groups; the pipeline therefore treats
// the scratch directory, not the process tree, as its cleanup unit.
func (c *Convert) runCommand(ctx context.Context, j *job.Job, out string) error {
	argv := make([]string, len(c.o.Command))
	for i, arg := range c.o.Command {
		arg = strings.ReplaceAll(arg, "{in}", j.SourcePath)
		arg = strings.ReplaceAll(arg, "{out}", out)
		arg = strings.ReplaceAll(arg, "{dir}", j.TempDir)
		argv[i] = arg
	}
	if strings.TrimSpace(argv[0]) == "" {
		return Fatal(errors.New("convert: convert.command[0] is empty"))
	}

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.WaitDelay = 2 * time.Second
	stderr := &tailBuffer{max: 4096}
	cmd.Stdout = io.Discard
	cmd.Stderr = stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, exec.ErrNotFound) {
			return Fatal(fmt.Errorf("convert: %q not found: %w", argv[0], err))
		}
		msg := stderr.String()
		if msg == "" {
			msg = "no stderr output"
		}
		return fmt.Errorf("convert: %s failed: %w (%s)", argv[0], err, msg)
	}
	return nil
}

// normalizeInto runs the normaliser into path and flushes the file durably.
func normalizeInto(ctx context.Context, r io.Reader, path string, maxOutput int64) (int64, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, fmt.Errorf("convert: create %s: %w", filepath.Base(path), err)
	}
	defer func() { _ = f.Close() }()

	bw := bufio.NewWriterSize(f, 64<<10)
	n := &normalizer{bw: bw, max: maxOutput, pending: make([]byte, 0, 4096)}
	if err := n.run(ctx, r); err != nil {
		return 0, err
	}
	if err := bw.Flush(); err != nil {
		return 0, fmt.Errorf("convert: flush: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("convert: sync: %w", err)
	}
	return n.written, nil
}

const maxPendingSpace = 4096

// normalizer is a streaming text canonicaliser. It holds at most one chunk, one
// incomplete rune and a bounded run of deferred whitespace, so its memory use is
// O(1) with respect to the input size.
type normalizer struct {
	bw       *bufio.Writer
	max      int64
	written  int64
	pending  []byte // deferred spaces and tabs at the end of the current line
	lineOpen bool   // the current line already has content
}

func (n *normalizer) put(b []byte) error {
	if n.written+int64(len(b)) > n.max {
		return fmt.Errorf("%w: output exceeded %d bytes", ErrFileTooLarge, n.max)
	}
	if _, err := n.bw.Write(b); err != nil {
		return fmt.Errorf("convert: write: %w", err)
	}
	n.written += int64(len(b))
	return nil
}

func (n *normalizer) flushPending() error {
	if len(n.pending) == 0 {
		return nil
	}
	if err := n.put(n.pending); err != nil {
		return err
	}
	n.pending = n.pending[:0]
	return nil
}

func (n *normalizer) emit(b []byte) error {
	if err := n.flushPending(); err != nil {
		return err
	}
	if err := n.put(b); err != nil {
		return err
	}
	n.lineOpen = true
	return nil
}

func (n *normalizer) deferSpace(b []byte) error {
	if len(n.pending)+len(b) > maxPendingSpace {
		if err := n.flushPending(); err != nil {
			return err
		}
	}
	n.pending = append(n.pending, b...)
	return nil
}

func (n *normalizer) newline() error {
	// Trailing whitespace is dropped with the line it belongs to.
	n.pending = n.pending[:0]
	if err := n.put([]byte("\n")); err != nil {
		return err
	}
	n.lineOpen = false
	return nil
}

func (n *normalizer) finish() error {
	n.pending = n.pending[:0]
	if !n.lineOpen {
		return nil
	}
	return n.put([]byte("\n"))
}

// run consumes r chunk by chunk, carrying at most three bytes of an incomplete
// rune across chunk boundaries.
func (n *normalizer) run(ctx context.Context, r io.Reader) error {
	buf := make([]byte, 64<<10)
	var carry []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		nr, err := r.Read(buf)
		if nr > 0 {
			chunk := buf[:nr]
			if len(carry) > 0 {
				joined := make([]byte, 0, len(carry)+nr)
				joined = append(joined, carry...)
				joined = append(joined, chunk...)
				chunk = joined
				carry = nil
			}
			rest, cerr := n.consume(chunk, false)
			if cerr != nil {
				return cerr
			}
			if len(rest) > 0 {
				carry = append(carry[:0], rest...)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("convert: read: %w", err)
		}
	}
	if len(carry) > 0 {
		if _, err := n.consume(carry, true); err != nil {
			return err
		}
	}
	return n.finish()
}

// consume processes complete runes and returns the trailing incomplete rune,
// if any. With eof set an incomplete rune is a malformed input error.
func (n *normalizer) consume(p []byte, eof bool) ([]byte, error) {
	for len(p) > 0 {
		if !utf8.FullRune(p) {
			if eof {
				return nil, fmt.Errorf("%w: truncated UTF-8 sequence at end of input", ErrNotText)
			}
			return p, nil
		}
		r, size := utf8.DecodeRune(p)
		if r == utf8.RuneError && size == 1 {
			return nil, fmt.Errorf("%w: invalid byte 0x%02x at offset %d", ErrNotText, p[0], n.written)
		}
		switch r {
		case '\r':
			// Dropped so CRLF and lone CR both collapse to a single newline.
		case '\n':
			if err := n.newline(); err != nil {
				return nil, err
			}
		case ' ', '\t':
			if err := n.deferSpace(p[:size]); err != nil {
				return nil, err
			}
		default:
			if err := n.emit(p[:size]); err != nil {
				return nil, err
			}
		}
		p = p[size:]
	}
	return nil, nil
}

// tailBuffer keeps the last max bytes written to it, which is enough to report
// an external tool's error without buffering its whole output.
type tailBuffer struct {
	buf bytes.Buffer
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if t.max <= 0 {
		t.max = 4096
	}
	if n >= t.max {
		t.buf.Reset()
		t.buf.Write(p[n-t.max:])
		return n, nil
	}
	if t.buf.Len()+n > t.max {
		existing := t.buf.Bytes()
		keep := t.max - n
		if keep < 0 {
			keep = 0
		}
		trimmed := append([]byte(nil), existing[len(existing)-keep:]...)
		t.buf.Reset()
		t.buf.Write(trimmed)
	}
	t.buf.Write(p)
	return n, nil
}

func (t *tailBuffer) String() string { return strings.TrimSpace(t.buf.String()) }
