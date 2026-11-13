// Package obs holds the observability primitives used by the file pipeline:
// a trace aware slog handler and a hand written Prometheus text-format registry.
package obs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

type traceIDKey struct{}

// WithTraceID attaches a trace identifier to ctx. Every record logged from a
// context carrying a trace id gets a trace_id attribute, which is how a single
// job can be followed across scan/convert/compress/publish in the logs.
func WithTraceID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDKey{}, id)
}

// TraceID returns the trace identifier stored in ctx, or "" when absent.
func TraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(traceIDKey{}).(string)
	return id
}

// TraceHandler injects the context trace id into every record it handles.
type TraceHandler struct {
	Inner slog.Handler
}

func (h TraceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.Inner.Enabled(ctx, level)
}

func (h TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := TraceID(ctx); id != "" {
		r = r.Clone()
		r.AddAttrs(slog.String("trace_id", id))
	}
	return h.Inner.Handle(ctx, r)
}

func (h TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return TraceHandler{Inner: h.Inner.WithAttrs(attrs)}
}

func (h TraceHandler) WithGroup(name string) slog.Handler {
	return TraceHandler{Inner: h.Inner.WithGroup(name)}
}

// NewLogger builds a logger that writes to w. jsonFormat switches between the
// human readable text handler and the structured JSON handler.
func NewLogger(w io.Writer, level slog.Level, jsonFormat bool) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if jsonFormat {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(TraceHandler{Inner: h})
}

// ParseLevel maps a config string onto a slog level.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("obs: unknown log level %q", s)
	}
}
