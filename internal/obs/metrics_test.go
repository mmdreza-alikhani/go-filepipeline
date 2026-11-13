package obs_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"example.com/filepipeline/internal/obs"
)

func TestMetricsExpositionFormat(t *testing.T) {
	m := obs.NewMetrics()
	m.IncCounter("pipeline_jobs_total", "Jobs processed.", map[string]string{"stage": "scan"}, 3)
	m.IncCounter("pipeline_jobs_total", "Jobs processed.", map[string]string{"stage": "scan"}, 2)
	m.SetGauge("pipeline_queue_depth", "Queue depth.", map[string]string{"stage": "scan"}, 7)
	m.RegisterGaugeFunc("pipeline_live", "Live value.", nil, func() float64 { return 42 })
	m.ObserveHistogram("pipeline_latency_seconds", "Latency.",
		[]float64{0.1, 1}, map[string]string{"stage": "scan"}, 0.5)
	m.ObserveHistogram("pipeline_latency_seconds", "Latency.",
		[]float64{0.1, 1}, map[string]string{"stage": "scan"}, 2.0)

	var buf bytes.Buffer
	if err := m.WriteProm(&buf); err != nil {
		t.Fatalf("WriteProm: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"# HELP pipeline_jobs_total Jobs processed.",
		"# TYPE pipeline_jobs_total counter",
		`pipeline_jobs_total{stage="scan"} 5`,
		"# TYPE pipeline_queue_depth gauge",
		`pipeline_queue_depth{stage="scan"} 7`,
		"pipeline_live 42",
		"# TYPE pipeline_latency_seconds histogram",
		`pipeline_latency_seconds_bucket{stage="scan",le="0.1"} 0`,
		`pipeline_latency_seconds_bucket{stage="scan",le="1"} 1`,
		`pipeline_latency_seconds_bucket{stage="scan",le="+Inf"} 2`,
		`pipeline_latency_seconds_sum{stage="scan"} 2.5`,
		`pipeline_latency_seconds_count{stage="scan"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition is missing %q\n---\n%s", want, out)
		}
	}
}

func TestMetricsEscapingAndOrdering(t *testing.T) {
	m := obs.NewMetrics()
	m.IncCounter("weird_total", "Has a \\ and a\nnewline.", map[string]string{"path": "a\"b\\c\nd"}, 1)
	m.IncCounter("a_total", "First alphabetically.", nil, 1)

	var buf bytes.Buffer
	if err := m.WriteProm(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `weird_total{path="a\"b\\c\nd"} 1`) {
		t.Errorf("label value was not escaped:\n%s", out)
	}
	if strings.Index(out, "a_total") > strings.Index(out, "weird_total") {
		t.Errorf("families are not sorted:\n%s", out)
	}
}

func TestMetricsHandler(t *testing.T) {
	m := obs.NewMetrics()
	m.IncCounter("x_total", "X.", nil, 1)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content type = %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "x_total 1") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestCounterValue(t *testing.T) {
	m := obs.NewMetrics()
	if got := m.CounterValue("missing_total", nil); got != 0 {
		t.Fatalf("missing counter = %v, want 0", got)
	}
	m.IncCounter("present_total", "P.", map[string]string{"stage": "scan"}, 1.5)
	if got := m.CounterValue("present_total", map[string]string{"stage": "scan"}); got != 1.5 {
		t.Fatalf("counter = %v, want 1.5", got)
	}
	if got := m.CounterValue("present_total", map[string]string{"stage": "other"}); got != 0 {
		t.Fatalf("other label series = %v, want 0", got)
	}
}

func TestTraceHandlerInjectsTraceID(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelDebug, false)
	ctx := obs.WithTraceID(context.Background(), "trace-123")

	logger.InfoContext(ctx, "hello")
	if !strings.Contains(buf.String(), "trace_id=trace-123") {
		t.Fatalf("log record has no trace id: %q", buf.String())
	}

	buf.Reset()
	logger.InfoContext(context.Background(), "no trace")
	if strings.Contains(buf.String(), "trace_id") {
		t.Fatalf("log record got a trace id from nowhere: %q", buf.String())
	}
}

func TestTraceHandlerJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo, true)
	logger.InfoContext(obs.WithTraceID(context.Background(), "abc"), "msg", "job", "j1")
	out := buf.String()
	if !strings.Contains(out, `"trace_id":"abc"`) || !strings.Contains(out, `"job":"j1"`) {
		t.Fatalf("json log = %s", out)
	}
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]slog.Level{
		"":        slog.LevelInfo,
		"debug":   slog.LevelDebug,
		"WARN":    slog.LevelWarn,
		"error":   slog.LevelError,
		"warning": slog.LevelWarn,
	} {
		got, err := obs.ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := obs.ParseLevel("verbose"); err == nil {
		t.Error("ParseLevel accepted an unknown level")
	}
}
