package config_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"example.com/filepipeline/internal/config"
)

func minimalJSON(t *testing.T) []byte {
	t.Helper()
	cfg := config.Default()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal default config: %v", err)
	}
	return data
}

func TestParseDefaults(t *testing.T) {
	cfg, err := config.Parse(minimalJSON(t))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.QueueSize != 16 {
		t.Errorf("QueueSize = %d, want 16", cfg.QueueSize)
	}
	if got := cfg.Compress.GzipLevel(); got != 6 {
		t.Errorf("GzipLevel = %d, want 6", got)
	}
	if cfg.Recovery.Mode != "requeue" {
		t.Errorf("Recovery.Mode = %q, want requeue", cfg.Recovery.Mode)
	}
	if len(cfg.Stages) != 3 || cfg.Stages[0].Name != "scan" {
		t.Fatalf("stages = %+v", cfg.Stages)
	}
	if cfg.JournalDir() != filepath.Join(cfg.WorkDir, "jobs") {
		t.Errorf("JournalDir = %q", cfg.JournalDir())
	}
}

func TestParseAppliesStageDefaults(t *testing.T) {
	raw := `{
	  "inbox_dir": "in", "out_dir": "out", "work_dir": "work", "failed_dir": "failed",
	  "max_file_size": 1024,
	  "stages": [{"name": "scan"}]
	}`
	cfg, err := config.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Stages[0].Workers != 1 {
		t.Errorf("default workers = %d, want 1", cfg.Stages[0].Workers)
	}
	if cfg.Stages[0].Timeout.D().String() != "30s" {
		t.Errorf("default timeout = %v", cfg.Stages[0].Timeout.D())
	}
	if cfg.Stages[0].QueueSize != cfg.QueueSize {
		t.Errorf("stage queue = %d, want %d", cfg.Stages[0].QueueSize, cfg.QueueSize)
	}
}

func TestParseExplicitZeroGzipLevel(t *testing.T) {
	raw := `{
	  "inbox_dir": "in", "out_dir": "out", "work_dir": "work", "failed_dir": "failed",
	  "max_file_size": 1024, "compress": {"level": 0},
	  "stages": [{"name": "scan"}]
	}`
	cfg, err := config.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.Compress.GzipLevel(); got != 0 {
		t.Fatalf("GzipLevel = %d, want an explicit 0", got)
	}
}

func TestParseRejectsUnboundedQueue(t *testing.T) {
	for _, size := range []int{1, 7, 33, 10000} {
		cfg := config.Default()
		cfg.QueueSize = size
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := config.Parse(data); err == nil {
			t.Errorf("Parse accepted queue_size=%d", size)
		}
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"unknown field": `{"inbox_dir":"in","out_dir":"o","work_dir":"w","failed_dir":"f","max_file_size":10,"nope":1,"stages":[{"name":"scan"}]}`,
		"trailing data": `{"inbox_dir":"in","out_dir":"o","work_dir":"w","failed_dir":"f","max_file_size":10,"stages":[{"name":"scan"}]} {}`,
		"empty stages":  `{"inbox_dir":"in","out_dir":"o","work_dir":"w","failed_dir":"f","max_file_size":10,"stages":[]}`,
		"no inbox":      `{"out_dir":"o","work_dir":"w","failed_dir":"f","max_file_size":10,"stages":[{"name":"scan"}]}`,
		"zero max size": `{"inbox_dir":"in","out_dir":"o","work_dir":"w","failed_dir":"f","max_file_size":0,"stages":[{"name":"scan"}]}`,
		"bad mode":      `{"inbox_dir":"in","out_dir":"o","work_dir":"w","failed_dir":"f","max_file_size":10,"recovery":{"mode":"sometimes"},"stages":[{"name":"scan"}]}`,
		"duplicate":     `{"inbox_dir":"in","out_dir":"o","work_dir":"w","failed_dir":"f","max_file_size":10,"stages":[{"name":"scan"},{"name":"scan"}]}`,
		"bad duration":  `{"inbox_dir":"in","out_dir":"o","work_dir":"w","failed_dir":"f","max_file_size":10,"poll_interval":"soon","stages":[{"name":"scan"}]}`,
	}
	for name, raw := range cases {
		if _, err := config.Parse([]byte(raw)); err == nil {
			t.Errorf("%s: Parse accepted an invalid configuration", name)
		}
	}
}

func TestLoadExample(t *testing.T) {
	path := filepath.Join("..", "..", "configs", "config.example.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read example config: %v", err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		t.Fatalf("example config is invalid: %v", err)
	}
	if cfg.Describe() == "" {
		t.Fatal("Describe returned an empty summary")
	}
	if names := cfg.StageNames(); len(names) != 3 || names[0] != "scan" || names[2] != "compress" {
		t.Fatalf("stage order = %v", names)
	}
}

func TestDurationRoundTrip(t *testing.T) {
	var d config.Duration
	if err := json.Unmarshal([]byte(`"1.5s"`), &d); err != nil {
		t.Fatalf("unmarshal string duration: %v", err)
	}
	if d.D().String() != "1.5s" {
		t.Fatalf("duration = %v", d.D())
	}
	if err := json.Unmarshal([]byte(`250000000`), &d); err != nil {
		t.Fatalf("unmarshal numeric duration: %v", err)
	}
	if d.D().String() != "250ms" {
		t.Fatalf("duration = %v", d.D())
	}
	data, err := json.Marshal(config.Duration(d.D()))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `"250ms"` {
		t.Fatalf("marshal = %s", data)
	}
	if err := json.Unmarshal([]byte(`"nonsense"`), &d); err == nil {
		t.Fatal("invalid duration was accepted")
	}
}

func TestValidationMessagesAreJoined(t *testing.T) {
	cfg := config.Default()
	cfg.InboxDir = ""
	cfg.QueueSize = 1
	cfg.Compress.Level = intPtr(42)
	cfg.Stages[0].Workers = 0
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate accepted a broken config")
	}
	msg := err.Error()
	for _, want := range []string{"inbox_dir", "queue_size", "compress.level", "workers"} {
		if !strings.Contains(msg, want) {
			t.Errorf("validation error %q does not mention %q", msg, want)
		}
	}
}

func intPtr(v int) *int { return &v }

func FuzzParse(f *testing.F) {
	defaults, err := json.Marshal(config.Default())
	if err != nil {
		f.Fatalf("marshal default config: %v", err)
	}
	f.Add(defaults)
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`{"inbox_dir":"in","stages":[{"name":"scan"}]}`))
	f.Add([]byte(`{"poll_interval":"-1s","stages":[{"name":"scan"}]}`))
	f.Add([]byte(`{"max_file_size":-5,"queue_size":999,"stages":[]}`))
	f.Add([]byte(`{"compress":{"level":99},"stages":[{"name":"scan","workers":-1}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := config.Parse(data)
		if err != nil {
			return
		}
		if cfg == nil {
			t.Fatal("Parse returned a nil config without an error")
		}
		// Whatever survived validation must be self consistent, otherwise New()
		// would panic later on a channel with a negative capacity.
		if cfg.QueueSize < config.MinQueueSize || cfg.QueueSize > config.MaxQueueSize {
			t.Fatalf("accepted queue_size=%d", cfg.QueueSize)
		}
		for _, s := range cfg.Stages {
			if s.Workers < 1 || s.QueueSize < config.MinQueueSize || s.QueueSize > config.MaxQueueSize {
				t.Fatalf("accepted stage %+v", s)
			}
			if s.Timeout.D() <= 0 || s.Backoff.MaxAttempts < 1 {
				t.Fatalf("accepted stage with a broken policy: %+v", s)
			}
		}
		if cfg.MaxFileSize <= 0 {
			t.Fatalf("accepted max_file_size=%d", cfg.MaxFileSize)
		}
	})
}
