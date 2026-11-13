// Package config loads and validates the pipeline configuration. The file
// format is JSON so the binary keeps a zero dependency footprint: encoding/json
// is the only parser, which also keeps the fuzz target meaningful.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Bounds that the validator enforces. The queue bounds are the anti-pattern
// guard from the design: bounded channels are the backpressure mechanism, so an
// operator cannot configure an unbounded buffer.
const (
	MinQueueSize       = 8
	MaxQueueSize       = 32
	MaxWorkersPerStage = 256
	MaxFileSizeLimit   = int64(1) << 40 // 1 TiB
)

// Duration is a time.Duration that unmarshals from either a Go duration string
// ("250ms") or an integer number of nanoseconds.
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" || s == `""` {
		*d = 0
		return nil
	}
	if strings.HasPrefix(s, `"`) {
		var str string
		if err := json.Unmarshal(b, &str); err != nil {
			return err
		}
		v, err := time.ParseDuration(str)
		if err != nil {
			return fmt.Errorf("config: invalid duration %q: %w", str, err)
		}
		*d = Duration(v)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("config: invalid duration %s", s)
	}
	*d = Duration(n)
	return nil
}

// MarshalJSON implements json.Marshaler so a config round trips through the
// same format a human would write.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// BackoffConfig is the retry policy of one stage.
type BackoffConfig struct {
	Base        Duration `json:"base"`
	Max         Duration `json:"max"`
	Factor      float64  `json:"factor"`
	Jitter      float64  `json:"jitter"`
	MaxAttempts int      `json:"max_attempts"`
}

// StageConfig configures one stage's worker pool and retry policy.
type StageConfig struct {
	Name      string        `json:"name"`
	Workers   int           `json:"workers"`
	Timeout   Duration      `json:"timeout"`
	QueueSize int           `json:"queue_size"`
	Backoff   BackoffConfig `json:"backoff"`
}

// ConvertConfig optionally replaces the built in normaliser with an external
// tool. Placeholders {in}, {out} and {dir} are substituted per job.
type ConvertConfig struct {
	Command []string `json:"command"`
}

// CompressConfig configures gzip output.
type CompressConfig struct {
	// Level is the gzip level, -1 (default speed) .. 9. A nil value means "use
	// the built in default" (6), which is different from an explicit 0 (store
	// only), so the field is a pointer.
	Level *int `json:"level"`
}

// GzipLevel resolves the effective gzip level.
func (c *CompressConfig) GzipLevel() int {
	if c == nil || c.Level == nil {
		return 6
	}
	return *c.Level
}

// ShutdownConfig holds the two shutdown deadlines.
type ShutdownConfig struct {
	// Grace is how long the pipeline may spend draining after the source stops.
	Grace Duration `json:"grace"`
	// Hard is how long it may take to abort after the forced cancel.
	Hard Duration `json:"hard"`
}

// RecoveryConfig selects what happens to jobs found in the journal at startup.
type RecoveryConfig struct {
	// Mode is "requeue" (default: replay the whole chain, which is idempotent)
	// or "resume" (publish a finished artefact first, then requeue the rest).
	Mode string `json:"mode"`
}

// Config is the whole pipeline configuration.
type Config struct {
	InboxDir         string         `json:"inbox_dir"`
	OutDir           string         `json:"out_dir"`
	WorkDir          string         `json:"work_dir"`
	FailedDir        string         `json:"failed_dir"`
	MaxFileSize      int64          `json:"max_file_size"`
	MaxInflightBytes int64          `json:"max_inflight_bytes"`
	PollInterval     Duration       `json:"poll_interval"`
	StableAge        Duration       `json:"stable_age"`
	QueueSize        int            `json:"queue_size"`
	MetricsAddr      string         `json:"metrics_addr"`
	LogLevel         string         `json:"log_level"`
	LogFormat        string         `json:"log_format"`
	Stages           []StageConfig  `json:"stages"`
	Convert          ConvertConfig  `json:"convert"`
	Compress         CompressConfig `json:"compress"`
	Shutdown         ShutdownConfig `json:"shutdown"`
	Recovery         RecoveryConfig `json:"recovery"`
}

// JournalDir is where the per job journal documents live.
func (c *Config) JournalDir() string { return filepath.Join(c.WorkDir, "jobs") }

// TempRoot is the parent of the per job scratch directories.
func (c *Config) TempRoot() string { return filepath.Join(c.WorkDir, "tmp") }

// PublishTempDir is the staging area inside the output directory. It must live
// on the same file system as the final outputs for os.Rename to stay atomic.
func (c *Config) PublishTempDir() string { return filepath.Join(c.OutDir, ".tmp") }

// Stage returns the configuration of the named stage.
func (c *Config) Stage(name string) (StageConfig, bool) {
	for _, s := range c.Stages {
		if s.Name == name {
			return s, true
		}
	}
	return StageConfig{}, false
}

// StageNames lists the configured stage order.
func (c *Config) StageNames() []string {
	out := make([]string, len(c.Stages))
	for i, s := range c.Stages {
		out[i] = s.Name
	}
	return out
}

// Default returns a complete, valid configuration. It doubles as the fuzz seed
// and as the reference used by tests.
func Default() *Config {
	return &Config{
		InboxDir:         filepath.Join("data", "inbox"),
		OutDir:           filepath.Join("data", "out"),
		WorkDir:          filepath.Join("data", "work"),
		FailedDir:        filepath.Join("data", "failed"),
		MaxFileSize:      64 << 20,
		MaxInflightBytes: 128 << 20,
		PollInterval:     Duration(200 * time.Millisecond),
		StableAge:        Duration(300 * time.Millisecond),
		QueueSize:        16,
		MetricsAddr:      "127.0.0.1:9090",
		LogLevel:         "info",
		LogFormat:        "text",
		Stages: []StageConfig{
			{Name: "scan", Workers: 2, Timeout: Duration(10 * time.Second), QueueSize: 16,
				Backoff: BackoffConfig{Base: Duration(100 * time.Millisecond), Max: Duration(2 * time.Second), Factor: 2, Jitter: 0.2, MaxAttempts: 3}},
			{Name: "convert", Workers: 4, Timeout: Duration(30 * time.Second), QueueSize: 16,
				Backoff: BackoffConfig{Base: Duration(200 * time.Millisecond), Max: Duration(5 * time.Second), Factor: 2, Jitter: 0.2, MaxAttempts: 3}},
			{Name: "compress", Workers: 2, Timeout: Duration(30 * time.Second), QueueSize: 16,
				Backoff: BackoffConfig{Base: Duration(200 * time.Millisecond), Max: Duration(5 * time.Second), Factor: 2, Jitter: 0.2, MaxAttempts: 3}},
		},
		Compress: CompressConfig{Level: intPtr(6)},
		Shutdown: ShutdownConfig{Grace: Duration(15 * time.Second), Hard: Duration(5 * time.Second)},
		Recovery: RecoveryConfig{Mode: "requeue"},
	}
}

// Parse decodes, defaults and validates a configuration document.
func Parse(data []byte) (*Config, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: decode: %w", err)
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return nil, errors.New("config: trailing data after the configuration object")
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Load reads and parses a configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	return Parse(data)
}

func (c *Config) applyDefaults() {
	if c.MaxInflightBytes < 0 {
		c.MaxInflightBytes = 0
	}
	if c.QueueSize == 0 {
		c.QueueSize = 16
	}
	if c.PollInterval == 0 {
		c.PollInterval = Duration(200 * time.Millisecond)
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.LogFormat == "" {
		c.LogFormat = "text"
	}
	if c.Compress.Level == nil {
		c.Compress.Level = intPtr(6)
	}
	if c.Shutdown.Grace == 0 {
		c.Shutdown.Grace = Duration(15 * time.Second)
	}
	if c.Shutdown.Hard == 0 {
		c.Shutdown.Hard = Duration(5 * time.Second)
	}
	if c.Recovery.Mode == "" {
		c.Recovery.Mode = "requeue"
	}
	for i := range c.Stages {
		s := &c.Stages[i]
		if s.Workers == 0 {
			s.Workers = 1
		}
		if s.Timeout == 0 {
			s.Timeout = Duration(30 * time.Second)
		}
		if s.QueueSize == 0 {
			s.QueueSize = c.QueueSize
		}
		if s.Backoff.Base == 0 {
			s.Backoff.Base = Duration(100 * time.Millisecond)
		}
		if s.Backoff.Max == 0 {
			s.Backoff.Max = Duration(5 * time.Second)
		}
		if s.Backoff.Factor == 0 {
			s.Backoff.Factor = 2
		}
		if s.Backoff.MaxAttempts == 0 {
			s.Backoff.MaxAttempts = 3
		}
	}
}

// Validate returns every problem in one error, sorted by stage order.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	for _, f := range []struct {
		name string
		val  string
	}{
		{"inbox_dir", c.InboxDir},
		{"out_dir", c.OutDir},
		{"work_dir", c.WorkDir},
		{"failed_dir", c.FailedDir},
	} {
		if strings.TrimSpace(f.val) == "" {
			add("%s must not be empty", f.name)
		}
	}
	if c.MaxFileSize <= 0 || c.MaxFileSize > MaxFileSizeLimit {
		add("max_file_size must be in (0, %d], got %d", MaxFileSizeLimit, c.MaxFileSize)
	}
	if c.MaxInflightBytes < 0 {
		add("max_inflight_bytes must be >= 0")
	}
	if c.QueueSize < MinQueueSize || c.QueueSize > MaxQueueSize {
		add("queue_size must be in [%d, %d], got %d", MinQueueSize, MaxQueueSize, c.QueueSize)
	}
	if c.PollInterval.D() <= 0 {
		add("poll_interval must be > 0")
	}
	if c.StableAge.D() < 0 {
		add("stable_age must be >= 0")
	}
	if c.Shutdown.Grace.D() <= 0 {
		add("shutdown.grace must be > 0")
	}
	if c.Shutdown.Hard.D() <= 0 {
		add("shutdown.hard must be > 0")
	}
	if lvl := c.Compress.GzipLevel(); lvl < -1 || lvl > 9 {
		add("compress.level must be in [-1, 9], got %d", lvl)
	}
	switch strings.ToLower(c.LogFormat) {
	case "text", "json":
	default:
		add("log_format must be text or json, got %q", c.LogFormat)
	}
	switch c.Recovery.Mode {
	case "requeue", "resume":
	default:
		add("recovery.mode must be requeue or resume, got %q", c.Recovery.Mode)
	}
	if len(c.Stages) == 0 {
		add("stages must contain at least one stage")
	}

	seen := make(map[string]bool, len(c.Stages))
	for i, s := range c.Stages {
		if strings.TrimSpace(s.Name) == "" {
			add("stages[%d].name must not be empty", i)
			continue
		}
		if seen[s.Name] {
			add("duplicate stage %q", s.Name)
		}
		seen[s.Name] = true
		if s.Workers < 1 || s.Workers > MaxWorkersPerStage {
			add("stage %q: workers must be in [1, %d], got %d", s.Name, MaxWorkersPerStage, s.Workers)
		}
		if s.Timeout.D() <= 0 {
			add("stage %q: timeout must be > 0", s.Name)
		}
		if s.QueueSize < MinQueueSize || s.QueueSize > MaxQueueSize {
			add("stage %q: queue_size must be in [%d, %d], got %d", s.Name, MinQueueSize, MaxQueueSize, s.QueueSize)
		}
		if s.Backoff.Base.D() <= 0 {
			add("stage %q: backoff.base must be > 0", s.Name)
		}
		if s.Backoff.Max.D() < s.Backoff.Base.D() {
			add("stage %q: backoff.max must be >= backoff.base", s.Name)
		}
		if s.Backoff.Factor < 1 {
			add("stage %q: backoff.factor must be >= 1", s.Name)
		}
		if s.Backoff.Jitter < 0 || s.Backoff.Jitter > 0.5 {
			add("stage %q: backoff.jitter must be in [0, 0.5]", s.Name)
		}
		if s.Backoff.MaxAttempts < 1 || s.Backoff.MaxAttempts > 10 {
			add("stage %q: backoff.max_attempts must be in [1, 10], got %d", s.Name, s.Backoff.MaxAttempts)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("config: invalid configuration: %w", errors.Join(errs...))
}

// Describe renders a short human readable summary used in the startup log.
func (c *Config) Describe() string {
	parts := make([]string, 0, len(c.Stages))
	for _, s := range c.Stages {
		parts = append(parts, fmt.Sprintf("%s/%dw/%s", s.Name, s.Workers, s.Timeout.D().String()))
	}
	return strings.Join(parts, " -> ")
}

func intPtr(v int) *int { return &v }
