package job

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// ErrInvalidID is returned when a job identifier could escape the journal
// directory or is otherwise unusable as a path element.
var ErrInvalidID = errors.New("job: invalid id")

// ValidateID enforces the identifier alphabet used for journal file names.
// It is the path traversal guard for everything that derives a path from a job.
func ValidateID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: empty", ErrInvalidID)
	}
	if len(id) > 64 {
		return fmt.Errorf("%w: too long", ErrInvalidID)
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return fmt.Errorf("%w: illegal character %q", ErrInvalidID, r)
		}
	}
	return nil
}

// Store is the durable journal: one JSON document per job under jobs/<id>.json.
//
// Writes go to a temporary file in the same directory, are fsynced, and are
// then renamed over the target. A reader therefore sees either the previous or
// the new document, never a half written one, even after a power loss.
type Store struct {
	dir    string
	tmpDir string
	log    *slog.Logger
	mu     sync.Mutex // serialises writers; reads rely on the atomic rename
}

// NewStore creates the journal directories below dir.
func NewStore(dir string, log *slog.Logger) (*Store, error) {
	if dir == "" {
		return nil, errors.New("job: empty journal directory")
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	tmp := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, fmt.Errorf("job: create journal dir: %w", err)
	}
	return &Store{dir: dir, tmpDir: tmp, log: log}, nil
}

// Dir returns the journal root.
func (s *Store) Dir() string { return s.dir }

// Path returns the journal path for one job.
func (s *Store) Path(id string) string {
	return filepath.Join(s.dir, id+".json")
}

// Save writes the job state atomically. The context is checked before the
// write starts; once started the rename always completes, because aborting
// half way is worse than finishing a single small write during shutdown.
func (s *Store) Save(ctx context.Context, j *Job) error {
	if j == nil {
		return errors.New("job: nil job")
	}
	if err := ValidateID(j.ID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return fmt.Errorf("job: marshal %s: %w", j.ID, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		copy(suffix[:], []byte("tmp0"))
	}
	tmp := filepath.Join(s.tmpDir, j.ID+"."+hex.EncodeToString(suffix[:])+".json")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("job: create temp journal: %w", err)
	}
	_, werr := f.Write(data)
	if werr == nil {
		werr = f.Sync()
	}
	cerr := f.Close()
	if werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("job: write journal %s: %w", j.ID, werr)
	}
	if err := os.Rename(tmp, s.Path(j.ID)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("job: publish journal %s: %w", j.ID, err)
	}
	syncDir(s.dir)
	return nil
}

// Load reads one job document.
func (s *Store) Load(id string) (*Job, error) {
	if err := ValidateID(id); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.Path(id))
	if err != nil {
		return nil, err
	}
	var j Job
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("job: decode journal %s: %w", id, err)
	}
	if j.ID != id {
		return nil, fmt.Errorf("job: journal %s contains id %q", id, j.ID)
	}
	if j.Attempts == nil {
		j.Attempts = make(map[Stage]int, 3)
	}
	return &j, nil
}

// List returns every readable journal document sorted by creation time. A
// corrupt document is quarantined as <id>.json.corrupt and skipped: keeping the
// rest of the journal usable matters more than failing the whole recovery.
func (s *Store) List(ctx context.Context) ([]*Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("job: read journal dir: %w", err)
	}
	out := make([]*Job, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		j, err := s.Load(id)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			s.log.Warn("quarantining unreadable journal entry", "file", e.Name(), "err", err)
			_ = os.Rename(s.Path(id), s.Path(id)+".corrupt")
			continue
		}
		out = append(out, j)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].CreatedAt.Before(out[b].CreatedAt) })
	return out, nil
}

// Delete removes one journal document.
func (s *Store) Delete(id string) error {
	if err := ValidateID(id); err != nil {
		return err
	}
	if err := os.Remove(s.Path(id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// syncDir flushes a directory entry so a rename survives a crash. Windows and
// some file systems refuse to open directories; that best effort failure is
// ignored on purpose.
func syncDir(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = f.Close()
}
