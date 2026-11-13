package job

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Clock is the time source used for all backoff sleeps and latency
// measurements. Tests inject FakeClock so retry tests are instant and
// deterministic instead of sleeping for real.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

// RealClock is the production Clock.
type RealClock struct{}

// Now implements Clock.
func (RealClock) Now() time.Time { return time.Now() }

// Sleep implements Clock and returns early when ctx is cancelled.
func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// FakeClock is a manually advanced Clock for tests.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

// NewFakeClock returns a clock frozen at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now implements Clock.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Sleep implements Clock by advancing the clock immediately.
func (c *FakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.sleeps = append(c.sleeps, d)
	c.mu.Unlock()
	return nil
}

// Advance moves the clock forward without recording a sleep.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Sleeps returns every duration passed to Sleep, in order.
func (c *FakeClock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.sleeps))
	copy(out, c.sleeps)
	return out
}

// Rand is the source of jitter randomness. Injecting it keeps backoff tests
// deterministic.
type Rand interface {
	Float64() float64
}

// LockedRand is a goroutine safe wrapper around math/rand.
type LockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

// NewRand returns a seeded, goroutine safe Rand.
func NewRand(seed int64) *LockedRand {
	return &LockedRand{r: rand.New(rand.NewSource(seed))}
}

// Float64 implements Rand.
func (l *LockedRand) Float64() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Float64()
}

// FixedRand always returns the same value, which is what tests use.
type FixedRand struct{ V float64 }

// Float64 implements Rand.
func (f FixedRand) Float64() float64 { return f.V }

const defaultFactor = 2.0

// Backoff describes the retry policy for one stage: exponential growth capped
// at Max, with symmetric jitter of +/- Jitter (0..0.5) to avoid synchronised
// retry storms.
type Backoff struct {
	Base        time.Duration
	Max         time.Duration
	Factor      float64
	Jitter      float64
	MaxAttempts int
}

// Normalize fills in zero values and clamps nonsense so a partial config still
// produces a safe policy.
func (b Backoff) Normalize() Backoff {
	if b.Base <= 0 {
		b.Base = 100 * time.Millisecond
	}
	if b.Factor < 1 {
		b.Factor = defaultFactor
	}
	if b.Max <= 0 {
		b.Max = 30 * time.Second
	}
	if b.Max < b.Base {
		b.Max = b.Base
	}
	if b.Jitter < 0 || b.Jitter > 0.5 || math.IsNaN(b.Jitter) {
		b.Jitter = 0
	}
	if b.MaxAttempts < 1 {
		b.MaxAttempts = 3
	}
	return b
}

// Delay returns the wait before retry number attempt (1 means the wait after
// the first failed attempt). rnd is in [0,1) and only matters when Jitter > 0.
func (b Backoff) Delay(attempt int, rnd float64) time.Duration {
	b = b.Normalize()
	if attempt < 1 {
		attempt = 1
	}
	d := float64(b.Base) * math.Pow(b.Factor, float64(attempt-1))
	if d > float64(b.Max) {
		d = float64(b.Max)
	}
	if b.Jitter > 0 {
		// rnd in [0,1) maps to the factor range [1-jitter, 1+jitter).
		d *= 1 + b.Jitter*(2*rnd-1)
	}
	if d < 0 {
		d = 0
	}
	if d > float64(b.Max) {
		d = float64(b.Max)
	}
	return time.Duration(d)
}
