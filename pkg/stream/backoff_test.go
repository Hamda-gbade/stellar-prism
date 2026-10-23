package stream

import (
	"context"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

// This file lives in the internal test package because it asserts on the
// poller's unexported tuning state, and the internal and external test
// packages cannot see each other's helpers — so the clock and sink doubles
// below mirror the ones tail_test.go defines for `package stream_test`.

// fakeClock replaces wall time with a recorded list of sleeps, so the follow
// loop's waits can be asserted exactly.
type fakeClock struct {
	now   time.Time
	slept []time.Duration

	// maxSleeps bounds how long a follow loop may run. Without it, a
	// regression that stops the tailer terminating turns a failing test into
	// a hanging one, which is far harder to diagnose in CI.
	maxSleeps int
	onBudget  func()
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1700000000, 0).UTC()}
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.slept = append(c.slept, d)
	c.now = c.now.Add(d)
	if c.maxSleeps > 0 && len(c.slept) >= c.maxSleeps && c.onBudget != nil {
		c.onBudget()
	}
	return ctx.Err()
}

// collector records everything emitted, which is what the delivery
// guarantee's invariants are checked against.
type collector struct {
	ids     []string
	records []Record
}

func (c *collector) Emit(r Record) error {
	c.ids = append(c.ids, r.ID)
	c.records = append(c.records, r)
	return nil
}

func (c *collector) Flush() error { return nil }

// TestAdaptivePollerBounds checks the tuning invariant from both sides: no
// sequence of observations can push the wait below Min or past Max.
func TestAdaptivePollerBounds(t *testing.T) {
	t.Parallel()

	p := NewAdaptivePoller(time.Second, 30*time.Second, 2)
	if got := p.current; got != 2*time.Second {
		t.Fatalf("starting wait = %s, want 2s", got)
	}

	p.Observe(0)
	for range 64 {
		if wait := p.Observe(0); wait > p.Max {
			t.Fatalf("idle tuning produced %s, past the %s cap", wait, p.Max)
		}
	}
	for range 64 {
		if wait := p.Observe(3); wait < p.Min {
			t.Fatalf("busy tuning produced %s, below the %s floor", wait, p.Min)
		}
	}
}

// TestAdaptivePollerDeterministicTuning pins the exact transition sequence:
// a baseline first wait, growth on idle observations, shrink on activity,
// and convergence to the floor.
func TestAdaptivePollerDeterministicTuning(t *testing.T) {
	t.Parallel()

	p := NewAdaptivePoller(time.Second, 30*time.Second, 2)
	cases := []struct {
		observed int
		want     time.Duration
	}{
		{observed: 0, want: 2 * time.Second}, // baseline priming
		{observed: 0, want: 4 * time.Second}, // idle: grow
		{observed: 0, want: 8 * time.Second},
		{observed: 0, want: 16 * time.Second},
		{observed: 0, want: 30 * time.Second}, // capped
		{observed: 0, want: 30 * time.Second},
		{observed: 2, want: 15 * time.Second}, // busy: shrink
		{observed: 2, want: 7500 * time.Millisecond},
		{observed: 2, want: 3750 * time.Millisecond},
		{observed: 2, want: 1875 * time.Millisecond},
		{observed: 2, want: 1 * time.Second}, // floored at Min
		{observed: 2, want: 1 * time.Second},
		{observed: 0, want: 2 * time.Second}, // grow again from the floor
	}
	for i, tc := range cases {
		if got := p.Observe(tc.observed); got != tc.want {
			t.Fatalf("Observe(%d) call %d = %s, want %s", tc.observed, i, got, tc.want)
		}
	}
}

// TestAdaptivePollerStartClamps checks the constructor against unusual
// inputs: nonsense durations and factors must yield a working poller, not a
// panic, and Min==Max must hold the line exactly.
func TestAdaptivePollerStartClamps(t *testing.T) {
	t.Parallel()

	fixed := NewAdaptivePoller(5*time.Second, 5*time.Second, 0)
	if got := fixed.Observe(1); got != 5*time.Second {
		t.Errorf("Min==Max poller first wait = %s, want 5s", got)
	}
	if got := fixed.Observe(0); got != 5*time.Second {
		t.Errorf("Min==Max poller second wait = %s, want 5s", got)
	}

	degenerate := NewAdaptivePoller(0, -1, 0.5)
	// Min falls back to the default floor, Max to Min, Grow to 2.
	if degenerate.Min != DefaultAdaptiveMinPoll || degenerate.Max != DefaultAdaptiveMinPoll {
		t.Errorf("degenerate poller bounds = %s..%s, want both %s",
			degenerate.Min, degenerate.Max, DefaultAdaptiveMinPoll)
	}
}

// TestAdaptivePollerJitterRange pins the jitter shape with an injected
// source: r=0 halves again, r=1 leaves the wait whole.
func TestAdaptivePollerJitterRange(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		r    float64
		want time.Duration
	}{
		{name: "lowest half", r: 0, want: 1 * time.Second},
		{name: "exact wait", r: 1, want: 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := NewAdaptivePoller(time.Second, 30*time.Second, 2)
			p.Jitter = true
			p.rand = func() float64 { return tc.r }
			if got := p.Observe(2); got != tc.want {
				t.Errorf("Observe(2) = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestTailerAdaptivePollIntervals wires the poller into a real tailer and
// asserts the sleeps the follow loop actually performs: growth on idle
// follow-up polls, capped, and none while draining backfill pages.
func TestTailerAdaptivePollIntervals(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(800, 30)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(30),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			// After the initial page, report an idle head.
			1: {Empty: true},
			2: {Empty: true},
			3: {Empty: true},
		}),
	)
	src, err := NewEventSource(srv, EventQuery{StartLedger: 800, PageLimit: 30})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	// Bound the follow loop: three sleeps is exactly where this test's
	// assertions end, and a regression that keeps sleeping hangs the run
	// instead of failing an assertion.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newFakeClock()
	clock.maxSleeps = 3
	clock.onBudget = cancel

	tl := NewTailer(src, &collector{}, Options{
		Follow:   true,
		Adaptive: true,
		// Deterministic tuning, starting at 2s.
		AdaptiveMin: 1 * time.Second,
		AdaptiveMax: 30 * time.Second,
		Clock:       clock,
	})
	runErr := tl.Run(ctx)
	if runErr != nil {
		t.Fatalf("Run() error = %v, want a clean stop on budget exhaustion", runErr)
	}

	// The first page leaves the tailer caught up: every subsequent wait is
	// an idle observation, so the waits grow 2s, 4s, 8s.
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}
	if len(clock.slept) != len(want) {
		t.Fatalf("adaptive tailer slept %d time(s): %v, want %v", len(clock.slept), clock.slept, want)
	}
	for i, w := range want {
		if clock.slept[i] != w {
			t.Errorf("sleep %d = %s, want %s", i, clock.slept[i], w)
		}
	}
}
