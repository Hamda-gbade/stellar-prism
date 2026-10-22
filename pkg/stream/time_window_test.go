package stream_test

import (
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestStartLedgerForWindow pins the arithmetic contract: windows round up to
// whole interval units, so a --since window never starts late, long windows
// clamp to ledger 1, and degenerate windows start at latest.
func TestStartLedgerForWindow(t *testing.T) {
	t.Parallel()

	const latest = uint32(1_000_000)

	tests := []struct {
		name   string
		window time.Duration
		// latest overrides the head the window counts back from; zero means
		// "use the table's default".
		latest uint32
		want   uint32
	}{
		{name: "one interval", window: 5 * time.Second, want: latest - 1},
		{name: "sub-interval rounds up", window: 1 * time.Second, want: latest - 1},
		{name: "exactly two intervals", window: 10 * time.Second, want: latest - 2},
		{name: "ten minutes", window: 10 * time.Minute, want: latest - 120},
		{name: "two hours", window: 2 * time.Hour, want: latest - 1440},
		{name: "just under an interval rounds up", window: 4*time.Second + 999*time.Millisecond, want: latest - 1},
		{name: "zero window starts at latest", window: 0, want: latest},
		{name: "negative window starts at latest", window: -time.Minute, want: latest},
		{name: "longer than the chain clamps", window: 300 * 24 * time.Hour, want: 1},
		{name: "latest of 1 clamps", window: time.Minute, latest: 1, want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			at := latest
			if tc.latest != 0 {
				at = tc.latest
			}
			if got := stream.StartLedgerForWindow(at, tc.window); got != tc.want {
				t.Errorf("StartLedgerForWindow(%d, %s) = %d, want %d", at, tc.window, got, tc.want)
			}
		})
	}
}

// TestStartLedgerForWindowAlwaysStartsNotLate checks the conversion rounds
// the window up for every duration in the first two ledger intervals rather
// than only the table's few hand-picked points.
func TestStartLedgerForWindowAlwaysStartsNotLate(t *testing.T) {
	t.Parallel()

	const latest = uint32(1000)
	for ms := time.Duration(1); ms <= 10*1000; ms += 100 {
		got := stream.StartLedgerForWindow(latest, ms*time.Millisecond)
		want := latest - uint32((ms+stream.RoughLedgerCloseInterval-1)/stream.RoughLedgerCloseInterval) //nolint:gosec // bounded by latest
		if want < 1 {
			want = 1
		}
		if got > want {
			t.Fatalf("window %s resolved to ledger %d, later than %d", ms*time.Millisecond, got, want)
		}
	}
}
