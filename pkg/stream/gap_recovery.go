package stream

import (
	"fmt"
	"strings"
)

// GapStrategy selects what a stream does when retention has pruned records.
type GapStrategy string

const (
	// GapFail stops the stream with a *GapError. This is the default and
	// preserves prism's promise never to pass over a gap silently.
	GapFail GapStrategy = "fail"
	// GapResume skips the pruned range, restarts at the oldest ledger the
	// server still retains, and keeps streaming. The missed range is reported
	// through Stats and, when configured, the OnGap callback.
	GapResume GapStrategy = "resume"
)

// ParseGapStrategy validates a user-supplied strategy name.
func ParseGapStrategy(value string) (GapStrategy, error) {
	switch GapStrategy(strings.ToLower(strings.TrimSpace(value))) {
	case "", GapFail:
		return GapFail, nil
	case GapResume:
		return GapResume, nil
	default:
		return "", fmt.Errorf(
			"unknown gap strategy %q; expected %q or %q", value, GapFail, GapResume)
	}
}

// Rewinder is implemented by a Source that can be repositioned after a
// retention gap. Without it, GapResume cannot recover safely and the stream
// fails as it would under GapFail.
type Rewinder interface {
	// Rewind makes the next Fetch start at ledger. The next call to Fetch
	// should use an empty cursor.
	Rewind(ledger uint32)
}

// GapEvent reports a detected retention gap and what the recovery policy
// decided to do about it.
type GapEvent struct {
	// Gap describes the records that cannot be recovered.
	Gap *GapError
	// Strategy is the policy that handled the gap.
	Strategy GapStrategy
	// Recovered is true when the stream was repositioned and will continue.
	// It is false when the run is stopping.
	Recovered bool
}

// GapRecovery applies a GapStrategy to detected retention gaps.
//
// A zero value behaves like GapFail. GapRecovery is not safe for concurrent
// use; a Tailer drives it from one goroutine.
type GapRecovery struct {
	// Strategy is the action to take. The empty value means GapFail.
	Strategy GapStrategy
	// MaxRecoveries bounds how many times the stream may skip ahead. Zero
	// means no bound.
	MaxRecoveries int
	// OnGap, when set, is called for every gap, including the one that stops
	// the stream, so an operator can see what happened.
	OnGap func(GapEvent)

	recovered int
}

// Recovered reports how many gaps have been recovered so far.
func (r *GapRecovery) Recovered() int { return r.recovered }

// Handle applies the configured strategy. It returns true when src has been
// repositioned and the caller should fetch again with an empty cursor.
func (r *GapRecovery) Handle(src Source, gap *GapError) (bool, error) {
	strategy, err := ParseGapStrategy(string(r.Strategy))
	if err != nil {
		return false, err
	}
	r.Strategy = strategy

	if strategy == GapFail {
		r.notify(GapEvent{Gap: gap, Strategy: strategy})
		return false, gap
	}

	if gap.Oldest == 0 {
		r.notify(GapEvent{Gap: gap, Strategy: strategy})
		return false, fmt.Errorf("cannot recover from a gap without a retention floor: %w", gap)
	}
	if r.MaxRecoveries > 0 && r.recovered >= r.MaxRecoveries {
		r.notify(GapEvent{Gap: gap, Strategy: strategy})
		return false, fmt.Errorf(
			"giving up after %d gap recoveries: %w", r.recovered, gap)
	}

	rewinder, ok := src.(Rewinder)
	if !ok {
		r.notify(GapEvent{Gap: gap, Strategy: strategy})
		return false, fmt.Errorf(
			"source %q cannot resume after a gap because it does not implement Rewinder: %w",
			src.Name(), gap)
	}

	rewinder.Rewind(gap.Oldest)
	r.recovered++
	r.notify(GapEvent{Gap: gap, Strategy: strategy, Recovered: true})
	return true, nil
}

func (r *GapRecovery) notify(event GapEvent) {
	if r.OnGap != nil {
		r.OnGap(event)
	}
}
