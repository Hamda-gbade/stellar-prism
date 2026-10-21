package stream

import (
	"math"
	"math/rand/v2"
	"time"
)

// DefaultAdaptiveMinPoll and DefaultAdaptiveMaxPoll bound the adaptive poll
// interval: floor when the ledger closes are arriving almost on top of each
// poll, ceiling when the network looks idle. The midpoint behavior is derived
// from observations, not configured — see AdaptivePoller.
const (
	DefaultAdaptiveMinPoll = 1 * time.Second
	DefaultAdaptiveMaxPoll = 30 * time.Second
)

// AdaptivePoller tunes the poll interval from how busy the network looks,
// so a long-running tail spends its request budget where the data actually
// is instead of polling at a rate some operator guessed once.
//
// The signal is deliberately simple: how many new ledgers closed during the
// previous wait.
//
//   - At least one new ledger means the wait matched or beat the ledger
//     cadence — shorten the next wait, so a busy stretch is followed closely.
//   - Zero new ledgers means most of the wait was dead time — lengthen it.
//
// With no cadence parameter at all that rule converges on oscillating around
// the real close rate: a wait just under the cadence sees zero and grows, a
// wait past it sees one and shrinks. The poller therefore tracks changes in
// the network it is never told about, which is what makes it adaptive rather
// than merely jittered, and the invariant remains: never faster than Min,
// never slower than Max.
//
// Jitter spreads each returned wait over its lower half, the same full-jitter
// shape Backoff applies to retries, because several prism instances watching
// one network poll in phase and produce waves that hit the RPC together.
//
// The poller is not safe for concurrent use: it lives entirely inside one
// tailer goroutine. State is one number, so tests construct one inline with
// obvious bounds and run it deterministically.
type AdaptivePoller struct {
	// Min is the shortest wait returned, for chains closing in bursts.
	Min time.Duration
	// Max is the longest wait, for chains that look idle.
	Max time.Duration
	// Grow is the factor a zero-ledger observation multiplies the current
	// wait by — and its inverse is the shrink when ledgers appear. It must
	// be at least one to make tuning monotone toward the bounds.
	Grow float64
	// Jitter spreads returned waits across [current/2, current] when set.
	Jitter bool

	// started flips false on a baseline (first) Observe, so the first wait
	// happens at the configured start rather than being retuned from zero
	// observations that never happened.
	started bool
	// current is the tuned wait, without jitter, always kept in [Min, Max].
	current time.Duration
	// rand is injectable so tests can pin the jittered output. Zero falls
	// back to the package rand source.
	rand func() float64
}

// NewAdaptivePoller returns a poller with current starting at 2*Min, clamped
// into [Min, Max], so the first waits err on the side of responsiveness and
// grow outward if the network is calmer than that. Durations at or below
// zero, or Grow under one, are pulled back to their defaults: a caller
// passing nonsense gets a working poller, not a panic — the bounds exist to
// protect the request budget, and clamping cannot violate them.
func NewAdaptivePoller(minPoll, maxPoll time.Duration, grow float64) *AdaptivePoller {
	if minPoll <= 0 {
		minPoll = DefaultAdaptiveMinPoll
	}
	if maxPoll < minPoll {
		maxPoll = minPoll
	}
	if grow < 1 {
		grow = 2
	}
	start := 2 * minPoll
	if start > maxPoll {
		start = maxPoll
	}
	return &AdaptivePoller{Min: minPoll, Max: maxPoll, Grow: grow, current: start}
}

// Observe reports that newLedgers closed during the last wait and returns
// the next wait. The first call only primes the poller and returns its
// starting interval, because there is no earlier observation to tune from.
func (a *AdaptivePoller) Observe(newLedgers int) time.Duration {
	if !a.started {
		a.started = true
		return a.jitter(a.current)
	}

	g := a.Grow
	if g < 1 {
		g = 2
	}
	if newLedgers <= 0 {
		// Dead time: stretch the wait.
		a.current = clampDuration(float64(a.current)*g, a.Min, a.Max)
	} else {
		// New ledgers arrived within the last wait: poll sooner.
		a.current = clampDuration(float64(a.current)/g, a.Min, a.Max)
	}
	// The wait returned is always the freshly tuned one, so idle growth and
	// busy shrinkage are both visible on the poll they caused rather than
	// one poll later.
	return a.jitter(a.current)
}

// jitter applies the same full-jitter shape Backoff uses, over the lower
// half of the wait, to a copy — the tuned current is kept clean so repeated
// observations accumulate coherently instead of through random noise.
func (a *AdaptivePoller) jitter(d time.Duration) time.Duration {
	if !a.Jitter {
		return d
	}
	r := a.rand
	if r == nil {
		r = rand.Float64
	}
	half := float64(d) / 2
	return time.Duration(half + half*r())
}

// clampDuration pulls v into [lo, hi] with float rounding, the only place
// float wait arithmetic is allowed to lose precision before it re-enters a
// Duration.
func clampDuration(v float64, lo, hi time.Duration) time.Duration {
	return time.Duration(math.Min(
		math.Max(v, float64(lo)),
		float64(hi),
	))
}
