package stream

import "time"

// RoughLedgerCloseInterval is the ordinary Stellar ledger cadence on mainnet
// and testnet — roughly one close every five seconds. prism deliberately
// keeps the --since conversion offline and deterministic: no extra RPC call
// to measure the true rate, because the point of a relative window is to be
// approximate ("start about ten minutes back"), and determinism keeps the
// behaviour testable and documented. Streams that need an exact ledger use
// --from instead.
const RoughLedgerCloseInterval = 5 * time.Second

// StartLedgerForWindow converts a relative time window into an approximate
// start ledger, counting back from latest at RoughLedgerCloseInterval.
//
// The conversion rounds the window up to whole ledger intervals, so
// `--since 10m` never starts *after* ten minutes ago: a slightly early start
// is harmless for tailing, while a late one silently omits records inside
// the requested window. Windows longer than the chain clamp to ledger 1 and
// the server then reports its retention floor from there, which is exactly
// the behaviour streams that reach too far back want. A non-positive window
// degenerates to starting at latest, which is how the --since flag's caller
// treats an unconfigured reset.
func StartLedgerForWindow(latest uint32, window time.Duration) uint32 {
	if window <= 0 {
		return latest
	}

	roundUp := func(d time.Duration) time.Duration {
		if rem := d % RoughLedgerCloseInterval; rem != 0 {
			return d + RoughLedgerCloseInterval - rem
		}
		return d
	}

	ledgersBack := roundUp(window) / RoughLedgerCloseInterval
	// The window is positive here, so ledgersBack is a non-negative count of
	// intervals rather than a span of nanoseconds; only as a count can it be
	// compared against a ledger sequence.
	if int64(ledgersBack) >= int64(latest) {
		// The window reaches before the first ledger ever closed.
		return 1
	}
	return latest - uint32(ledgersBack) //nolint:gosec // bounded by the guard above
}
