package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// DefaultPollInterval is roughly one Stellar ledger close. Polling faster
// than ledgers close only burns request quota.
const DefaultPollInterval = 5 * time.Second

// Stats records what a tail run did, for diagnostics and for tests to assert
// on.
type Stats struct {
	// Emitted counts records handed to the Sink.
	Emitted int
	// Filtered counts records rejected by a configured client-side filter.
	Filtered int
	// Duplicates counts records the server re-sent that were suppressed.
	// A non-zero value after a reconnect is expected and healthy.
	Duplicates int
	// Fetches counts successful upstream calls.
	Fetches int
	// Retries counts fetches that failed and were retried.
	Retries int
	// Gaps counts retention gaps the stream detected.
	Gaps int
	// Recovered counts gaps the stream skipped past under GapResume.
	Recovered int
}

// Page describes the progress of one page that was emitted in full.
//
// It carries the position a later run could resume from, which is what makes
// a durable cursor possible without the tailer knowing anything about
// storage.
type Page struct {
	// Source names the stream that produced the page.
	Source string
	// Cursor is the position to resume from after this page. It is empty
	// when the source offers no further position.
	Cursor string
	// Ledger is the ledger the cursor refers to, or zero when the source
	// cannot say.
	Ledger uint32
	// Records counts the records emitted in the page.
	Records int
}

// Options configures a Tailer.
type Options struct {
	// Follow keeps the stream open, polling for new records. When false the
	// tailer drains what is already available and returns.
	Follow bool
	// Limit stops the stream after this many records. Zero means unlimited.
	Limit int
	// Filter runs before the sink. Rejected records still advance the source
	// cursor, but do not count toward Limit or Emitted.
	Filter RecordFilter
	// PollInterval is the wait between polls once caught up.
	PollInterval time.Duration
	// Backoff governs retry delays after a failed fetch.
	Backoff Backoff
	// MaxRetries bounds consecutive failed fetches before giving up. Zero
	// means retry indefinitely, which is usually what a long-running tail
	// wants.
	MaxRetries int
	// Adaptive switches the follow loop's poll wait to the adaptive poller in
	// backoff.go, which tunes the interval from how many new ledgers close
	// during each wait instead of holding PollInterval fixed. With it on,
	// PollInterval is not consulted; the bounds below are.
	Adaptive bool
	// AdaptiveMin and AdaptiveMax bound the tuned interval. Zero values fall
	// back to DefaultAdaptiveMinPoll and DefaultAdaptiveMaxPoll, and a Max
	// below Min is pulled back up, so out-of-range configuration degrades to
	// clamping rather than failing the stream mid-flight.
	AdaptiveMin time.Duration
	// AdaptiveMax is the longest interval the poller may take.
	AdaptiveMax time.Duration
	// Clock is injectable for tests. Defaults to RealClock.
	Clock Clock
	// StartCursor resumes the stream from a position an earlier run
	// recorded. The empty value means "start where the Source was
	// configured to start".
	//
	// A resumed stream is gap-checked like any other, because the cursor is
	// a real position and the server may have pruned past it.
	StartCursor string
	// OnRetry, when set, is called before each backoff wait so the CLI can
	// tell the user what is happening rather than appearing to hang.
	OnRetry func(attempt int, delay time.Duration, err error)
	// GapStrategy selects what to do when the server has pruned records.
	// The empty value means GapFail.
	GapStrategy GapStrategy
	// MaxGapRecoveries bounds how many times GapResume may skip ahead. Zero
	// means no bound.
	MaxGapRecoveries int
	// OnGap, when set, is called for every detected gap.
	OnGap func(GapEvent)
	// Process transforms a complete ledger's records before they are emitted.
	// It is used by derived streams such as footprint conflict detection.
	Process func([]Record) ([]Record, error)
	// OnPage, when set, is called after every page that was emitted in
	// full, with the position to resume from.
	//
	// A page that was only partly emitted — because the limit was reached
	// or the sink failed — does not produce a callback, so a recorded
	// position always refers to records that were actually delivered.
	OnPage func(Page)
}

// Tailer drives a Source, emitting records to a Sink.
//
// # Delivery guarantee
//
// A Tailer provides at-least-once delivery with in-process de-duplication,
// and explicit gap detection. Concretely:
//
//   - No duplicates within a run. Cursor paging is exact, and every record is
//     checked against the highest ID already emitted, so a page replayed after
//     a reconnect is suppressed.
//
//   - No silent gaps, ever. A failed fetch is retried with the cursor
//     unchanged, so nothing is skipped by a dropped connection. The one way
//     records can genuinely be lost is falling behind the server's retention
//     window, and that is detected against the server-reported oldest ledger
//     and returned as a *GapError rather than passed over.
//
//   - Not exactly-once across restarts. The tailer cannot atomically emit a
//     record and commit its cursor, so a process killed between the two will
//     re-emit on restart. Options.StartCursor and Options.OnPage checkpoint
//     fully delivered pages; pages still held by the reorder buffer may be
//     replayed. Consumers that care should key off Record.ID.
type Tailer struct {
	src  Source
	sink Sink
	opts Options

	// cursor is the position to resume from. Empty means "start where the
	// Source was configured to start".
	cursor string
	// lastID is the highest record ID emitted so far. Because IDs are
	// fixed-width and zero-padded, a plain string comparison orders them
	// correctly, which is what makes de-duplication a single comparison.
	lastID string

	stats    Stats
	recovery GapRecovery
	reorder  *ReorderBuffer
	// Checkpoints wait until all records through their page have left the
	// reorder buffer and reached the sink. The fetch cursor can run ahead.
	pages           []pendingPage
	deliveredCursor string

	// adaptive is non-nil only when Options.Adaptive is set, and then drives
	// the wait between polls instead of the fixed interval.
	adaptive *AdaptivePoller
	// latestSeen tracks the head the previous poll reported, so the adaptive
	// poller can count how many ledgers closed during each wait.
	latestSeen uint32
}

type pendingPage struct {
	page   Page
	lastID string
	// records identifies the unique records first seen on this page, so
	// client-side rejections can be removed from its delivered count.
	records map[string]struct{}
}

// NewTailer returns a Tailer wired to src and sink.
func NewTailer(src Source, sink Sink, opts Options) *Tailer {
	if opts.PollInterval <= 0 {
		opts.PollInterval = DefaultPollInterval
	}
	if opts.Clock == nil {
		opts.Clock = RealClock{}
	}
	if opts.Backoff.Initial == 0 && opts.Backoff.Max == 0 {
		opts.Backoff = DefaultBackoff()
	}
	strategy, err := ParseGapStrategy(string(opts.GapStrategy))
	if err != nil {
		strategy = GapFail
	}
	t := &Tailer{
		src:             src,
		sink:            sink,
		opts:            opts,
		cursor:          opts.StartCursor,
		deliveredCursor: opts.StartCursor,
		recovery: GapRecovery{
			Strategy:      strategy,
			MaxRecoveries: opts.MaxGapRecoveries,
			OnGap:         opts.OnGap,
		},
		reorder: NewReorderBuffer(),
	}
	if opts.Adaptive {
		t.adaptive = NewAdaptivePoller(opts.AdaptiveMin, opts.AdaptiveMax, 2)
	}
	return t
}

// wait returns how long to hold before the next poll, given what the last
// batch reported. The fixed interval is the default; the adaptive poller
// replaces it by observing how many new ledgers closed during the previous
// wait.
func (t *Tailer) wait(b Batch) time.Duration {
	if t.adaptive == nil {
		return t.opts.PollInterval
	}
	return t.adaptive.Observe(t.newLedgers(b))
}

// newLedgers reports how many ledgers closed since the previous poll, or
// zero when the head did not advance or was reported out of order by a
// lagging backend, which is both indistinguishable from idle for tuning
// purposes and safe to treat as such.
func (t *Tailer) newLedgers(b Batch) int {
	if b.Latest == 0 || t.latestSeen == 0 || b.Latest <= t.latestSeen {
		t.latestSeen = b.Latest
		return 0
	}
	n := int64(b.Latest) - int64(t.latestSeen)
	t.latestSeen = b.Latest
	// The delta is only consumed against thresholds, so saturate at int32 to
	// stay portable without losing the decision.
	const int32Max = int64(1)<<31 - 1
	if n > int32Max {
		n = int32Max
	}
	return int(n)
}

// Stats returns a snapshot of what the run has done so far.
func (t *Tailer) Stats() Stats { return t.stats }

// Cursor returns the position the stream has reached, suitable for resuming
// a later run.
func (t *Tailer) Cursor() string { return t.deliveredCursor }

// Run drives the stream until it completes, hits an error, or ctx is
// cancelled.
//
// Cancellation is a clean stop, not a failure: Run flushes the sink and
// returns nil so that Ctrl-C on a tail does not look like a crash.
func (t *Tailer) Run(ctx context.Context) error {
	defer func() {
		// Best-effort flush; a flush error on an already-failing path should
		// not mask the original cause.
		_ = t.sink.Flush()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return t.finish(nil)
		}

		batch, err := t.fetchWithRetry(ctx)
		if err != nil {
			if IsCancellation(err) {
				return t.finish(nil)
			}
			return err
		}

		if gapErr := t.checkGap(batch); gapErr != nil {
			t.stats.Gaps++
			recovered, recoverErr := t.recovery.Handle(t.src, gapErr)
			if recoverErr != nil {
				return recoverErr
			}
			if recovered {
				t.stats.Recovered = t.recovery.Recovered()
				t.cursor = ""
				continue
			}
		}

		t.queuePage(batch)
		done, _, err := t.emit(batch)
		if err != nil {
			return err
		}
		// A bounded follow has all it needs once the available pages are
		// drained and the buffered suffix satisfies the limit. Do not wait
		// for another ledger just to finish an explicitly bounded request.
		if !done && !batch.Full && t.opts.Limit > 0 && t.stats.Emitted+len(t.reorder.pending) >= t.opts.Limit {
			done, _, err = t.emitReady(t.reorder.Flush())
			if err != nil {
				return err
			}
		}
		if done {
			return t.finish(nil)
		}

		t.reportReadyPages()

		// Advance only after a fully successful emit. If emitting failed we
		// return above with the cursor untouched, so a caller retrying the
		// whole run resumes from the same place rather than past the failure.
		if batch.NextCursor != "" {
			t.cursor = batch.NextCursor
		}

		// A full page means the server already has more for us; keep draining
		// before sleeping, so backfill runs at network speed rather than at
		// one page per poll interval.
		if batch.Full {
			continue
		}

		if !t.opts.Follow {
			return t.finish(nil)
		}

		if err := t.opts.Clock.Sleep(ctx, t.wait(batch)); err != nil {
			return t.finish(nil)
		}
	}
}

// finish flushes and returns err, converting a flush failure into the error
// when there is no more pressing one to report.
func (t *Tailer) finish(err error) error {
	if err == nil && (t.opts.Limit == 0 || t.stats.Emitted < t.opts.Limit) {
		if _, _, flushErr := t.emitReady(t.reorder.Flush()); flushErr != nil {
			return flushErr
		}
	}
	t.reportReadyPages()
	if flushErr := t.sink.Flush(); flushErr != nil && err == nil {
		return fmt.Errorf("flushing output: %w", flushErr)
	}
	return err
}

// fetchWithRetry calls the Source, retrying transient failures with backoff
// and always reusing the same cursor so that no record can be skipped.
func (t *Tailer) fetchWithRetry(ctx context.Context) (Batch, error) {
	var attempt int
	for {
		batch, err := t.src.Fetch(ctx, t.cursor)
		if err == nil {
			t.stats.Fetches++
			return batch, nil
		}

		if IsCancellation(err) {
			return Batch{}, err
		}
		if !rpc.IsRetryable(err) {
			return Batch{}, fmt.Errorf("%s stream: %w", t.src.Name(), err)
		}

		attempt++
		t.stats.Retries++
		if t.opts.MaxRetries > 0 && attempt > t.opts.MaxRetries {
			return Batch{}, fmt.Errorf(
				"%s stream: giving up after %d consecutive failures: %w",
				t.src.Name(), attempt-1, err)
		}

		delay := t.opts.Backoff.Delay(attempt)
		if t.opts.OnRetry != nil {
			t.opts.OnRetry(attempt, delay, err)
		}
		if sleepErr := t.opts.Clock.Sleep(ctx, delay); sleepErr != nil {
			return Batch{}, sleepErr
		}
	}
}

// checkGap reports whether the stream has fallen behind the server's
// retention window, which is the only circumstance under which records are
// irrecoverably lost.
//
// It only applies once a cursor exists: the first fetch has not resumed from
// anywhere, and an out-of-range start ledger is rejected by the server with a
// clearer message than this check could produce.
func (t *Tailer) checkGap(b Batch) *GapError {
	if t.cursor == "" || b.Oldest == 0 {
		return nil
	}
	at, ok := t.src.CursorLedger(t.cursor)
	if !ok {
		return nil
	}
	if at < b.Oldest {
		return &GapError{Source: t.src.Name(), From: at, Oldest: b.Oldest}
	}
	return nil
}

// reportsPage reports whether a page moved the stream forward.
//
// An empty page that leaves the cursor where it was says nothing new, and
// reporting it would fill a cursor journal with duplicates of the position
// already recorded.
func (t *Tailer) reportsPage(b Batch) bool {
	return len(b.Records) > 0 || (b.NextCursor != "" && b.NextCursor != t.cursor)
}

// queuePage remembers a fetched position without publishing it before the
// reorder buffer has delivered that page, including earlier pending pages.
func (t *Tailer) queuePage(b Batch) {
	if !t.reportsPage(b) {
		return
	}
	p := pendingPage{
		page:    Page{Source: t.src.Name(), Cursor: b.NextCursor},
		lastID:  t.lastID,
		records: make(map[string]struct{}),
	}
	if len(t.pages) > 0 {
		p.lastID = t.pages[len(t.pages)-1].lastID
	}
	seen := make(map[string]bool)
	for _, r := range b.Records {
		if r.ID > p.lastID {
			p.lastID = r.ID
		}
		_, buffered := t.reorder.seen[r.ID]
		if r.ID > t.lastID && !buffered && !seen[r.ID] {
			p.page.Records++
			p.records[r.ID] = struct{}{}
		}
		seen[r.ID] = true
	}
	if ledger, ok := t.src.CursorLedger(b.NextCursor); ok {
		p.page.Ledger = ledger
	}
	t.pages = append(t.pages, p)
}

func (t *Tailer) reportReadyPages() {
	for len(t.pages) > 0 && t.pages[0].lastID <= t.lastID {
		p := t.pages[0].page
		t.pages[0] = pendingPage{}
		t.pages = t.pages[1:]
		if p.Cursor != "" {
			t.deliveredCursor = p.Cursor
		}
		if t.opts.OnPage != nil {
			t.opts.OnPage(p)
		}
	}
}

// emit writes a batch's records, suppressing any the server replayed, and
// reports whether the configured limit has been reached and records written.
func (t *Tailer) emit(b Batch) (done bool, emitted int, err error) {
	return t.emitReady(t.reorder.Add(b.Records))
}

func (t *Tailer) emitReady(records []Record) (done bool, emitted int, err error) {
	if len(records) == 0 {
		return false, 0, nil
	}
	if t.opts.Process != nil {
		records, err = t.opts.Process(records)
		if err != nil {
			return false, 0, err
		}
	}
	for _, r := range records {
		// IDs are fixed-width and zero-padded, so this comparison is a
		// chronological one. Anything at or before the high-water mark is a
		// replay after a reconnect, or an out-of-order server response.
		if t.lastID != "" && r.ID <= t.lastID {
			t.stats.Duplicates++
			continue
		}
		if t.opts.Filter != nil {
			matched, err := t.opts.Filter.Match(r)
			if err != nil {
				return false, emitted, fmt.Errorf("filtering record %s: %w", r.ID, err)
			}
			if !matched {
				t.lastID = r.ID
				t.stats.Filtered++
				for i := range t.pages {
					if _, ok := t.pages[i].records[r.ID]; ok {
						t.pages[i].page.Records--
						delete(t.pages[i].records, r.ID)
						break
					}
				}
				t.reportReadyPages()
				continue
			}
		}

		if err := t.sink.Emit(r); err != nil {
			return false, emitted, fmt.Errorf("writing record %s: %w", r.ID, err)
		}
		t.lastID = r.ID
		t.stats.Emitted++
		emitted++
		t.reportReadyPages()

		if t.opts.Limit > 0 && t.stats.Emitted >= t.opts.Limit {
			return true, emitted, nil
		}
	}
	return false, emitted, nil
}
