package stream

import (
	"context"
	"errors"
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
	// Duplicates counts records the server re-sent that were suppressed.
	// A non-zero value after a reconnect is expected and healthy.
	Duplicates int
	// Fetches counts successful upstream calls.
	Fetches int
	// Retries counts fetches that failed and were retried.
	Retries int
}

// Options configures a Tailer.
type Options struct {
	// Follow keeps the stream open, polling for new records. When false the
	// tailer drains what is already available and returns.
	Follow bool
	// Limit stops the stream after this many records. Zero means unlimited.
	Limit int
	// PollInterval is the wait between polls once caught up.
	PollInterval time.Duration
	// Backoff governs retry delays after a failed fetch.
	Backoff Backoff
	// MaxRetries bounds consecutive failed fetches before giving up. Zero
	// means retry indefinitely, which is usually what a long-running tail
	// wants.
	MaxRetries int
	// Clock is injectable for tests. Defaults to RealClock.
	Clock Clock
	// OnRetry, when set, is called before each backoff wait so the CLI can
	// tell the user what is happening rather than appearing to hang.
	OnRetry func(attempt int, delay time.Duration, err error)
	// State, when set, makes the stream position durable. It is saved after
	// every successfully emitted record and restored when Run starts, so a
	// restarted process resumes where the previous one stopped instead of
	// re-emitting from its original start. Nil (the default) keeps the
	// position in memory only. *StateFile is the file-backed implementation.
	State StateStore
}

// StateStore is the seam durable cursors plug into: it holds the stream's
// resumable position, and the tailer reads it once at startup and writes it
// after each successful emit. *StateFile is the file-backed implementation;
// tests substitute fakes to exercise failure paths.
//
// Implementations must treat Save as synchronous and durable when it
// returns nil: the delivery guarantee across restarts rests on the position
// being on disk before the tailer advances past it.
type StateStore interface {
	// Load returns the persisted position, or the zero State when nothing
	// has been persisted yet.
	Load() (State, error)
	// Save replaces the persisted position.
	Save(st State) error
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
//     re-emit on restart. When Options.State is set, the window narrows to
//     the interrupted record: the position is persisted after every emit and
//     the restored high-water mark suppresses replays. True exactly-once
//     would need a transaction spanning the sink and the cursor store,
//     which a CLI writing to stdout cannot have; consumers that care should
//     still key off Record.ID.
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

	stats Stats
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
	return &Tailer{src: src, sink: sink, opts: opts}
}

// Stats returns a snapshot of what the run has done so far.
func (t *Tailer) Stats() Stats { return t.stats }

// Cursor returns the position the stream has reached, suitable for resuming
// a later run.
func (t *Tailer) Cursor() string { return t.cursor }

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

	// Resuming happens before the first fetch, so a persisted position wins
	// over whatever start the Source was configured with — that is the point
	// of a durable cursor — and a state file that cannot be applied fails
	// the run before any request is made.
	if err := t.restoreState(); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return t.finish(nil)
		}

		batch, err := t.fetchWithRetry(ctx)
		if err != nil {
			if isCancellation(err) {
				return t.finish(nil)
			}
			return err
		}

		if err := t.checkGap(batch); err != nil {
			return err
		}

		done, err := t.emit(batch)
		if err != nil {
			return err
		}
		if done {
			return t.finish(nil)
		}

		// Advance only after a fully successful emit. If emitting failed we
		// return above with the cursor untouched, so a caller retrying the
		// whole run resumes from the same place rather than past the failure.
		if batch.NextCursor != "" {
			t.cursor = batch.NextCursor
			// Re-persist with the server's own token now that the batch has
			// fully drained. It is equivalent to the last record's ID but in
			// the exact form the server issued it, which is the most
			// conservative position to resume from.
			if ledger, ok := t.src.CursorLedger(t.cursor); ok {
				if err := t.persist(t.cursor, ledger); err != nil {
					return err
				}
			}
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

		if err := t.opts.Clock.Sleep(ctx, t.opts.PollInterval); err != nil {
			return t.finish(nil)
		}
	}
}

// finish flushes and returns err, converting a flush failure into the error
// when there is no more pressing one to report.
func (t *Tailer) finish(err error) error {
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

		if isCancellation(err) {
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
func (t *Tailer) checkGap(b Batch) error {
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

// emit writes a batch's records, suppressing any the server replayed, and
// reports whether the configured limit has been reached.
func (t *Tailer) emit(b Batch) (done bool, err error) {
	for _, r := range b.Records {
		// IDs are fixed-width and zero-padded, so this comparison is a
		// chronological one. Anything at or before the high-water mark is a
		// replay after a reconnect, or an out-of-order server response.
		if t.lastID != "" && r.ID <= t.lastID {
			t.stats.Duplicates++
			continue
		}

		if err := t.sink.Emit(r); err != nil {
			return false, fmt.Errorf("writing record %s: %w", r.ID, err)
		}
		t.lastID = r.ID
		t.stats.Emitted++

		// The position becomes durable only once the record is out: the
		// state file is written after the emit, never before, so it can
		// never run ahead of the stream. A crash in between re-emits this
		// one record on restart — at-least-once, as documented — and the
		// restored high-water mark suppresses anything already delivered.
		if err := t.persist(r.ID, r.Sequence); err != nil {
			return false, err
		}

		if t.opts.Limit > 0 && t.stats.Emitted >= t.opts.Limit {
			return true, nil
		}
	}
	return false, nil
}

// restoreState resumes the in-memory position from the persisted one.
//
// The saved position wins over a freshly configured start, because that is
// what "durable" means. A state file written by a different stream is
// refused rather than misapplied, and a token the Source cannot interpret is
// reported before any request is made rather than surfacing as a confusing
// server rejection mid-run.
func (t *Tailer) restoreState() error {
	if t.opts.State == nil {
		return nil
	}
	st, err := t.opts.State.Load()
	if err != nil {
		return fmt.Errorf("loading stream state: %w", err)
	}
	if st.Source == "" {
		// Nothing persisted yet: start where the Source was configured.
		return nil
	}
	if st.Source != t.src.Name() {
		return fmt.Errorf(
			"state holds a position for the %q stream, but this run follows %q; use a separate state file per stream",
			st.Source, t.src.Name())
	}
	if st.PagingToken != "" {
		if _, ok := t.src.CursorLedger(st.PagingToken); !ok {
			return fmt.Errorf(
				"state file cursor %q is not a valid %s cursor", st.PagingToken, t.src.Name())
		}
		t.cursor = st.PagingToken
	}
	t.lastID = st.LastID
	return nil
}

// persist saves the current position, recording pagingToken as the resume
// point and the tailer's high-water mark. It is a no-op when no StateStore
// is configured, which keeps the non-durable path free of extra work.
func (t *Tailer) persist(pagingToken string, ledger uint32) error {
	if t.opts.State == nil {
		return nil
	}
	st := State{
		Source:      t.src.Name(),
		Ledger:      ledger,
		PagingToken: pagingToken,
		LastID:      t.lastID,
		UpdatedAt:   t.opts.Clock.Now(),
	}
	if err := t.opts.State.Save(st); err != nil {
		return fmt.Errorf("persisting stream position: %w", err)
	}
	return nil
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
