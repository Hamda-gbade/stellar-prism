package stream_test

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// memoryStore is an in-memory MetadataStore, used to observe what the pruner
// asks for without touching a file.
type memoryStore struct {
	mu       sync.Mutex
	entries  []stream.CursorMetadata
	policies []stream.PrunePolicy
	prunes   int
	err      error
}

func (s *memoryStore) Append(_ context.Context, meta stream.CursorMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.entries = append(s.entries, meta)
	return nil
}

func (s *memoryStore) Latest(_ context.Context, key string) (stream.CursorMetadata, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return stream.CursorMetadata{}, false, s.err
	}
	for i := len(s.entries) - 1; i >= 0; i-- {
		if s.entries[i].Key == key {
			return s.entries[i], true, nil
		}
	}
	return stream.CursorMetadata{}, false, nil
}

func (s *memoryStore) Prune(_ context.Context, policy stream.PrunePolicy) (stream.PruneStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prunes++
	s.policies = append(s.policies, policy)
	if s.err != nil {
		return stream.PruneStats{}, s.err
	}

	counts := map[string]int{}
	for _, entry := range s.entries {
		if !policy.Cutoff.IsZero() && entry.RecordedAt.Before(policy.Cutoff) {
			continue
		}
		counts[entry.Key]++
	}

	seen := map[string]int{}
	var kept []stream.CursorMetadata
	for _, entry := range s.entries {
		if !policy.Cutoff.IsZero() && entry.RecordedAt.Before(policy.Cutoff) {
			continue
		}
		index := seen[entry.Key]
		seen[entry.Key] = index + 1
		if policy.Keep > 0 && index < counts[entry.Key]-policy.Keep {
			continue
		}
		kept = append(kept, entry)
	}
	stats := stream.PruneStats{Kept: len(kept), Removed: len(s.entries) - len(kept)}
	s.entries = kept
	return stats, nil
}

func (s *memoryStore) pruneCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prunes
}

func (s *memoryStore) lastPolicy() stream.PrunePolicy {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.policies) == 0 {
		return stream.PrunePolicy{}
	}
	return s.policies[len(s.policies)-1]
}

func (s *memoryStore) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func TestNewPrunerRequiresAStore(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(nil, stream.PruneOptions{})
	if err == nil {
		t.Fatal("NewPruner() error = nil, want a missing-store failure")
	}
	if pruner != nil {
		t.Error("NewPruner() returned a pruner alongside the error")
	}
}

// TestNewPrunerTreatsNegativeOptionsAsDisabled checks that a negative limit
// is read as "disabled" rather than as a limit nothing can satisfy.
//
// The CLI rejects a negative flag outright, so this only matters to a library
// caller: a negative Keep would otherwise prune every entry, which is the
// opposite of what the caller asked for.
func TestNewPrunerTreatsNegativeOptionsAsDisabled(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		Interval: -time.Hour,
		Keep:     -1,
		MaxAge:   -time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}

	// A negative Keep must not prune anything: zero means "keep every
	// entry".
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}
	policy := store.lastPolicy()
	if policy.Keep != 0 {
		t.Errorf("policy.Keep = %d, want 0 (keep everything)", policy.Keep)
	}
	if !policy.Cutoff.IsZero() {
		t.Errorf("policy.Cutoff = %v, want the zero time (keep every age)", policy.Cutoff)
	}
}

// TestNewPrunerDefaultsToTheRealClock checks that a pruner built without a
// clock still knows what time it is.
func TestNewPrunerDefaultsToTheRealClock(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}
}

// TestPrunerStartAcceptsANilContext checks that a caller with no context
// still gets a working pruner.
func TestPrunerStartAcceptsANilContext(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	//nolint:staticcheck // SA1012: a nil context is exactly what is under test.
	if err := pruner.Start(nil); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := pruner.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestPrunerPrunesOnceWithTheConfiguredPolicy(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		Keep:   5,
		MaxAge: time.Hour,
		Clock:  fixedClock{now: time.Unix(1700000000, 0).UTC()},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}

	stats, err := pruner.PruneOnce(context.Background())
	if err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}
	if stats.Kept != 0 || stats.Removed != 0 {
		t.Errorf("PruneOnce() = %+v, want an empty journal to change nothing", stats)
	}

	policy := store.lastPolicy()
	if policy.Keep != 5 {
		t.Errorf("policy keep = %d, want 5", policy.Keep)
	}
	want := time.Unix(1700000000, 0).UTC().Add(-time.Hour)
	if !policy.Cutoff.Equal(want) {
		t.Errorf("policy cutoff = %v, want %v", policy.Cutoff, want)
	}
}

func TestPrunerRemovesExpiredEntries(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	store := &memoryStore{entries: []stream.CursorMetadata{
		{Key: "events", Cursor: "old", RecordedAt: now.Add(-2 * time.Hour)},
		{Key: "events", Cursor: "new", RecordedAt: now.Add(-time.Minute)},
	}}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		MaxAge: time.Hour,
		Clock:  fixedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}

	stats, err := pruner.PruneOnce(context.Background())
	if err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}
	if stats.Removed != 1 || stats.Kept != 1 {
		t.Errorf("PruneOnce() = %+v, want 1 removed and 1 kept", stats)
	}
	if store.entries[0].Cursor != "new" {
		t.Errorf("kept %q, want the newer entry", store.entries[0].Cursor)
	}
}

func TestPrunerKeepsTheNewestEntriesPerKey(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	for _, cursor := range []string{"a", "b", "c", "d"} {
		store.entries = append(store.entries, stream.CursorMetadata{Key: "events", Cursor: cursor})
	}
	store.entries = append(store.entries, stream.CursorMetadata{Key: "ledgers", Cursor: "other"})

	pruner, err := stream.NewPruner(store, stream.PruneOptions{Keep: 2})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}

	var events, ledgers int
	for _, entry := range store.entries {
		switch entry.Key {
		case "events":
			events++
		case "ledgers":
			ledgers++
		}
	}
	if events != 2 {
		t.Errorf("kept %d event entries, want 2", events)
	}
	if ledgers != 1 {
		t.Errorf("kept %d ledger entries, want 1", ledgers)
	}
	if store.entries[0].Cursor != "c" {
		t.Errorf("first kept entry = %q, want the newest two", store.entries[0].Cursor)
	}
}

func TestPrunerStartPrunesInTheBackground(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{Interval: time.Millisecond})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	waitFor(t, "the background loop to prune", func() bool { return store.pruneCount() >= 2 })
}

func TestPrunerWithAZeroIntervalPrunesOnce(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{Interval: 0})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	waitFor(t, "the single pass to run", func() bool { return store.pruneCount() >= 1 })

	// The loop must stop after that pass rather than spinning.
	time.Sleep(50 * time.Millisecond)
	if got := store.pruneCount(); got != 1 {
		t.Errorf("pruned %d times, want exactly one", got)
	}
}

func TestPrunerStartTwiceIsRejected(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	if err := pruner.Start(context.Background()); !errors.Is(err, stream.ErrPrunerStarted) {
		t.Errorf("Start() error = %v, want ErrPrunerStarted", err)
	}
}

func TestPrunerStartAfterCloseIsRejected(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := pruner.Start(context.Background()); !errors.Is(err, stream.ErrPrunerClosed) {
		t.Errorf("Start() error = %v, want ErrPrunerClosed", err)
	}
}

func TestPrunerCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := pruner.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
}

// TestPrunerConcurrentCloseWaitsForTheGoroutine checks that every Close
// waits for the goroutine, not only the first one.
//
// A second Close that returned while the loop was still running would report
// the pruner as stopped when it is not, and a caller that trusted that
// report could exit with the goroutine still alive.
func TestPrunerConcurrentCloseWaitsForTheGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	store := &blockingStore{started: make(chan struct{}, 1), release: make(chan struct{})}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the pruner never started a pass")
	}

	closed := make(chan error, 2)
	go func() { closed <- pruner.Close() }()
	// Give the first Close time to mark the pruner closed, so the second
	// Close takes the already-closed path.
	time.Sleep(50 * time.Millisecond)
	go func() { closed <- pruner.Close() }()

	// The pass is still running, so neither Close may return yet.
	select {
	case err := <-closed:
		close(store.release)
		t.Fatalf("Close() returned (%v) while the pruner was still running", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(store.release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-closed:
			if err != nil {
				t.Errorf("Close() error = %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close() did not return after the pruner finished")
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Errorf("goroutines = %d, want at most %d: the pruner leaked one", got, before)
	}
}

// blockingStore holds a prune pass open until the test releases it, so the
// pruner's goroutine can be observed in the middle of its work.
type blockingStore struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingStore) Append(context.Context, stream.CursorMetadata) error { return nil }

func (s *blockingStore) Latest(context.Context, string) (stream.CursorMetadata, bool, error) {
	return stream.CursorMetadata{}, false, nil
}

func (s *blockingStore) Prune(context.Context, stream.PrunePolicy) (stream.PruneStats, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return stream.PruneStats{}, nil
}

// TestPrunerCloseJoinsItsGoroutine checks the property that makes the
// pruner leak-free: Close waits for the goroutine it owns instead of merely
// asking it to stop.
func TestPrunerCloseJoinsItsGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	store := &blockingStore{started: make(chan struct{}, 1), release: make(chan struct{})}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{Interval: time.Hour})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the pruner never started a pass")
	}

	closed := make(chan error, 1)
	go func() { closed <- pruner.Close() }()

	// The pass is still running, so Close must still be waiting for it.
	select {
	case err := <-closed:
		close(store.release)
		t.Fatalf("Close() returned (%v) while the pruner was still running", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(store.release)
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return after the pruner finished")
	}

	// The goroutine is joined, so it is gone by the time Close returns.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Errorf("goroutines = %d, want at most %d: the pruner leaked one", got, before)
	}
}

func TestPrunerReportsFailuresAndKeepsRunning(t *testing.T) {
	t.Parallel()

	store := &memoryStore{}
	var (
		mu       sync.Mutex
		failures []error
	)
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		Interval: time.Millisecond,
		OnError: func(err error) {
			mu.Lock()
			defer mu.Unlock()
			failures = append(failures, err)
		},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	store.setError(errors.New("disk on fire"))
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	waitFor(t, "the failure to be reported", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(failures) > 0
	})

	mu.Lock()
	first := failures[0]
	mu.Unlock()
	if first.Error() != "pruning cursor metadata: disk on fire" {
		t.Errorf("reported failure = %q, want the store error to be wrapped", first.Error())
	}

	// A failing pass must not stop the loop.
	waitFor(t, "the loop to keep going", func() bool { return store.pruneCount() >= 2 })
}

func TestPrunerCountsPassesAndRemovals(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	store := &memoryStore{entries: []stream.CursorMetadata{
		{Key: "events", Cursor: "a", RecordedAt: now.Add(-2 * time.Hour)},
		{Key: "events", Cursor: "b", RecordedAt: now},
	}}
	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		MaxAge: time.Hour,
		Clock:  fixedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}

	stats := pruner.Stats()
	if stats.Passes != 1 {
		t.Errorf("passes = %d, want 1", stats.Passes)
	}
	if stats.Removed != 1 {
		t.Errorf("removed = %d, want 1", stats.Removed)
	}
	if stats.Errors != 0 {
		t.Errorf("errors = %d, want 0", stats.Errors)
	}
}

func TestPrunerReportsEachPass(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		stats []stream.PruneStats
	)
	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{
		Interval: time.Millisecond,
		OnPrune: func(s stream.PruneStats) {
			mu.Lock()
			defer mu.Unlock()
			stats = append(stats, s)
		},
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	waitFor(t, "a pass to be reported", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(stats) > 0
	})
}

func TestPrunerPruneOnceHonoursCancellation(t *testing.T) {
	t.Parallel()

	pruner, err := stream.NewPruner(&memoryStore{}, stream.PruneOptions{})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pruner.PruneOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("PruneOnce() error = %v, want context.Canceled", err)
	}
}

// TestPrunerBoundsTheMetadataFileDuringAStream is the end-to-end behaviour:
// a stream that records a checkpoint per page must not leave an unbounded
// file behind, even while it is still running.
func TestPrunerBoundsTheMetadataFileDuringAStream(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 40)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))

	src, err := stream.NewEventSource(srv, stream.EventQuery{
		StartLedger: 1000,
		PageLimit:   5,
	})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	store, err := stream.NewFileMetadataStore(path)
	if err != nil {
		t.Fatalf("NewFileMetadataStore() error = %v", err)
	}

	pruner, err := stream.NewPruner(store, stream.PruneOptions{
		Interval: time.Millisecond,
		Keep:     3,
	})
	if err != nil {
		t.Fatalf("NewPruner() error = %v", err)
	}
	if err := pruner.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := pruner.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	tailer := stream.NewTailer(src, &collector{}, stream.Options{
		OnPage: func(page stream.Page) {
			if err := store.Append(context.Background(), stream.CursorMetadata{
				Key:        "events",
				Cursor:     page.Cursor,
				Ledger:     page.Ledger,
				RecordedAt: time.Now().UTC(),
			}); err != nil {
				t.Errorf("Append() error = %v", err)
			}
		},
	})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// The pruner runs on an interval, so the file is compacted again once
	// the stream is done, exactly as the CLI does at shutdown.
	if _, err := pruner.PruneOnce(context.Background()); err != nil {
		t.Fatalf("PruneOnce() error = %v", err)
	}

	entries, err := store.Entries(context.Background(), "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) > 3 {
		t.Errorf("kept %d entries, want at most 3", len(entries))
	}
	if len(entries) == 0 {
		t.Error("the stream recorded no checkpoints at all")
	}

	// The newest checkpoint must be the position the stream reached, so a
	// later run can pick the stream up where this one stopped.
	latest, found, err := store.Latest(context.Background(), "events")
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if !found {
		t.Fatal("Latest() found no checkpoint")
	}
	if latest.Cursor != events[len(events)-1].ID {
		t.Errorf("latest cursor = %q, want %q", latest.Cursor, events[len(events)-1].ID)
	}
}

// fixedClock is a Clock whose time never moves.
type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

func (c fixedClock) Sleep(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}

// waitFor waits for a condition, so a test about a background goroutine fails
// with a clear message instead of hanging.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
