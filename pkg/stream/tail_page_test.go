package stream_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestTailReportsEveryFullyEmittedPage checks the checkpoint hook: a page
// that was delivered in full reports the position to resume from.
func TestTailReportsEveryFullyEmittedPage(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 25)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))

	var pages []stream.Page
	opts := stream.Options{
		OnPage: func(p stream.Page) { pages = append(pages, p) },
	}
	sink, _, err := runTail(t, srv, opts, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sink.ids == nil {
		t.Fatal("no records were emitted")
	}
	if len(pages) != 3 {
		t.Fatalf("reported %d pages, want 3", len(pages))
	}
	if pages[0].Source != "events" {
		t.Errorf("page source = %q, want events", pages[0].Source)
	}
	if pages[0].Records != 10 {
		t.Errorf("page records = %d, want 10", pages[0].Records)
	}
	if pages[0].Ledger != 109 {
		t.Errorf("page ledger = %d, want 109", pages[0].Ledger)
	}
	if pages[0].Cursor != events[9].ID {
		t.Errorf("page cursor = %q, want %q", pages[0].Cursor, events[9].ID)
	}
	if last := pages[len(pages)-1]; last.Cursor != events[len(events)-1].ID {
		t.Errorf("last page cursor = %q, want %q", last.Cursor, events[len(events)-1].ID)
	}
}

// TestTailDoesNotReportAPartiallyEmittedPage is the safety property: a
// recorded position must never point past records that were not delivered.
func TestTailDoesNotReportAPartiallyEmittedPage(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 25)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))

	var pages []stream.Page
	opts := stream.Options{
		Limit:  12,
		OnPage: func(p stream.Page) { pages = append(pages, p) },
	}
	sink, _, err := runTail(t, srv, opts, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(sink.ids) != 12 {
		t.Fatalf("emitted %d records, want 12", len(sink.ids))
	}
	if len(pages) != 1 {
		t.Fatalf("reported %d pages, want only the complete one", len(pages))
	}
	if pages[0].Cursor != events[9].ID {
		t.Errorf("page cursor = %q, want %q", pages[0].Cursor, events[9].ID)
	}
}

// TestTailDoesNotReportAPageTheSinkRejected covers the other way a page can
// be incomplete: the sink failed halfway through it.
func TestTailDoesNotReportAPageTheSinkRejected(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 25)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))

	sink := &collector{failAt: 3, err: errors.New("pipe closed")}
	var pages []stream.Page
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}
	tailer := stream.NewTailer(src, sink, stream.Options{
		OnPage: func(p stream.Page) { pages = append(pages, p) },
	})
	if err := tailer.Run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want the sink failure")
	}
	if len(pages) != 0 {
		t.Errorf("reported %d pages, want none", len(pages))
	}
}

// TestTailResumesFromARecordedCursor checks that a position recorded by an
// earlier run is where the next run starts.
func TestTailResumesFromARecordedCursor(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 25)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(10))

	opts := stream.Options{StartCursor: events[9].ID}
	sink, _, err := runTail(t, srv, opts, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	want := idsOf(events[10:])
	assertStreamInvariants(t, sink.ids, want)
}

// TestTailChecksForAGapAfterResuming makes sure a resumed stream is still
// gap-checked: the recorded cursor is a real position, and the server may
// have pruned past it.
func TestTailChecksForAGapAfterResuming(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 25)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(10),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			0: {OldestLedger: 500},
		}),
	)

	opts := stream.Options{StartCursor: events[9].ID}
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}
	tailer := stream.NewTailer(src, &collector{}, opts)
	err = tailer.Run(context.Background())

	var gap *stream.GapError
	if !errors.As(err, &gap) {
		t.Fatalf("Run() error = %v, want a gap", err)
	}
	if gap.From != 109 {
		t.Errorf("gap from = %d, want 109", gap.From)
	}
}
