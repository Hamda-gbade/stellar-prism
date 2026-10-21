package stream_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestTailCheckpointsWaitForReorderedPages(t *testing.T) {
	t.Parallel()
	low := stream.Record{ID: "0000000010-01", Sequence: 10}
	high := stream.Record{ID: "0000000010-02", Sequence: 10}
	next := stream.Record{ID: "0000000011-01", Sequence: 11}
	src := &replaySource{batches: []stream.Batch{
		{Records: []stream.Record{high}, NextCursor: "page-1", Full: true},
		{Records: []stream.Record{low}, NextCursor: "page-2", Full: true},
		{NextCursor: "page-3", Full: true},
		{Records: []stream.Record{next}, NextCursor: "page-4"},
	}}
	var emitted []string
	var pages []stream.Page
	tailer := stream.NewTailer(src, stream.SinkFunc(func(r stream.Record) error {
		emitted = append(emitted, r.ID)
		return nil
	}), stream.Options{OnPage: func(p stream.Page) {
		// Neither a partial page nor an empty filtered page may publish a
		// checkpoint while earlier same-ledger records are still buffered.
		if len(emitted) < 2 {
			t.Errorf("checkpoint %s preceded ledger delivery: %v", p.Cursor, emitted)
		}
		pages = append(pages, p)
	}})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{low.ID, high.ID, next.ID}; !reflect.DeepEqual(emitted, want) {
		t.Fatalf("emitted %v, want %v", emitted, want)
	}
	if len(pages) != 4 {
		t.Fatalf("got %d checkpoints, want 4", len(pages))
	}
	for i, count := range []int{1, 1, 0, 1} {
		if pages[i].Records != count {
			t.Errorf("page %d records = %d, want %d", i, pages[i].Records, count)
		}
	}
	if tailer.Cursor() != "page-4" {
		t.Errorf("Cursor() = %q, want page-4", tailer.Cursor())
	}
}

type interruptedPageSource struct {
	replaySource
	err error
}

func (s *interruptedPageSource) Fetch(ctx context.Context, cursor string) (stream.Batch, error) {
	if s.calls == len(s.batches) {
		return stream.Batch{}, s.err
	}
	return s.replaySource.Fetch(ctx, cursor)
}

func TestTailDoesNotCheckpointBufferedRecordsOnFetchFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("invalid RPC response")
	src := &interruptedPageSource{
		replaySource: replaySource{batches: []stream.Batch{{
			Records:    []stream.Record{{ID: "0000000010-01", Sequence: 10}},
			NextCursor: "undelivered", Full: true,
		}}},
		err: wantErr,
	}
	var pages []stream.Page
	tailer := stream.NewTailer(src, &collector{}, stream.Options{
		StartCursor: "delivered",
		OnPage:      func(p stream.Page) { pages = append(pages, p) },
	})
	if err := tailer.Run(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Run() = %v, want %v", err, wantErr)
	}
	if len(pages) != 0 || tailer.Cursor() != "delivered" {
		t.Fatalf("published buffered progress: pages=%v, cursor=%s", pages, tailer.Cursor())
	}
}

func TestTailLimitDoesNotCheckpointPartiallyReorderedLedger(t *testing.T) {
	t.Parallel()
	src := &replaySource{batches: []stream.Batch{
		{Records: []stream.Record{{ID: "0000000010-02", Sequence: 10}}, NextCursor: "page-1", Full: true},
		{Records: []stream.Record{{ID: "0000000010-01", Sequence: 10}}, NextCursor: "page-2", Full: true},
		{Records: []stream.Record{{ID: "0000000011-01", Sequence: 11}}, NextCursor: "page-3"},
	}}
	var pages []stream.Page
	sink := &collector{}
	tailer := stream.NewTailer(src, sink, stream.Options{
		Limit:  1,
		OnPage: func(p stream.Page) { pages = append(pages, p) },
	})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.ids) != 1 || sink.ids[0] != "0000000010-01" {
		t.Fatalf("emitted %v, want only the earlier record", sink.ids)
	}
	if len(pages) != 0 || tailer.Cursor() != "" {
		t.Fatalf("published incomplete page: pages=%v, cursor=%s", pages, tailer.Cursor())
	}
}

func TestBoundedFollowDrainsBufferedRecordsAtHead(t *testing.T) {
	t.Parallel()
	src := &replaySource{batches: []stream.Batch{{
		Records:    []stream.Record{{ID: "0000000010-01", Sequence: 10}},
		NextCursor: "page-1",
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newFakeClock()
	clock.onWake = cancel
	tailer := stream.NewTailer(src, &collector{}, stream.Options{Follow: true, Limit: 1, Clock: clock})
	if err := tailer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if tailer.Stats().Emitted != 1 || len(clock.slept) != 0 {
		t.Fatalf("bounded follow waited for another ledger: emitted=%d, sleeps=%d", tailer.Stats().Emitted, len(clock.slept))
	}
}

type reorderedPageFilter func(stream.Record) (bool, error)

func (f reorderedPageFilter) Match(r stream.Record) (bool, error) { return f(r) }

func TestReorderedCheckpointsCountOnlyMatchingRecords(t *testing.T) {
	t.Parallel()
	src := &replaySource{batches: []stream.Batch{
		{Records: []stream.Record{{ID: "0000000010-02", Sequence: 10}}, NextCursor: "page-1", Full: true},
		{Records: []stream.Record{{ID: "0000000010-01", Sequence: 10}}, NextCursor: "page-2", Full: true},
		{Records: []stream.Record{{ID: "0000000011-01", Sequence: 11}}, NextCursor: "page-3"},
	}}
	var pages []stream.Page
	sink := &collector{}
	tailer := stream.NewTailer(src, sink, stream.Options{
		Filter: reorderedPageFilter(func(r stream.Record) (bool, error) {
			return r.ID == "0000000010-02", nil
		}),
		OnPage: func(p stream.Page) { pages = append(pages, p) },
	})
	if err := tailer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pages) != 3 {
		t.Fatalf("got %d checkpoints, want 3", len(pages))
	}
	for i, want := range []int{1, 0, 0} {
		if pages[i].Records != want {
			t.Errorf("page %d delivered %d records, want %d", i, pages[i].Records, want)
		}
	}
	if tailer.Cursor() != "page-3" || tailer.Stats().Emitted != 1 || tailer.Stats().Filtered != 2 {
		t.Fatalf("cursor=%s stats=%+v", tailer.Cursor(), tailer.Stats())
	}
}

func TestFilteredCheckpointSurvivesLaterFilterFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("invalid filter value")
	src := &replaySource{batches: []stream.Batch{
		{Records: []stream.Record{{ID: "0000000010-01", Sequence: 10}}, NextCursor: "page-1", Full: true},
		{Records: []stream.Record{{ID: "0000000011-01", Sequence: 11}}, NextCursor: "page-2"},
	}}
	var pages []stream.Page
	tailer := stream.NewTailer(src, &collector{}, stream.Options{
		Filter: reorderedPageFilter(func(r stream.Record) (bool, error) {
			if r.Sequence == 11 {
				return false, wantErr
			}
			return false, nil
		}),
		OnPage: func(p stream.Page) { pages = append(pages, p) },
	})
	if err := tailer.Run(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Run() = %v, want %v", err, wantErr)
	}
	if len(pages) != 1 || pages[0].Records != 0 || tailer.Cursor() != "page-1" {
		t.Fatalf("pages=%v cursor=%s, want only the fully filtered page", pages, tailer.Cursor())
	}
}
