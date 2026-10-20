package stream_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type recordBatchSink struct {
	mu         sync.Mutex
	batches    [][]stream.Record
	batchCalls int
	emitCalls  int
	err        error
}

func (s *recordBatchSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emitCalls++
	if s.err != nil {
		return s.err
	}
	s.batches = append(s.batches, []stream.Record{r})
	return nil
}

func (s *recordBatchSink) EmitBatch(records []stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchCalls++
	if s.err != nil {
		return s.err
	}
	s.batches = append(s.batches, records)
	return nil
}

func (s *recordBatchSink) Flush() error { return nil }

func (s *recordBatchSink) snapshot() ([][]stream.Record, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]stream.Record(nil), s.batches...), s.batchCalls, s.emitCalls
}

func batchRecords(ids ...string) []stream.Record {
	out := make([]stream.Record, 0, len(ids))
	for _, id := range ids {
		out = append(out, stream.Record{ID: id})
	}
	return out
}

func TestBatcherFlushesAtRecordCount(t *testing.T) {
	t.Parallel()

	sink := &recordBatchSink{}
	b := stream.NewBatcher(sink, stream.BatcherOptions{MaxRecords: 2})

	for _, r := range batchRecords("a", "b", "c") {
		if err := b.Emit(r); err != nil {
			t.Fatalf("Emit(%s) error = %v", r.ID, err)
		}
	}

	batches, calls, emits := sink.snapshot()
	if calls != 1 || emits != 0 {
		t.Fatalf("EmitBatch calls = %d, Emit calls = %d, want 1 and 0", calls, emits)
	}
	if len(batches) != 1 || len(batches[0]) != 2 || batches[0][0].ID != "a" || batches[0][1].ID != "b" {
		t.Fatalf("batches = %v, want one batch of a and b", batches)
	}
	if err := b.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	batches, _, _ = sink.snapshot()
	if len(batches) != 2 || len(batches[1]) != 1 || batches[1][0].ID != "c" {
		t.Fatalf("batches after Flush = %v, want a final batch with c", batches)
	}
}

func TestBatcherFlushesAfterDelay(t *testing.T) {
	t.Parallel()

	flushed := make(chan []stream.Record, 1)
	b := stream.NewBatcher(stream.SinkFunc(func(r stream.Record) error {
		flushed <- []stream.Record{r}
		return nil
	}), stream.BatcherOptions{MaxDelay: 20 * time.Millisecond})

	if err := b.Emit(stream.Record{ID: "a"}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	select {
	case got := <-flushed:
		if len(got) != 1 || got[0].ID != "a" {
			t.Fatalf("flushed %v, want record a", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("time threshold did not flush the buffer")
	}
}

func TestBatcherFlushEmitsPendingRecords(t *testing.T) {
	t.Parallel()

	sink := &recordBatchSink{}
	b := stream.NewBatcher(sink, stream.BatcherOptions{MaxRecords: 100})
	if err := b.Emit(stream.Record{ID: "a"}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	got, _, _ := sink.snapshot()
	if len(got) != 0 {
		t.Fatalf("emitted %v before Flush", got)
	}
	if err := b.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	got, _, _ = sink.snapshot()
	if len(got) != 1 || got[0][0].ID != "a" {
		t.Fatalf("flushed %v, want record a", got)
	}
	if err := b.Flush(); err != nil {
		t.Fatalf("second Flush() error = %v", err)
	}
	got, _, _ = sink.snapshot()
	if len(got) != 1 {
		t.Fatalf("empty Flush emitted another batch: %v", got)
	}
}

func TestBatcherCloseFlushesAndIsIdempotent(t *testing.T) {
	t.Parallel()

	sink := &recordBatchSink{}
	b := stream.NewBatcher(sink, stream.BatcherOptions{MaxDelay: 20 * time.Millisecond})
	if err := b.Emit(stream.Record{ID: "a"}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := b.Emit(stream.Record{ID: "b"}); err == nil {
		t.Fatal("Emit() after Close() error = nil, want an error")
	}

	time.Sleep(40 * time.Millisecond)
	got, _, _ := sink.snapshot()
	if len(got) != 1 || got[0][0].ID != "a" {
		t.Fatalf("batches after Close = %v, want just the record flushed by Close", got)
	}
}

func TestBatcherPropagatesSinkErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("disk full")
	sink := &recordBatchSink{err: wantErr}
	b := stream.NewBatcher(sink, stream.BatcherOptions{MaxRecords: 1})

	err := b.Emit(stream.Record{ID: "a"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Emit() error = %v, want it to wrap %v", err, wantErr)
	}
	if err := b.Flush(); !errors.Is(err, wantErr) {
		t.Fatalf("Flush() error = %v, want it to wrap %v", err, wantErr)
	}
}

func TestBatcherReportsTimeTriggeredErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("sink failed in the background")
	errs := make(chan error, 1)
	b := stream.NewBatcher(stream.SinkFunc(func(stream.Record) error {
		return wantErr
	}), stream.BatcherOptions{
		MaxDelay: 10 * time.Millisecond,
		OnError:  func(err error) { errs <- err },
	})

	if err := b.Emit(stream.Record{ID: "a"}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	select {
	case err := <-errs:
		if !errors.Is(err, wantErr) {
			t.Fatalf("OnError(%v), want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnError was not called for a failed timed flush")
	}
	if err := b.Emit(stream.Record{ID: "b"}); !errors.Is(err, wantErr) {
		t.Fatalf("Emit() after a failed flush = %v, want %v", err, wantErr)
	}
}

func TestBatcherPreservesRecordOrder(t *testing.T) {
	t.Parallel()

	var got []string
	b := stream.NewBatcher(stream.SinkFunc(func(r stream.Record) error {
		got = append(got, r.ID)
		return nil
	}), stream.BatcherOptions{MaxRecords: 5})

	for i := range 10 {
		if err := b.Emit(stream.Record{ID: fmt.Sprintf("%02d", i)}); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	want := []string{"00", "01", "02", "03", "04", "05", "06", "07", "08", "09"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("emitted %v, want %v", got, want)
	}
}

func TestBatcherFallsBackToPerRecordEmit(t *testing.T) {
	t.Parallel()

	var got []string
	b := stream.NewBatcher(stream.SinkFunc(func(r stream.Record) error {
		got = append(got, r.ID)
		return nil
	}), stream.BatcherOptions{MaxRecords: 2})

	for _, r := range batchRecords("a", "b") {
		if err := b.Emit(r); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	if fmt.Sprint(got) != "[a b]" {
		t.Fatalf("emitted %v, want [a b]", got)
	}
}

func TestNewBatcherTreatsNegativeThresholdsAsDisabled(t *testing.T) {
	t.Parallel()

	var got []string
	b := stream.NewBatcher(stream.SinkFunc(func(r stream.Record) error {
		got = append(got, r.ID)
		return nil
	}), stream.BatcherOptions{MaxRecords: -1, MaxDelay: -time.Second})
	if err := b.Emit(stream.Record{ID: "a"}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("negative thresholds flushed immediately: %v", got)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if fmt.Sprint(got) != "[a]" {
		t.Fatalf("emitted %v, want [a]", got)
	}
}
