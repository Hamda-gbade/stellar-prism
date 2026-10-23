package fakerpc_test

import (
	"context"
	"errors"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

// TestGetEventsBatchServesEveryRequest pins the contract the SDK client, the
// pool and the tailer all rely on: a batch of N requests produces N responses
// in request order, each reflecting that request's starting point.
func TestGetEventsBatchServesEveryRequest(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(fakerpc.MakeEvents(10, 6))

	reqs := []protocol.GetEventsRequest{
		{StartLedger: 10},
		{StartLedger: 14},
	}
	responses, err := srv.GetEventsBatch(context.Background(), reqs)
	if err != nil {
		t.Fatalf("GetEventsBatch() error = %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("GetEventsBatch() returned %d responses, want one per request", len(responses))
	}
	if got, want := len(responses[0].Events), 6; got != want {
		t.Errorf("responses[0] served %d events, want %d from ledger 10", got, want)
	}
	if got, want := len(responses[1].Events), 2; got != want {
		t.Errorf("responses[1] served %d events, want %d from ledger 14", got, want)
	}
	if got, want := responses[1].Events[0].Ledger, int32(14); got != want {
		t.Errorf("responses[1] starts at ledger %d, want %d", got, want)
	}
}

// TestGetEventsBatchStopsAtScriptedError proves a failure aborts the batch and
// reports how far it got, so a caller never has to guess which entries were
// served before the error.
func TestGetEventsBatchStopsAtScriptedError(t *testing.T) {
	t.Parallel()

	dropped := errors.New("connection reset by peer")
	srv := fakerpc.NewServer(fakerpc.MakeEvents(10, 6), fakerpc.WithScript(map[int]fakerpc.Behaviour{
		1: {Err: dropped},
	}))

	reqs := []protocol.GetEventsRequest{
		{StartLedger: 10},
		{StartLedger: 11},
		{StartLedger: 12},
	}
	responses, err := srv.GetEventsBatch(context.Background(), reqs)
	if !errors.Is(err, dropped) {
		t.Fatalf("GetEventsBatch() error = %v, want the scripted failure", err)
	}
	if len(responses) != 1 {
		t.Errorf("GetEventsBatch() returned %d responses before failing, want 1", len(responses))
	}
	// The failed entry still consumes its call slot, so a retry resumes at the
	// next request rather than silently skipping one.
	if got := srv.Calls(); got != 2 {
		t.Errorf("Calls() = %d, want 2 (the batch stopped at the scripted call)", got)
	}
}

// TestGetEventsBatchRejectsConflictingCursor pins the validation shared with
// the single-request path: a start ledger and a cursor are mutually exclusive.
func TestGetEventsBatchRejectsConflictingCursor(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(fakerpc.MakeEvents(10, 3))
	cursor := protocol.Cursor{Ledger: 11, Tx: 1, Op: 0, Event: 0}

	_, err := srv.GetEventsBatch(context.Background(), []protocol.GetEventsRequest{
		{StartLedger: 10, Pagination: &protocol.PaginationOptions{Cursor: &cursor}},
	})
	if err == nil {
		t.Fatal("GetEventsBatch() error = nil, want a rejection of cursor plus start ledger")
	}
}

// TestGetEventsBatchEmptyIsNoop keeps the fast path honest: an empty batch
// costs nothing and reports nothing.
func TestGetEventsBatchEmptyIsNoop(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(fakerpc.MakeEvents(10, 3))

	responses, err := srv.GetEventsBatch(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetEventsBatch() error = %v", err)
	}
	if len(responses) != 0 {
		t.Errorf("GetEventsBatch() returned %d responses for an empty batch", len(responses))
	}
	if got := srv.Calls(); got != 0 {
		t.Errorf("Calls() = %d, want 0 for an empty batch", got)
	}
}
