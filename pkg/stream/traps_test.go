package stream_test

import (
	"context"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestTrapSourceEmitsOnlyTrappedExecutions(t *testing.T) {
	t.Parallel()

	envelope := encodeEnvelope(t, v1Envelope(
		hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		classifiedTransaction(40, 1, "ok", envelope, successResult(t), true),
		classifiedTransaction(40, 2, "trap", envelope, trappedResult(t), false),
		classifiedTransaction(40, 3, "limit", envelope, resourceLimitResult(t), false),
	})

	src, err := stream.NewTrapSource(srv, stream.TrapQuery{})
	if err != nil {
		t.Fatalf("NewTrapSource() error = %v", err)
	}
	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("got %d records, want only the trapped execution", len(batch.Records))
	}

	record := batch.Records[0]
	if record.Transaction.Hash != "trap" {
		t.Errorf("hash = %q, want the trapped transaction", record.Transaction.Hash)
	}
	if !stream.IsTrapped(record.Transaction) {
		t.Error("IsTrapped() = false, want true")
	}
	if record.Transaction.Failure == nil || record.Transaction.Failure.Operation == nil {
		t.Fatal("record carries no decoded operation failure")
	}
	if record.Transaction.Failure.Operation.Trapped != true {
		t.Error("operation failure was not marked trapped")
	}
}

func TestTrappedFilterRejectsNonTraps(t *testing.T) {
	t.Parallel()

	filter := stream.TrappedFilter()
	if filter(nil) {
		t.Error("TrappedFilter() accepted a nil transaction")
	}
	if filter(&stream.Transaction{}) {
		t.Error("TrappedFilter() accepted an unclassified transaction")
	}
	if filter(&stream.Transaction{Failure: &stream.Failure{}}) {
		t.Error("TrappedFilter() accepted a failure with no operation")
	}
	if filter(&stream.Transaction{Failure: &stream.Failure{
		Operation: &stream.OperationFailure{Trapped: false},
	}}) {
		t.Error("TrappedFilter() accepted a non-trap operation failure")
	}
	if !filter(&stream.Transaction{Failure: &stream.Failure{
		Operation: &stream.OperationFailure{Trapped: true},
	}}) {
		t.Error("TrappedFilter() rejected a trap")
	}
}
