package stream_test

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func txResultXDR(t *testing.T, result xdr.TransactionResult) string {
	t.Helper()
	b, err := result.MarshalBinary()
	if err != nil {
		t.Fatalf("marshalling result: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func invokeResult(code xdr.InvokeHostFunctionResultCode) xdr.OperationResult {
	result := &xdr.InvokeHostFunctionResult{Code: code}
	if code == xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess {
		// The success arm carries the host function's return hash; the
		// generated encoder dereferences it, so it must not be nil.
		var hash xdr.Hash
		result.Success = &hash
	}
	return xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr: &xdr.OperationResultTr{
			Type:                     xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionResult: result,
		},
	}
}

func txResult(code xdr.TransactionResultCode, ops ...xdr.OperationResult) xdr.TransactionResult {
	return xdr.TransactionResult{
		FeeCharged: 100,
		Result: xdr.TransactionResultResult{
			Code:    code,
			Results: &ops,
		},
	}
}

func successResult(t *testing.T) string {
	t.Helper()
	return txResultXDR(t, txResult(xdr.TransactionResultCodeTxSuccess,
		invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess)))
}

func trappedResult(t *testing.T) string {
	t.Helper()
	return txResultXDR(t, txResult(xdr.TransactionResultCodeTxFailed,
		invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped)))
}

func resourceLimitResult(t *testing.T) string {
	t.Helper()
	return txResultXDR(t, txResult(xdr.TransactionResultCodeTxFailed,
		invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionResourceLimitExceeded)))
}

func classifiedTransaction(ledger uint32, order int32, hash, envelope, result string, successful bool) protocol.TransactionInfo {
	status := protocol.TransactionStatusFailed
	if successful {
		status = protocol.TransactionStatusSuccess
	}
	return protocol.TransactionInfo{
		TransactionDetails: protocol.TransactionDetails{
			Status:           status,
			TransactionHash:  hash,
			ApplicationOrder: order,
			Ledger:           ledger,
			EnvelopeXDR:      envelope,
			ResultXDR:        result,
		},
		LedgerCloseTime: 1_700_000_100,
	}
}

func classifyOne(t *testing.T, tx protocol.TransactionInfo) *stream.Transaction {
	t.Helper()

	record := stream.Record{
		ID:       "0000000001-0001",
		Sequence: tx.Ledger,
		Kind:     stream.KindTransaction,
		Transaction: &stream.Transaction{
			Hash:        tx.TransactionHash,
			Successful:  tx.Status == protocol.TransactionStatusSuccess,
			EnvelopeXDR: tx.EnvelopeXDR,
			ResultXDR:   tx.ResultXDR,
		},
	}
	if err := stream.EnrichTransaction(record.Transaction); err != nil {
		t.Fatalf("EnrichTransaction() error = %v", err)
	}
	return record.Transaction
}

func TestEnrichTransactionSuccess(t *testing.T) {
	t.Parallel()

	envelope := encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	tx := classifyOne(t, classifiedTransaction(10, 1, "abc", envelope, successResult(t), true))

	if tx.Status != stream.TxStatusSuccess {
		t.Errorf("status = %q, want %q", tx.Status, stream.TxStatusSuccess)
	}
	if tx.Failure != nil {
		t.Errorf("failure = %+v, want nil for a successful transaction", tx.Failure)
	}
	if tx.FeeAccount != "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF" {
		t.Errorf("fee account = %q, want the transaction source", tx.FeeAccount)
	}
	if tx.FeeBump != nil {
		t.Errorf("fee bump = %+v, want nil for an ordinary envelope", tx.FeeBump)
	}
}

func TestEnrichTransactionDecodesTrappedFailure(t *testing.T) {
	t.Parallel()

	envelope := encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	tx := classifyOne(t, classifiedTransaction(11, 1, "bad", envelope, trappedResult(t), false))

	if tx.Status != stream.TxStatusFailed {
		t.Errorf("status = %q, want %q", tx.Status, stream.TxStatusFailed)
	}
	if tx.Failure == nil {
		t.Fatal("failure = nil, want a decoded reason")
	}
	if tx.Failure.Detail.Code != "tx_failed" {
		t.Errorf("transaction code = %q, want tx_failed", tx.Failure.Detail.Code)
	}
	if tx.Failure.Operation == nil {
		t.Fatal("operation failure = nil, want the failing operation")
	}
	if !tx.Failure.Operation.Trapped {
		t.Error("operation was not marked trapped")
	}
	if tx.Failure.Operation.Detail.Code != "invoke_host_function_trapped" {
		t.Errorf("operation code = %q, want invoke_host_function_trapped", tx.Failure.Operation.Detail.Code)
	}
	if tx.Failure.Operation.Detail.Summary == "" {
		t.Error("operation summary is empty, want decoded plain English")
	}
	if tx.Failure.Headline == "" {
		t.Error("headline is empty")
	}
	if !tx.Failure.Trapped() {
		t.Error("Failure.Trapped() = false, want true")
	}
}

func TestEnrichTransactionDistinguishesNonTrapFailure(t *testing.T) {
	t.Parallel()

	envelope := encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	tx := classifyOne(t, classifiedTransaction(12, 1, "bad", envelope, resourceLimitResult(t), false))

	if tx.Failure == nil || tx.Failure.Operation == nil {
		t.Fatal("failure or operation is nil")
	}
	if tx.Failure.Operation.Trapped {
		t.Error("resource-limit failure was marked trapped")
	}
	if !strings.Contains(tx.Failure.Operation.Detail.Code, "resource_limit_exceeded") {
		t.Errorf("operation code = %q, want a resource-limit code", tx.Failure.Operation.Detail.Code)
	}
}

func TestEnrichTransactionWithoutEnvelopeKeepsStatus(t *testing.T) {
	t.Parallel()

	tx := &stream.Transaction{Hash: "abc", Successful: false}
	if err := stream.EnrichTransaction(tx); err != nil {
		t.Fatalf("EnrichTransaction() error = %v, want a graceful skip", err)
	}
	if tx.Status != stream.TxStatusFailed {
		t.Errorf("status = %q, want failed from the RPC status", tx.Status)
	}
	if tx.FeeBump != nil || tx.Failure != nil {
		t.Errorf("derived fields = %+v, want none without an envelope", tx)
	}
}

func TestParseTransactionStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   []string
		want    map[string]bool
		wantErr bool
	}{
		{name: "empty", input: nil, want: nil},
		{name: "success", input: []string{"SUCCESS"}, want: map[string]bool{stream.TxStatusSuccess: true}},
		{name: "both", input: []string{"success", "failed"}, want: map[string]bool{
			stream.TxStatusSuccess: true,
			stream.TxStatusFailed:  true,
		}},
		{name: "unknown", input: []string{"pending"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := stream.ParseTransactionStatuses(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatal("ParseTransactionStatuses() error = nil, want a validation failure")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTransactionStatuses() error = %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("statuses = %v, want %v", got, tc.want)
			}
			for status := range tc.want {
				if !got[status] {
					t.Errorf("statuses = %v, missing %q", got, status)
				}
			}
		})
	}
}

func TestTxStatusSourceFiltersAndClassifies(t *testing.T) {
	t.Parallel()

	envelope := encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		classifiedTransaction(20, 1, "ok", envelope, successResult(t), true),
		classifiedTransaction(20, 2, "bad", envelope, trappedResult(t), false),
	})

	src, err := stream.NewTxStatusSource(srv, stream.TxStatusQuery{Statuses: []string{stream.TxStatusFailed}})
	if err != nil {
		t.Fatalf("NewTxStatusSource() error = %v", err)
	}

	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("got %d records, want only the failure", len(batch.Records))
	}
	record := batch.Records[0]
	if record.Transaction.Hash != "bad" {
		t.Errorf("hash = %q, want the failed transaction", record.Transaction.Hash)
	}
	if record.Transaction.Status != stream.TxStatusFailed || record.Transaction.Failure == nil {
		t.Errorf("record was not classified: %+v", record.Transaction)
	}
	// The cursor must still advance across the filtered-out record.
	if batch.NextCursor == "" {
		t.Error("NextCursor is empty, want the server's position")
	}
}

func TestTxStatusSourceMetadata(t *testing.T) {
	t.Parallel()

	src, err := stream.NewTxStatusSource(fakerpc.NewTransactionServer(nil), stream.TxStatusQuery{})
	if err != nil {
		t.Fatalf("NewTxStatusSource() error = %v", err)
	}
	if src.Name() != "transactions" {
		t.Errorf("Name() = %q, want transactions", src.Name())
	}
	if ledger, ok := src.CursorLedger("0000000050-000001"); !ok || ledger != 50 {
		t.Errorf("CursorLedger() = (%d, %v), want (50, true)", ledger, ok)
	}
	if _, ok := src.CursorLedger("not-a-cursor"); ok {
		t.Error("CursorLedger() accepted an invalid cursor")
	}

	// Rewind is what lets a filtered stream recover from a retention gap.
	src.Rewind(50)
}

func TestEnrichTransactionRejectsMalformedResult(t *testing.T) {
	t.Parallel()

	envelope := encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	err := stream.EnrichTransaction(&stream.Transaction{
		Hash:        "abc",
		EnvelopeXDR: envelope,
		ResultXDR:   "not-base64",
	})
	if err == nil {
		t.Fatal("EnrichTransaction() error = nil, want a malformed-result failure")
	}
}

func TestEnrichTransactionExplainsTransactionLevelFailure(t *testing.T) {
	t.Parallel()

	envelope := encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	// tx_bad_seq fails before any operation runs, so there is no operation
	// result to attribute the failure to.
	result := txResultXDR(t, xdr.TransactionResult{
		FeeCharged: 100,
		Result:     xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxBadSeq},
	})
	tx := classifyOne(t, classifiedTransaction(13, 1, "seq", envelope, result, false))

	if tx.Failure == nil {
		t.Fatal("failure = nil, want the transaction-level reason")
	}
	if tx.Failure.Detail.Code != "tx_bad_seq" {
		t.Errorf("code = %q, want tx_bad_seq", tx.Failure.Detail.Code)
	}
	if tx.Failure.Operation != nil {
		t.Errorf("operation = %+v, want none for a transaction-level failure", tx.Failure.Operation)
	}
	if tx.Failure.Trapped() {
		t.Error("transaction-level failure was marked trapped")
	}
}

func TestTxStatusSourceRejectsUnknownStatus(t *testing.T) {
	t.Parallel()

	if _, err := stream.NewTxStatusSource(fakerpc.NewTransactionServer(nil),
		stream.TxStatusQuery{Statuses: []string{"succeeded"}}); err == nil {
		t.Fatal("NewTxStatusSource() error = nil, want a validation failure")
	}
}
