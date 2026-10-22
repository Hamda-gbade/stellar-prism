package stream_test

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func testAccountAddress(t *testing.T, seed byte) string {
	t.Helper()
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = seed + byte(i)
	}
	address, err := strkey.Encode(strkey.VersionByteAccountID, raw)
	if err != nil {
		t.Fatalf("encoding account address: %v", err)
	}
	return address
}

func testMuxedAccountAddress(t *testing.T, seed byte) (muxed, account string) {
	t.Helper()
	raw := make([]byte, 40)
	for i := range raw[:32] {
		raw[i] = seed + byte(i)
	}
	raw[39] = 7
	encoded, err := strkey.Encode(strkey.VersionByteMuxedAccount, raw)
	if err != nil {
		t.Fatalf("encoding muxed account address: %v", err)
	}
	account, err = strkey.Encode(strkey.VersionByteAccountID, raw[:32])
	if err != nil {
		t.Fatalf("encoding underlying account address: %v", err)
	}
	return encoded, account
}

func feeBumpEnvelope(feeSource, innerSource string, ops ...xdr.Operation) xdr.TransactionEnvelope {
	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &xdr.FeeBumpTransactionEnvelope{
			Tx: xdr.FeeBumpTransaction{
				FeeSource: xdr.MustMuxedAddress(feeSource),
				Fee:       2000,
				InnerTx: xdr.FeeBumpTransactionInnerTx{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1: &xdr.TransactionV1Envelope{
						Tx: xdr.Transaction{
							SourceAccount: xdr.MustMuxedAddress(innerSource),
							Fee:           100,
							Operations:    ops,
						},
						Signatures: []xdr.DecoratedSignature{{}, {}},
					},
				},
			},
			Signatures: []xdr.DecoratedSignature{{}},
		},
	}
}

func feeBumpSuccessResult(t *testing.T) (string, xdr.Hash) {
	t.Helper()

	innerOps := []xdr.OperationResult{
		invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess),
	}
	var hash xdr.Hash
	for i := range hash {
		hash[i] = 0xab
	}
	inner := xdr.InnerTransactionResult{
		FeeCharged: 120,
		Result: xdr.InnerTransactionResultResult{
			Code:    xdr.TransactionResultCodeTxSuccess,
			Results: &innerOps,
		},
	}
	outer := xdr.TransactionResult{
		FeeCharged: 250,
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
			InnerResultPair: &xdr.InnerTransactionResultPair{
				TransactionHash: hash,
				Result:          inner,
			},
		},
	}
	return txResultXDR(t, outer), hash
}

func TestEnrichTransactionClassifiesFeeBump(t *testing.T) {
	t.Parallel()

	feeSource := testAccountAddress(t, 1)
	innerSource := testAccountAddress(t, 33)
	envelope := encodeEnvelope(t, feeBumpEnvelope(feeSource, innerSource,
		hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	result, innerHash := feeBumpSuccessResult(t)

	tx := classifyOne(t, classifiedTransaction(30, 1, "bump", envelope, result, true))

	if tx.Status != stream.TxStatusSuccess {
		t.Errorf("status = %q, want success", tx.Status)
	}
	if tx.FeeAccount != feeSource {
		t.Errorf("fee account = %q, want the fee-bump source %q", tx.FeeAccount, feeSource)
	}
	if tx.FeeBump == nil {
		t.Fatal("fee bump classification = nil, want wrapper details")
	}
	bump := tx.FeeBump
	if bump.FeeAccount != feeSource {
		t.Errorf("wrapper fee account = %q, want %q", bump.FeeAccount, feeSource)
	}
	if bump.InnerSource != innerSource {
		t.Errorf("inner source = %q, want %q", bump.InnerSource, innerSource)
	}
	if bump.MaxFee != 2000 {
		t.Errorf("max fee = %d, want 2000", bump.MaxFee)
	}
	if bump.InnerFee != 100 {
		t.Errorf("inner fee = %d, want 100", bump.InnerFee)
	}
	if bump.FeeCharged != 250 {
		t.Errorf("fee charged = %d, want 250", bump.FeeCharged)
	}
	if bump.InnerOperations != 1 {
		t.Errorf("inner operations = %d, want 1", bump.InnerOperations)
	}
	if bump.OuterSignatures != 1 || bump.InnerSignatures != 2 {
		t.Errorf("signatures = (%d outer, %d inner), want (1, 2)",
			bump.OuterSignatures, bump.InnerSignatures)
	}
	if bump.InnerHash != hex.EncodeToString(innerHash[:]) {
		t.Errorf("inner hash = %q, want %q", bump.InnerHash, hex.EncodeToString(innerHash[:]))
	}
}

func TestFeeBumpSourceEmitsOnlyWrappedTransactions(t *testing.T) {
	t.Parallel()

	feeSource := testAccountAddress(t, 1)
	innerSource := testAccountAddress(t, 33)
	wrapped := encodeEnvelope(t, feeBumpEnvelope(feeSource, innerSource,
		hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	plain := encodeEnvelope(t, v1Envelope(hostFunctionOp(uploadFunction([]byte{0x00, 0x61, 0x73, 0x6d}))))
	result, _ := feeBumpSuccessResult(t)

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		classifiedTransaction(31, 1, "plain", plain, successResult(t), true),
		classifiedTransaction(31, 2, "wrapped", wrapped, result, true),
	})
	src, err := stream.NewFeeBumpSource(srv, stream.FeeBumpQuery{})
	if err != nil {
		t.Fatalf("NewFeeBumpSource() error = %v", err)
	}

	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("got %d records, want only the fee-bump wrapper", len(batch.Records))
	}
	if batch.Records[0].Transaction.Hash != "wrapped" || batch.Records[0].Transaction.FeeBump == nil {
		t.Errorf("record = %+v, want the classified wrapper", batch.Records[0].Transaction)
	}
}

func TestFeeAccountFilterMatchesEffectivePayer(t *testing.T) {
	t.Parallel()

	feeSource := testAccountAddress(t, 1)
	innerSource := testAccountAddress(t, 33)

	filter, err := stream.FeeAccountFilter([]string{feeSource})
	if err != nil {
		t.Fatalf("FeeAccountFilter() error = %v", err)
	}
	if !filter(&stream.Transaction{FeeAccount: feeSource}) {
		t.Error("filter rejected the effective fee payer")
	}
	if filter(&stream.Transaction{FeeAccount: innerSource}) {
		t.Error("filter accepted the inner source, which did not pay the fee")
	}
}

func TestFeeAccountFilterNormalizesMuxedAddresses(t *testing.T) {
	t.Parallel()

	muxed, account := testMuxedAccountAddress(t, 5)
	filter, err := stream.FeeAccountFilter([]string{muxed})
	if err != nil {
		t.Fatalf("FeeAccountFilter() error = %v", err)
	}
	if !filter(&stream.Transaction{FeeAccount: account}) {
		t.Errorf("filter with %q did not match underlying account %q", muxed, account)
	}
}

func TestFeeAccountFilterRejectsInvalidAddress(t *testing.T) {
	t.Parallel()

	_, err := stream.FeeAccountFilter([]string{"not-an-account"})
	if err == nil {
		t.Fatal("FeeAccountFilter() error = nil, want a validation failure")
	}
	if !strings.Contains(err.Error(), "not a Stellar account address") {
		t.Errorf("error = %v, want an address validation message", err)
	}
}

func TestFeeBumpSourceRejectsInvalidAccount(t *testing.T) {
	t.Parallel()

	_, err := stream.NewFeeBumpSource(fakerpc.NewTransactionServer(nil), stream.FeeBumpQuery{
		FeeAccounts: []string{"not-an-account"},
	})
	if err == nil {
		t.Fatal("NewFeeBumpSource() error = nil, want a validation failure")
	}
}

func TestFeeBumpFilterIgnoresNil(t *testing.T) {
	t.Parallel()

	if stream.FeeBumpFilter()(nil) {
		t.Error("FeeBumpFilter() accepted a nil transaction")
	}
}
