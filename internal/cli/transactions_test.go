package cli

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

func cliInvokeResult(code xdr.InvokeHostFunctionResultCode) xdr.OperationResult {
	result := &xdr.InvokeHostFunctionResult{Code: code}
	if code == xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess {
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

func cliResultXDR(t *testing.T, code xdr.TransactionResultCode, ops ...xdr.OperationResult) string {
	t.Helper()
	result := xdr.TransactionResult{
		FeeCharged: 100,
		Result: xdr.TransactionResultResult{
			Code:    code,
			Results: &ops,
		},
	}
	b, err := result.MarshalBinary()
	if err != nil {
		t.Fatalf("marshalling result: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func cliFeeBumpEnvelope(t *testing.T, feeSource, innerSource string) string {
	t.Helper()
	envelope := xdr.TransactionEnvelope{
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
							Operations: []xdr.Operation{{
								Body: xdr.OperationBody{
									Type: xdr.OperationTypeInvokeHostFunction,
									InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
										HostFunction: xdr.HostFunction{
											Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
											Wasm: &[]byte{0x00, 0x61, 0x73, 0x6d},
										},
									},
								},
							}},
						},
					},
				},
			},
		},
	}
	b, err := envelope.MarshalBinary()
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func cliAccountAddress(t *testing.T, seed byte) string {
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

func TestTransactionsCommandFiltersByStatus(t *testing.T) {
	envelope := cliUploadEnvelope(t, []byte{0x00, 0x61, 0x73, 0x6d})

	ok := cliTransaction(9500, "ok", protocol.TransactionStatusSuccess, envelope, "")
	bad := cliTransaction(9501, "bad", protocol.TransactionStatusFailed, envelope, "")
	bad.ResultXDR = cliResultXDR(t, xdr.TransactionResultCodeTxFailed,
		cliInvokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped))

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{ok, bad})
	withFake(t, srv)

	stdout, _, err := run(t, "transactions", "--from", "9500", "--json", "--status", "failed")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want only the failure\n%s", len(lines), stdout)
	}

	var rec struct {
		Status  string `json:"status"`
		Hash    string `json:"hash"`
		Failure struct {
			Code      string `json:"code"`
			Operation *struct {
				Trapped bool   `json:"trapped"`
				Code    string `json:"code"`
			} `json:"operation"`
		} `json:"failure"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, lines[0])
	}
	if rec.Hash != "bad" || rec.Status != "failed" {
		t.Errorf("record = %+v, want the failed transaction", rec)
	}
	if rec.Failure.Code != "tx_failed" {
		t.Errorf("failure code = %q, want tx_failed", rec.Failure.Code)
	}
	if rec.Failure.Operation == nil || !rec.Failure.Operation.Trapped {
		t.Errorf("failure operation = %+v, want a trapped operation", rec.Failure.Operation)
	}
}

func TestTransactionsCommandTrappedFilter(t *testing.T) {
	envelope := cliUploadEnvelope(t, []byte{0x00, 0x61, 0x73, 0x6d})

	trapped := cliTransaction(9600, "trap", protocol.TransactionStatusFailed, envelope, "")
	trapped.ResultXDR = cliResultXDR(t, xdr.TransactionResultCodeTxFailed,
		cliInvokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped))

	limited := cliTransaction(9601, "limit", protocol.TransactionStatusFailed, envelope, "")
	limited.ResultXDR = cliResultXDR(t, xdr.TransactionResultCodeTxFailed,
		cliInvokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionResourceLimitExceeded))

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{trapped, limited})
	withFake(t, srv)

	stdout, _, err := run(t, "transactions", "--from", "9600", "--json", "--trapped")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want only the trapped execution\n%s", len(lines), stdout)
	}
	if !strings.Contains(lines[0], `"trapped":true`) {
		t.Errorf("record does not mark the operation trapped\n%s", lines[0])
	}
}

func TestTransactionsCommandFeeBump(t *testing.T) {
	feeSource := cliAccountAddress(t, 1)
	innerSource := cliAccountAddress(t, 33)

	wrapped := cliTransaction(9700, "bump", protocol.TransactionStatusSuccess, cliFeeBumpEnvelope(t, feeSource, innerSource), "")
	plain := cliTransaction(9701, "plain", protocol.TransactionStatusSuccess, cliUploadEnvelope(t, []byte{0x00, 0x61, 0x73, 0x6d}), "")

	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{wrapped, plain})
	withFake(t, srv)

	stdout, _, err := run(t, "transactions", "--from", "9700", "--json", "--fee-bump")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want only the fee-bump wrapper\n%s", len(lines), stdout)
	}

	var rec struct {
		FeeAccount string `json:"feeAccount"`
		FeeBump    struct {
			FeeAccount  string `json:"feeAccount"`
			InnerSource string `json:"innerSource"`
			MaxFee      int64  `json:"maxFee"`
			InnerFee    int64  `json:"innerFee"`
		} `json:"feeBump"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, lines[0])
	}
	if rec.FeeAccount != feeSource || rec.FeeBump.FeeAccount != feeSource {
		t.Errorf("fee account = %q / %q, want %q", rec.FeeAccount, rec.FeeBump.FeeAccount, feeSource)
	}
	if rec.FeeBump.InnerSource != innerSource {
		t.Errorf("inner source = %q, want %q", rec.FeeBump.InnerSource, innerSource)
	}
	if rec.FeeBump.MaxFee != 2000 || rec.FeeBump.InnerFee != 100 {
		t.Errorf("fees = (%d, %d), want (2000, 100)", rec.FeeBump.MaxFee, rec.FeeBump.InnerFee)
	}
}

func TestTransactionsCommandRejectsUnknownStatus(t *testing.T) {
	withFake(t, fakerpc.NewTransactionServer(nil))

	if _, _, err := run(t, "transactions", "--status", "pending"); err == nil {
		t.Fatal("Execute() error = nil, want a validation failure")
	}
}

func TestTransactionsCommandRejectsTrappedSuccess(t *testing.T) {
	withFake(t, fakerpc.NewTransactionServer(nil))

	if _, _, err := run(t, "transactions", "--trapped", "--status", "success"); err == nil {
		t.Fatal("Execute() error = nil, want trapped+success rejected")
	}
}
