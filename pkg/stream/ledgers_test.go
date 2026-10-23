package stream_test

import (
	"context"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func consensusLedger(t *testing.T, seq uint32, closeTime uint64, signed bool) protocol.LedgerInfo {
	t.Helper()
	h := xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(seq), ScpValue: xdr.StellarValue{CloseTime: xdr.TimePoint(closeTime)}}}
	if signed {
		key := xdr.Uint256{1}
		h.Header.ScpValue.Ext = xdr.StellarValueExt{V: xdr.StellarValueTypeStellarValueSigned, LcValueSignature: &xdr.LedgerCloseValueSignature{NodeId: xdr.NodeId{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &key}, Signature: make([]byte, 64)}}
	}
	encoded, err := xdr.MarshalBase64(h)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.LedgerInfo{Sequence: seq, LedgerCloseTime: int64(closeTime), LedgerHeader: encoded}
}
func TestLedgerConsensusDeltasAndRetry(t *testing.T) {
	srv := fakerpc.NewLedgerServer([]protocol.LedgerInfo{consensusLedger(t, 10, 100, true), consensusLedger(t, 11, 105, true), consensusLedger(t, 12, 116, false)})
	src := stream.NewLedgerSource(srv, stream.LedgerQuery{StartLedger: 10, PageLimit: 2, Consensus: true, ExpectedValidators: []string{"GEXPECTED"}})
	first, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	m := first.Records[0].Close.Consensus
	if m.CloseDeltaSeconds != nil || !m.SignaturePresent || len(m.ValueSigners) != 1 || m.QuorumDataAvailable {
		t.Fatalf("bad initial metrics %+v", m)
	}
	second := first.Records[1].Close.Consensus
	if second.CloseDeltaSeconds == nil || *second.CloseDeltaSeconds != 5 {
		t.Fatalf("bad delta %+v", second)
	}
	retry, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if retry.Records[1].Close.Consensus.CloseDeltaSeconds == nil || *retry.Records[1].Close.Consensus.CloseDeltaSeconds != 5 {
		t.Fatal("retry changed metrics")
	}
	next, err := src.Fetch(context.Background(), first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	m = next.Records[0].Close.Consensus
	if m.CloseDeltaSeconds == nil || *m.CloseDeltaSeconds != 11 || m.SignaturePresent || len(m.Warnings) < 3 {
		t.Fatalf("missing warnings %+v", m)
	}
	src.Rewind(12)
	rewound, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if rewound.Records[0].Close.Consensus.CloseDeltaSeconds != nil {
		t.Fatal("rewind retained old delta")
	}
}
func TestLedgerConsensusMissingHeaderAndOptIn(t *testing.T) {
	srv := fakerpc.NewLedgerServer([]protocol.LedgerInfo{{Sequence: 10, LedgerHeader: "bad"}})
	for _, enabled := range []bool{false, true} {
		src := stream.NewLedgerSource(srv, stream.LedgerQuery{StartLedger: 10, Consensus: enabled})
		b, err := src.Fetch(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		m := b.Records[0].Close.Consensus
		if !enabled && m != nil {
			t.Fatal("consensus not opt-in")
		}
		if enabled && (m == nil || !strings.Contains(m.Error, "decode ledger header")) {
			t.Fatalf("missing error %+v", m)
		}
	}
}
