package sink

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func memoRecord(t *testing.T, memo xdr.Memo) stream.Record {
	t.Helper()
	e := xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{SourceAccount: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"), Memo: memo}}}
	payload, err := xdr.MarshalBase64(e)
	if err != nil {
		t.Fatal(err)
	}
	return stream.Record{ID: "0000000010-0001", Kind: stream.KindTransaction, Sequence: 10, Transaction: &stream.Transaction{EnvelopeXDR: payload}}
}
func TestMemoHumanSanitizesDecodedPayloadAndSummary(t *testing.T) {
	r := memoRecord(t, xdr.MemoText("hello\x00\x1b[2J\u202eé😀"))
	for _, compact := range []bool{false, true} {
		var b bytes.Buffer
		if err := NewHuman(&b, lensformat.NoColor, compact).Emit(r); err != nil {
			t.Fatal(err)
		}
		got := b.String()
		if strings.ContainsAny(got, "\x00\x1b\u202eé😀") {
			t.Fatalf("unsafe terminal output %q", got)
		}
		if !strings.Contains(got, `\u00e9`) || !strings.Contains(got, `\U0001f600`) {
			t.Fatalf("missing escaped memo %q", got)
		}
	}
}
func TestMemoJSONBinaryAndUnicode(t *testing.T) {
	hash := xdr.Hash{}
	for i := range hash {
		hash[i] = 255
	}
	for _, m := range []xdr.Memo{xdr.MemoText("hello\x00é"), xdr.MemoHash(hash), xdr.MemoRetHash(hash)} {
		for _, decode := range []bool{false, true} {
			var b bytes.Buffer
			if err := NewNDJSON(&b, decode).Emit(memoRecord(t, m)); err != nil {
				t.Fatal(err)
			}
			if !utf8.Valid(b.Bytes()) || bytes.Count(b.Bytes(), []byte("\n")) != 1 {
				t.Fatalf("invalid NDJSON %q", b.Bytes())
			}
			var got struct {
				Memo stream.SanitizedMemo `json:"memo"`
			}
			if err := json.Unmarshal(b.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			want := stream.SanitizeMemo(m)
			if got.Memo != want {
				t.Fatalf("got %+v, want %+v", got.Memo, want)
			}
			if decode && bytes.Contains(b.Bytes(), []byte(`\u0000`)) {
				t.Fatalf("decoded memo not sanitized: %s", b.Bytes())
			}
		}
	}
}
func TestConsensusJSONAndHuman(t *testing.T) {
	delta := int64(11)
	r := stream.Record{Kind: stream.KindLedger, ID: "0000000010", Close: &stream.LedgerClose{Consensus: &stream.ConsensusMetrics{CloseDeltaSeconds: &delta, ValueSigners: []string{}, Warnings: []string{"consensus latency spike"}}}}
	var b bytes.Buffer
	if err := NewNDJSON(&b, false).Emit(r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), `"closeDeltaSeconds":11`) || !strings.Contains(b.String(), `"quorumDataAvailable":false`) {
		t.Fatal(b.String())
	}
	b.Reset()
	if err := NewHuman(&b, lensformat.NoColor, true).Emit(r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "close delta: 11s") || !strings.Contains(b.String(), "warning: consensus latency spike") {
		t.Fatal(b.String())
	}
}
