package stream_test

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"unicode/utf8"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestSanitizeMemo(t *testing.T) {
	hash := xdr.Hash{}
	for i := range hash {
		hash[i] = byte(255 - i)
	}
	tests := []struct {
		name       string
		memo       xdr.Memo
		typ, value string
	}{
		{"none", xdr.Memo{}, "none", ""},
		{"text", xdr.MemoText("a\x00\n\r\t\x1b[2J\u009b\u202ebé😀"), "text", "a[2Jbé😀"},
		{"invalid utf8", xdr.MemoText("x\xffy"), "text", "xy"},
		{"id", xdr.MemoID(^uint64(0)), "id", "18446744073709551615"},
		{"hash", xdr.MemoHash(hash), "hash", hex.EncodeToString(hash[:])},
		{"return", xdr.MemoRetHash(hash), "return", hex.EncodeToString(hash[:])},
		{"missing arm", xdr.Memo{Type: xdr.MemoTypeMemoText}, "invalid", ""},
		{"unknown", xdr.Memo{Type: 99}, "invalid", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stream.SanitizeMemo(tt.memo)
			if got.Type != tt.typ || got.Value != tt.value {
				t.Fatalf("got %+v, want %s %q", got, tt.typ, tt.value)
			}
			human := stream.HumanMemo(got)
			for _, r := range human {
				if r < 32 || r > 126 {
					t.Fatalf("unsafe human memo %q", human)
				}
			}
		})
	}
}
func TestDecodeMemoEnvelopes(t *testing.T) {
	v1 := v1Envelope()
	v1.V1.Tx.Memo = xdr.MemoText("hello\x00é")
	v0 := xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTxV0, V0: &xdr.TransactionV0Envelope{Tx: xdr.TransactionV0{Memo: xdr.MemoText("hello\x00é")}}}
	fee := xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump, FeeBump: &xdr.FeeBumpTransactionEnvelope{Tx: xdr.FeeBumpTransaction{FeeSource: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"), InnerTx: xdr.FeeBumpTransactionInnerTx{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: v1.V1}}}}
	for _, e := range []xdr.TransactionEnvelope{v1, v0, fee} {
		got, err := stream.DecodeMemo(encodeEnvelope(t, e))
		if err != nil || got.Value != "helloé" {
			t.Fatalf("got %+v, %v", got, err)
		}
	}
	if _, err := stream.DecodeMemo("not xdr"); err == nil {
		t.Fatal("accepted malformed envelope")
	}
}
func TestMemoFilterPreservesPageCursor(t *testing.T) {
	var txs []protocol.TransactionInfo
	for i, m := range []string{"skip", "pay\x00:123", "pay:456"} {
		e := v1Envelope()
		e.V1.Tx.Memo = xdr.MemoText(m)
		txs = append(txs, protocol.TransactionInfo{TransactionDetails: protocol.TransactionDetails{Ledger: 10, ApplicationOrder: int32(i + 1), EnvelopeXDR: encodeEnvelope(t, e)}})
	}
	srv := fakerpc.NewTransactionServer(txs)
	src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 10, PageLimit: 3, Memo: "pay:"})
	b, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Records) != 2 || !b.Full || b.NextCursor == "" {
		t.Fatalf("bad filtered page: %+v", b)
	}
	for _, r := range b.Records {
		if !strings.HasPrefix(r.Transaction.Memo.Value, "pay:") {
			t.Fatal(r.Transaction.Memo)
		}
	}
}
func FuzzSanitizeMemo(f *testing.F) {
	f.Add("\x1b]52;c;ZXZpbA==\a\x00\u202ehello")
	f.Fuzz(func(t *testing.T, s string) {
		m := stream.SanitizeMemo(xdr.MemoText(s))
		if !utf8.ValidString(m.Value) {
			t.Fatal("invalid utf8")
		}
		if stream.SanitizeText(m.Value) != m.Value {
			t.Fatal("not idempotent")
		}
		for _, r := range stream.HumanMemo(m) {
			if r < 32 || r > 126 {
				t.Fatal("unsafe terminal rune")
			}
		}
	})
}
