package sink

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestConflictOutputPreservesMemoAndClassification(t *testing.T) {
	r := memoRecord(t, xdr.MemoText("payment"))
	r.Transaction.Status = stream.TxStatusSuccess
	r.Transaction.Conflicts = []stream.FootprintConflict{{KeyXDR: "key", Transactions: []string{"a", "b"}}}
	for _, decode := range []bool{false, true} {
		var out bytes.Buffer
		if err := NewNDJSON(&out, decode).Emit(r); err != nil {
			t.Fatal(err)
		}
		var got struct {
			Memo      stream.SanitizedMemo `json:"memo"`
			Status    string               `json:"status"`
			Conflicts []conflictLine       `json:"conflicts"`
		}
		if err := json.Unmarshal(out.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.Memo.Value != "payment" || got.Status != stream.TxStatusSuccess || len(got.Conflicts) != 1 {
			t.Fatalf("missing combined transaction fields: %s", out.String())
		}
	}
	for _, compact := range []bool{false, true} {
		var out bytes.Buffer
		if err := NewHuman(&out, lensformat.NoColor, compact).Emit(r); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "conflict") || !strings.Contains(out.String(), `memo text: "payment"`) {
			t.Fatalf("missing conflict or memo: %s", out.String())
		}
	}
}
