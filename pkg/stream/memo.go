package stream

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// SanitizedMemo is a decoded memo. Binary values use lowercase hexadecimal.
type SanitizedMemo struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// SanitizeText removes terminal controls, invalid UTF-8 and invisible format
// characters (including bidi overrides). Printable Unicode is preserved for JSON.
func SanitizeText(s string) string {
	return strings.Map(func(r rune) rune {
		if !unicode.IsPrint(r) {
			return -1
		}
		return r
	}, strings.ToValidUTF8(s, ""))
}

// SanitizeMemo safely decodes every Stellar memo arm, including empty memos.
func SanitizeMemo(m xdr.Memo) SanitizedMemo {
	switch m.Type {
	case xdr.MemoTypeMemoNone:
		return SanitizedMemo{Type: "none"}
	case xdr.MemoTypeMemoText:
		if m.Text != nil {
			return SanitizedMemo{Type: "text", Value: SanitizeText(*m.Text)}
		}
	case xdr.MemoTypeMemoId:
		if m.Id != nil {
			return SanitizedMemo{Type: "id", Value: strconv.FormatUint(uint64(*m.Id), 10)}
		}
	case xdr.MemoTypeMemoHash:
		if m.Hash != nil {
			return SanitizedMemo{Type: "hash", Value: hex.EncodeToString(m.Hash[:])}
		}
	case xdr.MemoTypeMemoReturn:
		if m.RetHash != nil {
			return SanitizedMemo{Type: "return", Value: hex.EncodeToString(m.RetHash[:])}
		}
	}
	return SanitizedMemo{Type: "invalid"}
}

// DecodeMemo decodes a transaction envelope without trusting its union arms.
func DecodeMemo(payload string) (SanitizedMemo, error) {
	var e xdr.TransactionEnvelope
	if err := xdr.SafeUnmarshalBase64(payload, &e); err != nil {
		return SanitizedMemo{}, fmt.Errorf("decode memo: %w", err)
	}
	if _, err := envelopeOperations(&e); err != nil {
		return SanitizedMemo{}, err
	}
	return SanitizeMemo(e.Memo()), nil
}

// HumanMemo renders memo text as quoted ASCII, preventing terminal escapes and
// visually ambiguous non-ASCII characters from reaching a terminal.
func HumanMemo(m SanitizedMemo) string { return strconv.QuoteToASCII(SanitizeText(m.Value)) }
