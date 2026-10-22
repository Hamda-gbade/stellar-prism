package stream

import (
	"encoding/hex"
	"fmt"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ConsensusMetrics describes the evidence available in a ledger header. Headers
// contain a value signer, not the SCP envelopes or quorum sets needed to prove
// validator participation or quorum health. Signatures here are not verified.
type ConsensusMetrics struct {
	CloseTime           uint64   `json:"closeTime"`
	CloseDeltaSeconds   *int64   `json:"closeDeltaSeconds,omitempty"`
	TxSetHash           string   `json:"txSetHash"`
	ValueType           int32    `json:"valueType"`
	ValueSigners        []string `json:"valueSigners"`
	SignaturePresent    bool     `json:"signaturePresent"`
	QuorumDataAvailable bool     `json:"quorumDataAvailable"`
	Warnings            []string `json:"warnings,omitempty"`
	Error               string   `json:"error,omitempty"`
}

func consensusMetrics(payload string) *ConsensusMetrics {
	m := &ConsensusMetrics{ValueSigners: []string{}}
	var h xdr.LedgerHeaderHistoryEntry
	if err := xdr.SafeUnmarshalBase64(payload, &h); err != nil {
		m.Error = fmt.Sprintf("decode ledger header: %v", err)
		return m
	}
	v := h.Header.ScpValue
	m.CloseTime = uint64(v.CloseTime)
	m.TxSetHash = hex.EncodeToString(v.TxSetHash[:])
	m.ValueType = int32(v.Ext.V)
	sig := v.Ext.LcValueSignature
	if v.Ext.ProposedValue != nil {
		sig = &v.Ext.ProposedValue.LcValueSignature
	}
	if sig != nil && sig.NodeId.Ed25519 != nil && len(sig.Signature) > 0 {
		address, err := strkey.Encode(strkey.VersionByteAccountID, sig.NodeId.Ed25519[:])
		if err == nil {
			m.ValueSigners = append(m.ValueSigners, address)
			m.SignaturePresent = true
		}
	}
	if !m.SignaturePresent {
		m.Warnings = append(m.Warnings, "ledger value has no signer signature")
	}
	return m
}
