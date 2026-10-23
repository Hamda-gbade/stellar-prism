package stream

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// FootprintAnalyzer annotates transaction records with conflicts between
// Soroban read/write footprints in the same ledger. Read/read overlap is not
// a conflict; a key is reported only when at least one transaction writes it.
type FootprintAnalyzer struct{}

type footprintAccess struct {
	tx       string
	readOnly bool
	key      string
	contract string
}

// NewFootprintAnalyzer returns a stateless footprint analyzer.
func NewFootprintAnalyzer() *FootprintAnalyzer { return &FootprintAnalyzer{} }

// Analyze decodes all transaction records in each supplied ledger and adds
// symmetric conflict metadata to every transaction participating in a clash.
// The Tailer invokes this after the reorder buffer has closed a ledger, so a
// transaction cannot be emitted before a later page reveals its conflict.
func (a *FootprintAnalyzer) Analyze(records []Record) ([]Record, error) {
	out := append([]Record(nil), records...)
	byLedger := make(map[uint32][]int)
	for i := range out {
		if out[i].Kind == KindTransaction && out[i].Transaction != nil {
			byLedger[out[i].Sequence] = append(byLedger[out[i].Sequence], i)
		}
	}

	for _, indexes := range byLedger {
		accesses := make(map[string][]footprintAccess)
		for _, index := range indexes {
			tx := out[index].Transaction
			identity := transactionIdentity(out[index])
			if tx.EnvelopeXDR == "" {
				continue
			}
			var envelope xdr.TransactionEnvelope
			if err := xdr.SafeUnmarshalBase64(tx.EnvelopeXDR, &envelope); err != nil {
				return nil, fmt.Errorf("transaction %s: decoding envelope: %w", tx.Hash, err)
			}
			data, ok := sorobanData(&envelope)
			if !ok {
				continue
			}
			for _, key := range data.Resources.Footprint.ReadOnly {
				encoded, err := xdr.MarshalBase64(key)
				if err != nil {
					return nil, fmt.Errorf("transaction %s: encoding read-only footprint: %w", tx.Hash, err)
				}
				accesses[encoded] = append(accesses[encoded], footprintAccess{tx: identity, readOnly: true, key: encoded, contract: footprintContract(key)})
			}
			for _, key := range data.Resources.Footprint.ReadWrite {
				encoded, err := xdr.MarshalBase64(key)
				if err != nil {
					return nil, fmt.Errorf("transaction %s: encoding read-write footprint: %w", tx.Hash, err)
				}
				accesses[encoded] = append(accesses[encoded], footprintAccess{tx: identity, key: encoded, contract: footprintContract(key)})
			}
		}

		for key, entries := range accesses {
			if len(entries) < 2 || !hasWriteConflict(entries) {
				continue
			}
			transactions := uniqueTransactions(entries)
			if len(transactions) < 2 {
				continue
			}
			conflict := FootprintConflict{KeyXDR: key, ContractID: entries[0].contract, Transactions: transactions}
			for _, index := range indexes {
				tx := out[index].Transaction
				if !contains(transactions, tx.Hash) {
					continue
				}
				tx.Conflicts = append(tx.Conflicts, conflict)
			}
		}
	}
	return out, nil
}

func transactionIdentity(record Record) string {
	if record.Transaction != nil && record.Transaction.Hash != "" {
		return record.Transaction.Hash
	}
	return record.ID
}

func sorobanData(envelope *xdr.TransactionEnvelope) (*xdr.SorobanTransactionData, bool) {
	switch envelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		if envelope.V1 != nil && envelope.V1.Tx.Ext.V != 0 && envelope.V1.Tx.Ext.SorobanData != nil {
			return envelope.V1.Tx.Ext.SorobanData, true
		}
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		if envelope.FeeBump != nil && envelope.FeeBump.Tx.InnerTx.V1 != nil {
			tx := envelope.FeeBump.Tx.InnerTx.V1.Tx
			if tx.Ext.V != 0 && tx.Ext.SorobanData != nil {
				return tx.Ext.SorobanData, true
			}
		}
	}
	return nil, false
}

func hasWriteConflict(entries []footprintAccess) bool {
	for i := range entries {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].tx != entries[j].tx && (!entries[i].readOnly || !entries[j].readOnly) {
				return true
			}
		}
	}
	return false
}

func uniqueTransactions(entries []footprintAccess) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if _, ok := seen[entry.tx]; ok {
			continue
		}
		seen[entry.tx] = struct{}{}
		out = append(out, entry.tx)
	}
	return out
}

func footprintContract(key xdr.LedgerKey) string {
	if key.Type != xdr.LedgerEntryTypeContractData || key.ContractData == nil {
		return ""
	}
	value, err := key.ContractData.Contract.String()
	if err != nil {
		return ""
	}
	return value
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
