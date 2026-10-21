package stream

import "sort"

// ReorderBuffer holds the records belonging to the active ledger until a
// record from a later ledger proves that the active ledger is complete.
//
// RPC replicas can return pages with a small amount of intra-ledger skew. A
// high-water ID is therefore not safe to apply until the ledger has closed;
// keeping only that one ledger bounded the memory cost while allowing a
// deterministic sort by ledger, transaction, operation and event position.
type ReorderBuffer struct {
	pending []Record
	seen    map[string]struct{}
}

// NewReorderBuffer returns an empty sliding ledger reorder buffer.
func NewReorderBuffer() *ReorderBuffer {
	return &ReorderBuffer{seen: make(map[string]struct{})}
}

// Add accepts records and returns records from ledgers that are now known to
// be complete. Records from the highest ledger remain buffered.
func (b *ReorderBuffer) Add(records []Record) []Record {
	if b.seen == nil {
		b.seen = make(map[string]struct{})
	}
	maxLedger := uint32(0)
	haveLedger := false
	for _, r := range b.pending {
		if !haveLedger || r.Sequence > maxLedger {
			maxLedger = r.Sequence
			haveLedger = true
		}
	}
	for _, r := range records {
		// An absent ID cannot identify a replay. Leave content-based
		// deduplication to downstream sinks in that case.
		if r.ID != "" {
			if _, ok := b.seen[r.ID]; ok {
				continue
			}
			b.seen[r.ID] = struct{}{}
		}
		b.pending = append(b.pending, r)
		if !haveLedger || r.Sequence > maxLedger {
			maxLedger = r.Sequence
			haveLedger = true
		}
	}
	if !haveLedger {
		return nil
	}

	ready := make([]Record, 0, len(b.pending))
	keep := b.pending[:0]
	for _, r := range b.pending {
		if r.Sequence < maxLedger {
			ready = append(ready, r)
			delete(b.seen, r.ID)
		} else {
			keep = append(keep, r)
		}
	}
	b.pending = keep
	sortRecords(ready)
	return ready
}

// Flush returns all records still held by the active ledger, in stream order.
func (b *ReorderBuffer) Flush() []Record {
	ready := append([]Record(nil), b.pending...)
	b.pending = b.pending[:0]
	for _, r := range ready {
		delete(b.seen, r.ID)
	}
	sortRecords(ready)
	return ready
}

func sortRecords(records []Record) {
	sort.SliceStable(records, func(i, j int) bool {
		a, b := records[i], records[j]
		if a.Sequence != b.Sequence {
			return a.Sequence < b.Sequence
		}
		if a.Kind == KindEvent && b.Kind == KindEvent && a.Event != nil && b.Event != nil {
			if a.Event.TxIndex != b.Event.TxIndex {
				return a.Event.TxIndex < b.Event.TxIndex
			}
			if a.Event.OpIndex != b.Event.OpIndex {
				return a.Event.OpIndex < b.Event.OpIndex
			}
			if a.Event.EventIndex != b.Event.EventIndex {
				return a.Event.EventIndex < b.Event.EventIndex
			}
		}
		if a.Kind == KindOperation && b.Kind == KindOperation && a.Operation != nil && b.Operation != nil {
			if a.Operation.TxIndex != b.Operation.TxIndex {
				return a.Operation.TxIndex < b.Operation.TxIndex
			}
			return a.Operation.Index < b.Operation.Index
		}
		if a.Kind == KindTransaction && b.Kind == KindTransaction && a.Transaction != nil && b.Transaction != nil && a.Transaction.Index != b.Transaction.Index {
			return a.Transaction.Index < b.Transaction.Index
		}
		return a.ID < b.ID
	})
}
