package stream

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/go-stellar-sdk/toid"
)

// DefaultTransactionPageLimit is the page size requested for transaction streams.
const DefaultTransactionPageLimit = 100

// TransactionQuery describes which transactions to stream.
type TransactionQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
}

// TransactionSource streams transactions from RPC.
type TransactionSource struct {
	client rpc.Client
	query  TransactionQuery
}

// NewTransactionSource returns a Source over the given RPC client.
func NewTransactionSource(client rpc.Client, q TransactionQuery) *TransactionSource {
	if q.PageLimit == 0 {
		q.PageLimit = DefaultTransactionPageLimit
	}
	return &TransactionSource{client: client, query: q}
}

// Name implements Source.
func (s *TransactionSource) Name() string { return "transactions" }

// Rewind implements Rewinder, repositioning a fresh fetch at ledger.
func (s *TransactionSource) Rewind(ledger uint32) { s.query.StartLedger = ledger }

// CursorLedger implements Source.
// The RPC cursor format is "%019d-%010d" (toid-encoded + event order).
// We need to extract the ledger from the first part to enable gap detection.
func (s *TransactionSource) CursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}
	// Try parsing as a full event cursor (has the form ledger-tx-op-event)
	if parts := strings.Split(cursor, "-"); len(parts) >= 2 {
		// First part may be toid-encoded; parse it and extract the ledger
		// sequence. Legacy transaction cursors carry the ledger directly
		// ("%010d-%06d"), so fall back to the plain value when the TOID
		// payload has no ledger in it.
		idInt, err := strconv.ParseInt(parts[0], 10, 64)
		if err == nil {
			if parsed := toid.Parse(idInt); parsed.LedgerSequence > 0 {
				return uint32(parsed.LedgerSequence), true
			}
			if n, err := strconv.ParseUint(parts[0], 10, 32); err == nil {
				return uint32(n), true
			}
		}
		// Fall back: try the second part as a plain ledger number.
		if n, err := strconv.ParseUint(parts[1], 10, 32); err == nil {
			return uint32(n), true
		}
		return 0, false
	}
	// No dash in cursor; try parsing as a plain decimal ledger.
	n, err := strconv.ParseUint(cursor, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

// Fetch implements Source.
func (s *TransactionSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	req := protocol.GetTransactionsRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: s.query.PageLimit,
		},
	}
	if cursor == "" {
		req.StartLedger = s.query.StartLedger
	} else {
		req.Pagination.Cursor = cursor
	}

	resp, err := s.client.GetTransactions(ctx, req)
	if err != nil {
		return Batch{}, err
	}

	batch := Batch{
		Records:    make([]Record, 0, len(resp.Transactions)),
		NextCursor: resp.Cursor,
		Latest:     resp.LatestLedger,
		Oldest:     resp.OldestLedger,
		Full:       uint(len(resp.Transactions)) >= s.query.PageLimit,
	}
	for _, tx := range resp.Transactions {
		batch.Records = append(batch.Records, transactionRecord(tx))
	}
	return batch, nil
}

// transactionRecord converts an RPC transaction into a neutral Record.
func transactionRecord(tx protocol.TransactionInfo) Record {
	return Record{
		ID:       fmt.Sprintf("%010d-%04d", tx.Ledger, tx.ApplicationOrder),
		Sequence: tx.Ledger,
		Time:     time.Unix(tx.LedgerCloseTime, 0).UTC(),
		Kind:     KindTransaction,
		Transaction: &Transaction{
			Hash:          tx.TransactionHash,
			Index:         uint32(tx.ApplicationOrder),
			Successful:    tx.Status == protocol.TransactionStatusSuccess,
			EnvelopeXDR:   tx.EnvelopeXDR,
			ResultXDR:     tx.ResultXDR,
			ResultMetaXDR: tx.ResultMetaXDR,
		},
	}
}
