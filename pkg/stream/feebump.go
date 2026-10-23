package stream

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// FeeBump classifies a FeeBumpTransaction: the wrapper that lets a second
// account pay the fee for someone else's transaction.
//
// The wrapper and the inner transaction are two different transactions with
// two different fee bids, signature sets and source accounts. Keeping them
// separate on the record is what lets a consumer answer "who paid?" without
// re-decoding the envelope, and what keeps the effective payer from being
// mistaken for the transaction's author.
type FeeBump struct {
	// FeeAccount is the strkey account that paid the wrapper's fee.
	FeeAccount string
	// InnerSource is the strkey source account of the inner transaction,
	// which is the account whose sequence number advanced.
	InnerSource string
	// InnerHash is the hex hash of the inner transaction, when the result
	// reported it.
	InnerHash string
	// MaxFee is the wrapper's fee bid in stroops.
	MaxFee int64
	// InnerFee is the inner transaction's fee bid in stroops.
	InnerFee int64
	// FeeCharged is the fee actually taken, from the result.
	FeeCharged int64
	// InnerOperations counts the operations in the inner transaction.
	InnerOperations int
	// OuterSignatures counts the signatures on the wrapper.
	OuterSignatures int
	// InnerSignatures counts the signatures on the inner transaction.
	InnerSignatures int
}

// FeeBumpFilter accepts only transactions wrapped in a fee-bump envelope.
func FeeBumpFilter() TransactionFilter {
	return func(tx *Transaction) bool {
		return tx != nil && tx.FeeBump != nil
	}
}

// FeeAccountFilter accepts transactions whose effective fee payer matches one
// of accounts.
//
// Both G and M addresses are accepted. A muxed address is matched by its
// underlying account, because the fee payer on the ledger is the account, not
// the multiplexed identity.
func FeeAccountFilter(accounts []string) (TransactionFilter, error) {
	if len(accounts) == 0 {
		return nil, fmt.Errorf("fee account filter: at least one account is required")
	}

	wanted := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		normalized, err := normalizeAccount(account)
		if err != nil {
			return nil, fmt.Errorf("fee account filter: %w", err)
		}
		wanted[normalized] = struct{}{}
	}
	if len(wanted) == 0 {
		return nil, fmt.Errorf("fee account filter: at least one account is required")
	}

	return func(tx *Transaction) bool {
		if tx == nil {
			return false
		}
		_, ok := wanted[tx.FeeAccount]
		return ok
	}, nil
}

// normalizeAccount validates an account address and reduces a muxed address to
// the G-address of the account that actually holds the balance.
func normalizeAccount(account string) (string, error) {
	account = strings.TrimSpace(account)
	if account == "" {
		return "", fmt.Errorf("account address is empty")
	}

	if _, err := strkey.Decode(strkey.VersionByteAccountID, account); err == nil {
		return account, nil
	}

	raw, err := strkey.Decode(strkey.VersionByteMuxedAccount, account)
	if err != nil {
		return "", fmt.Errorf("%q is not a Stellar account address", account)
	}
	if len(raw) < 32 {
		return "", fmt.Errorf("%q is not a Stellar account address", account)
	}
	encoded, err := strkey.Encode(strkey.VersionByteAccountID, raw[:32])
	if err != nil {
		return "", fmt.Errorf("normalizing muxed account %q: %w", account, err)
	}
	return encoded, nil
}

// classifyFeeBump builds the classification for a fee-bump envelope, or
// returns nil for an ordinary one.
//
// It is called by EnrichTransaction after both XDR values have decoded, so it
// does no decoding of its own and returns no error: anything that would fail
// here has already failed earlier in that path.
func classifyFeeBump(env *xdr.TransactionEnvelope, result *xdr.TransactionResult) *FeeBump {
	if env == nil || !env.IsFeeBump() || env.FeeBump == nil {
		return nil
	}

	bump := env.FeeBump
	info := &FeeBump{
		FeeAccount:      bump.Tx.FeeSource.ToAccountId().Address(),
		MaxFee:          int64(bump.Tx.Fee),
		OuterSignatures: len(bump.Signatures),
	}

	inner := bump.Tx.InnerTx.V1
	if inner == nil {
		// The union only has the v1 arm today. A future envelope version
		// would leave the inner details unknown rather than misreport them.
		return info
	}
	info.InnerSource = inner.Tx.SourceAccount.ToAccountId().Address()
	info.InnerFee = int64(inner.Tx.Fee)
	info.InnerOperations = len(inner.Tx.Operations)
	info.InnerSignatures = len(inner.Signatures)

	if result != nil {
		info.FeeCharged = int64(result.FeeCharged)
		if pair, ok := result.Result.GetInnerResultPair(); ok {
			info.InnerHash = hex.EncodeToString(pair.TransactionHash[:])
		}
	}
	return info
}

// FeeBumpQuery describes a stream of fee-bumped transactions.
type FeeBumpQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
	// FeeAccounts limits results to transactions whose fee was paid by one
	// of these accounts. Empty means any payer.
	FeeAccounts []string
}

// FeeBumpSource is the fee-bump view of a transaction stream.
type FeeBumpSource = TransactionFilterSource

// NewFeeBumpSource returns a Source emitting only fee-bumped transactions,
// classified with the wrapper and inner transaction details.
func NewFeeBumpSource(client rpc.Client, q FeeBumpQuery) (*FeeBumpSource, error) {
	filters := []TransactionFilter{FeeBumpFilter()}
	if len(q.FeeAccounts) > 0 {
		byAccount, err := FeeAccountFilter(q.FeeAccounts)
		if err != nil {
			return nil, fmt.Errorf("fee bump source: %w", err)
		}
		filters = append(filters, byAccount)
	}

	src, err := NewTransactionFilterSource(client, TransactionFilterQuery{
		StartLedger: q.StartLedger,
		PageLimit:   q.PageLimit,
		Filters:     filters,
	})
	if err != nil {
		return nil, fmt.Errorf("fee bump source: %w", err)
	}
	return src, nil
}
