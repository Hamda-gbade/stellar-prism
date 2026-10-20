package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newTransactionsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags

	cmd := &cobra.Command{
		Use:   "transactions",
		Short: "Stream transactions",
		Long: `Stream transactions as ledgers close.

Transactions can be streamed continuously by using the --follow flag.

With --pg-dsn the position is committed to PostgreSQL as the stream advances,
so an interrupted run resumes where it stopped.`,
		Example: `  prism transactions --follow
  prism transactions --from 1234567 --limit 5 --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(); err != nil {
				return err
			}

			client := newClient(rpc.ResolveURL(g.rpcURL), g.timeout)

			from := sf.from
			if from == 0 {
				latest, err := latestLedger(cmd.Context(), client)
				if err != nil {
					return err
				}
				from = latest
			}

			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewTransactionSource(client, stream.TransactionQuery{
					StartLedger: startLedger,
					PageLimit:   sf.pageSize,
				}), nil
			}
			sf.derivedKey = cursorKey(cmd.Name(), fromKeyPart(sf.from))
			return runStream(cmd.Context(), newSource, from, &sf, g, stdout, stderr)
		},
	}

	sf.register(cmd, stream.DefaultTransactionPageLimit)
	return cmd
}
