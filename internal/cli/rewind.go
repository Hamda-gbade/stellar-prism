package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// newRewindCmd safely repositions a simple state-file cursor for replay.
func newRewindCmd(stdout io.Writer) *cobra.Command {
	var path string
	var ledger uint32
	var back uint32
	cmd := &cobra.Command{
		Use:   "rewind",
		Short: "Rewind a state file so a stream can replay records",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if path == "" {
				return fmt.Errorf("--state-file is required")
			}
			if (ledger == 0) == (back == 0) {
				return fmt.Errorf("pass exactly one of --to-ledger or --back")
			}
			state, err := stream.OpenStateFile(path)
			if err != nil {
				return err
			}
			if back > 0 {
				current, err := state.Load()
				if err != nil {
					return err
				}
				if current.Ledger <= back {
					return fmt.Errorf("--back %d reaches before the first ledger", back)
				}
				ledger = current.Ledger - back
			}
			backup, err := state.Rewind(ledger)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(stdout, "rewound state to ledger %d; backup: %s\n", ledger, backup)
			return err
		},
	}
	cmd.Flags().StringVar(&path, "state-file", "", "state file to rewind")
	cmd.Flags().Uint32Var(&ledger, "to-ledger", 0, "ledger to replay from")
	cmd.Flags().Uint32Var(&back, "back", 0, "number of ledgers to rewind")
	return cmd
}
