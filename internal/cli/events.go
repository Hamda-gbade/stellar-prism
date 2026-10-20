package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// streamFlags are the options shared by every streaming command.
type streamFlags struct {
	follow   bool
	from     uint32
	limit    int
	asJSON   bool
	raw      bool
	compact  bool
	poll     time.Duration
	pageSize uint
	retries  int

	// pgDSN enables durable cursors in PostgreSQL. Empty means off, subject
	// to the environment default resolved in runStream.
	pgDSN string
	// pgCursorKey overrides the derived cursor row name.
	pgCursorKey string
	// derivedKey is computed by each command from its own query before
	// runStream, because only the command knows which filters select records.
	// See cursorKey.
	derivedKey string
}

func (s *streamFlags) register(cmd *cobra.Command, defaultPage uint) {
	cmd.Flags().BoolVarP(&s.follow, "follow", "f", false,
		"keep streaming as new ledgers close, like tail -f")
	cmd.Flags().Uint32Var(&s.from, "from", 0,
		"start from this ledger and continue forward (default: the latest ledger)")
	cmd.Flags().IntVarP(&s.limit, "limit", "n", 0,
		"stop after this many records (0 means unlimited)")
	cmd.Flags().BoolVar(&s.asJSON, "json", false,
		"emit NDJSON, one record per line")
	cmd.Flags().BoolVar(&s.raw, "raw", false,
		"leave XDR base64-encoded instead of decoding it")
	cmd.Flags().BoolVar(&s.compact, "compact", false,
		"print only the header line for each record, omitting decoded payloads")
	cmd.Flags().DurationVar(&s.poll, "poll", stream.DefaultPollInterval,
		"how long to wait between polls once caught up")
	cmd.Flags().UintVar(&s.pageSize, "page-size", defaultPage,
		"how many records to request per RPC call")
	cmd.Flags().IntVar(&s.retries, "max-retries", 0,
		"give up after this many consecutive RPC failures (0 means never give up)")
	cmd.Flags().StringVar(&s.pgDSN, "pg-dsn", "",
		"PostgreSQL DSN for a durable cursor, so a restart resumes where this run stopped (default $"+envPGDSN+"; empty disables persistence)")
	cmd.Flags().StringVar(&s.pgCursorKey, "pg-cursor-key", "",
		"cursor row to save and resume under (default: <command>/<hash of --from and filters, so each distinct stream resumes itself)")
}

// newSink builds the output sink implied by the flags.
func (s *streamFlags) newSink(w io.Writer, colorMode string) stream.Sink {
	if s.asJSON {
		return sink.NewNDJSON(w, !s.raw)
	}
	return sink.NewHuman(w, lensformat.PaletteFor(w, colorMode), s.compact || s.raw)
}

// options builds the tailer options implied by the flags.
func (s *streamFlags) options(stderr io.Writer, quiet bool) stream.Options {
	return stream.Options{
		Follow:       s.follow,
		Limit:        s.limit,
		PollInterval: s.poll,
		MaxRetries:   s.retries,
		OnRetry: func(attempt int, delay time.Duration, err error) {
			if quiet {
				return
			}
			// Reconnection is reported on stderr so it never contaminates a
			// piped NDJSON stream, and so a stalled tail explains itself
			// rather than appearing to hang.
			fmt.Fprintf(stderr, "prism: RPC call failed (%v); retrying in %s (attempt %d)\n",
				err, delay.Round(time.Millisecond), attempt)
		},
	}
}

func newEventsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf          streamFlags
		contractIDs []string
		topics      []string
		eventTypes  []string
	)

	cmd := &cobra.Command{
		Use:   "events",
		Short: "Stream Soroban contract events",
		Long: `Stream Soroban contract events, decoded as they arrive.

Without --follow this drains what the server already has and exits, which is
useful in a pipeline. With --follow it keeps going, reconnecting through
dropped connections without losing or repeating events.

Topic filters are evaluated by the server and use its wildcard syntax: "*"
matches exactly one segment, and "**" matches zero or more and must come
last. The semantics are identical to ` + "`stellar events --topic`" + `.

Events are retained by RPC for roughly 24 hours, so --from cannot reach
further back than that. If the stream ever falls behind that window, prism
reports the missed ledger range and exits 2 rather than skipping silently.

With --pg-dsn the position is committed to PostgreSQL after every record, so
an interrupted run resumes where it stopped instead of starting over.`,
		Example: `  # Follow one contract
  prism events --contract CBGSB... --follow

  # Filter by topic, taking the last 20 and exiting
  prism events --contract CBGSB... --topic 'AAAAAQ==,*' --limit 20

  # NDJSON into jq
  prism events --contract CBGSB... --follow --json | jq -r '.contract'

  # Backfill from a ledger then keep following
  prism events --from 1234567 --follow

  # Resume across restarts with a durable cursor in PostgreSQL
  prism events --contract CBGSB... --follow --pg-dsn "$PRISM_PG_DSN"`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client := newClient(rpc.ResolveURL(g.rpcURL), g.timeout)

			from := sf.from
			if from == 0 {
				// Starting "now" means asking the server where now is; the
				// RPC requires a concrete start ledger.
				latest, err := latestLedger(cmd.Context(), client)
				if err != nil {
					return err
				}
				from = latest
			}

			src, err := stream.NewEventSource(client, stream.EventQuery{
				ContractIDs: contractIDs,
				Topics:      topics,
				EventTypes:  eventTypes,
				StartLedger: from,
				PageLimit:   sf.pageSize,
			})
			if err != nil {
				return err
			}

			// The key names this exact query: same filters, same stream, and
			// therefore the same durable position on a later run.
			parts := []string{fromKeyPart(sf.from)}
			for _, c := range contractIDs {
				parts = append(parts, "contract="+c)
			}
			for _, topic := range topics {
				parts = append(parts, "topic="+topic)
			}
			for _, et := range eventTypes {
				parts = append(parts, "type="+et)
			}
			sf.derivedKey = cursorKey(cmd.Name(), parts...)

			return runStream(cmd.Context(), src, &sf, g, stdout, stderr)
		},
	}

	cmd.Flags().StringSliceVar(&contractIDs, "contract", nil,
		"contract ID to watch; repeatable, up to 5")
	// StringArray, not StringSlice: a comma separates segments *within* one
	// topic filter, so comma-splitting the flag would turn a single
	// two-segment filter into two unrelated single-segment ones.
	cmd.Flags().StringArrayVar(&topics, "topic", nil,
		`topic filter as comma-separated base64 ScVals, with "*" and "**" wildcards; repeatable, up to 5`)
	cmd.Flags().StringSliceVar(&eventTypes, "type", nil,
		"event type to include: contract, system or diagnostic (default: all)")
	sf.register(cmd, stream.DefaultPageLimit)

	return cmd
}

func newLedgersCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags

	cmd := &cobra.Command{
		Use:   "ledgers",
		Short: "Stream ledger close metadata",
		Long: `Stream ledger close metadata as ledgers close.

Ledger metadata is large, so --compact is often what you want when watching
for liveness rather than reading contents.

With --pg-dsn the position is committed to PostgreSQL as the stream advances,
so an interrupted run resumes where it stopped.`,
		Example: `  prism ledgers --follow --compact
  prism ledgers --from 1234567 --limit 5 --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client := newClient(rpc.ResolveURL(g.rpcURL), g.timeout)

			from := sf.from
			if from == 0 {
				latest, err := latestLedger(cmd.Context(), client)
				if err != nil {
					return err
				}
				from = latest
			}

			src := stream.NewLedgerSource(client, stream.LedgerQuery{
				StartLedger: from,
				PageLimit:   sf.pageSize,
			})
			sf.derivedKey = cursorKey(cmd.Name(), fromKeyPart(sf.from))
			return runStream(cmd.Context(), src, &sf, g, stdout, stderr)
		},
	}

	sf.register(cmd, stream.DefaultLedgerPageLimit)
	return cmd
}

// runStream wires a source to a sink and runs the tailer until it finishes or
// the operator interrupts it.
func runStream(
	ctx context.Context,
	src stream.Source,
	sf *streamFlags,
	g *globalFlags,
	stdout, stderr io.Writer,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Ctrl-C ends the stream cleanly rather than killing it mid-record, so a
	// partially written line never reaches a downstream consumer.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	out := sf.newSink(stdout, g.color)
	opts := sf.options(stderr, sf.asJSON && stderr == nil)

	// Durable persistence is opt-in: only a DSN (flag or environment) opens
	// anything, and without one no connection is ever attempted.
	key := sf.derivedKey
	if sf.pgCursorKey != "" {
		key = sf.pgCursorKey
	}
	dsn := sf.pgDSN
	if dsn == "" {
		dsn = os.Getenv(envPGDSN)
	}
	store, closeStore, err := openCursorStore(ctx, dsn)
	if err != nil {
		return err
	}
	if closeStore != nil {
		defer func() {
			if closeErr := closeStore(); closeErr != nil {
				fmt.Fprintf(stderr, "prism: closing the PostgreSQL cursor store: %v\n", closeErr)
			}
		}()
	}
	opts.Store = store
	opts.StoreKey = key

	tailer := stream.NewTailer(src, out, opts)

	err = tailer.Run(ctx)

	var gap *stream.GapError
	if errors.As(err, &gap) {
		fmt.Fprintf(stderr, "prism: %v\n", gap)
		fmt.Fprintf(stderr,
			"prism: restart with --from %d to resume from the oldest data the server still has\n",
			gap.Oldest)
		if store != nil {
			// A saved position behind retention would gap again on every
			// restart, so say what to do about the row rather than leaving a
			// trap.
			fmt.Fprintf(stderr,
				"prism: that starts a fresh stream with a new cursor row; the stale row for %q can be deleted\n",
				key)
		}
		return &exitError{code: 2, msg: gap.Error()}
	}
	return err
}

// latestLedger asks the server where the head is, so a stream with no --from
// can start at "now".
func latestLedger(ctx context.Context, client rpc.Client) (uint32, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	resp, err := client.GetLatestLedger(ctx)
	if err != nil {
		return 0, fmt.Errorf("determining the latest ledger: %w", err)
	}
	if resp.Sequence == 0 {
		return 0, fmt.Errorf("server did not report a latest ledger; pass --from to choose a start point")
	}
	return resp.Sequence, nil
}
