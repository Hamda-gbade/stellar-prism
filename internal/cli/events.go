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

	// Sink selection. When either destination is set, records are written
	// there instead of stdout.
	timescaleDSN       string
	timescaleTable     string
	parquetDir         string
	parquetPrefix      string
	parquetCompression string
	batchSize          int
	flushInterval      time.Duration
}

// newTimescaleSink and newParquetSink are variables so tests can substitute a
// fake database, exactly like newClient. Production code never reassigns them.
var (
	newTimescaleSink = func(ctx context.Context, dsn string, cfg sink.TimescaleConfig) (stream.Sink, error) {
		return sink.NewTimescale(ctx, dsn, cfg)
	}
	newParquetSink = func(ctx context.Context, cfg sink.ParquetConfig) (stream.Sink, error) {
		return sink.NewParquet(ctx, cfg)
	}
)

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

	cmd.Flags().StringVar(&s.timescaleDSN, "timescale-dsn", "",
		"write records to PostgreSQL/TimescaleDB at this connection string instead of stdout")
	cmd.Flags().StringVar(&s.timescaleTable, "timescale-table", sink.DefaultTimescaleTable,
		"destination table for --timescale-dsn, optionally schema-qualified")
	cmd.Flags().StringVar(&s.parquetDir, "parquet-dir", "",
		"write records as Parquet part files in this directory instead of stdout")
	cmd.Flags().StringVar(&s.parquetPrefix, "parquet-prefix", sink.DefaultParquetPrefix,
		"file name prefix for Parquet parts (written as PREFIX-000001.parquet)")
	cmd.Flags().StringVar(&s.parquetCompression, "parquet-compression", sink.DefaultParquetCompression,
		"Parquet codec: zstd, snappy, gzip or uncompressed")
	cmd.Flags().IntVar(&s.batchSize, "batch-size", 0,
		"records per database transaction or Parquet part file (0 uses the sink default)")
	cmd.Flags().DurationVar(&s.flushInterval, "flush-interval", 0,
		"flush a partial batch after this long (0 uses the sink default)")
}

// validate rejects sink combinations that would silently discard output.
// It runs before any RPC call so a typo costs no network round trip.
func (s *streamFlags) validate() error {
	if s.timescaleDSN != "" && s.parquetDir != "" {
		return fmt.Errorf("--timescale-dsn and --parquet-dir cannot be used together")
	}
	if s.timescaleDSN == "" && s.parquetDir == "" {
		return nil
	}
	if s.asJSON || s.raw || s.compact {
		return fmt.Errorf(
			"--json, --raw and --compact describe stdout output and cannot be combined with a database or Parquet sink")
	}
	if s.batchSize < 0 {
		return fmt.Errorf("--batch-size must not be negative, got %d", s.batchSize)
	}
	if s.flushInterval < 0 {
		return fmt.Errorf("--flush-interval must not be negative, got %s", s.flushInterval)
	}
	return nil
}

// newSink builds the output sink implied by the flags.
//
// A database or Parquet destination takes precedence over stdout, and is
// rejected alongside the stdout-only flags by validate.
func (s *streamFlags) newSink(ctx context.Context, w io.Writer, colorMode string) (stream.Sink, error) {
	switch {
	case s.timescaleDSN != "":
		return newTimescaleSink(ctx, s.timescaleDSN, sink.TimescaleConfig{
			Table:         s.timescaleTable,
			BatchSize:     s.batchSize,
			FlushInterval: s.flushInterval,
		})
	case s.parquetDir != "":
		return newParquetSink(ctx, sink.ParquetConfig{
			Dir:           s.parquetDir,
			Prefix:        s.parquetPrefix,
			Compression:   s.parquetCompression,
			BatchSize:     s.batchSize,
			FlushInterval: s.flushInterval,
		})
	case s.asJSON:
		return sink.NewNDJSON(w, !s.raw), nil
	default:
		return sink.NewHuman(w, lensformat.PaletteFor(w, colorMode), s.compact || s.raw), nil
	}
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
reports the missed ledger range and exits 2 rather than skipping silently.`,
		Example: `  # Follow one contract
  prism events --contract CBGSB... --follow

  # Filter by topic, taking the last 20 and exiting
  prism events --contract CBGSB... --topic 'AAAAAQ==,*' --limit 20

  # NDJSON into jq
  prism events --contract CBGSB... --follow --json | jq -r '.contract'

  # Backfill from a ledger then keep following
  prism events --from 1234567 --follow`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(); err != nil {
				return err
			}
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
for liveness rather than reading contents.`,
		Example: `  prism ledgers --follow --compact
  prism ledgers --from 1234567 --limit 5 --json`,
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

			src := stream.NewLedgerSource(client, stream.LedgerQuery{
				StartLedger: from,
				PageLimit:   sf.pageSize,
			})
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

	out, err := sf.newSink(ctx, stdout, g.color)
	if err != nil {
		return err
	}
	tailer := stream.NewTailer(src, out, sf.options(stderr, sf.asJSON && stderr == nil))

	err = tailer.Run(ctx)

	// Batch sinks hold data until they are closed. Close explicitly so the
	// final part file or transaction is written before the process exits, and
	// so the sink's background goroutine stops.
	if closer, ok := out.(io.Closer); ok {
		if closeErr := closer.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing the sink: %w", closeErr)
		}
	}

	var gap *stream.GapError
	if errors.As(err, &gap) {
		fmt.Fprintf(stderr, "prism: %v\n", gap)
		fmt.Fprintf(stderr,
			"prism: restart with --from %d to resume from the oldest data the server still has\n",
			gap.Oldest)
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
