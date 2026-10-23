package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// healthFlags are the options specific to the health command.
type healthFlags struct {
	follow       bool
	asJSON       bool
	interval     time.Duration
	timeout      time.Duration
	maxLedgerAge time.Duration
	failAfter    int
}

func (h *healthFlags) register(cmd *cobra.Command) {
	cmd.Flags().BoolVarP(&h.follow, "follow", "f", false,
		"keep checking on an interval instead of exiting after one probe")
	cmd.Flags().BoolVar(&h.asJSON, "json", false,
		"emit one JSON object per check, one per line")
	cmd.Flags().DurationVar(&h.interval, "interval", rpc.DefaultHealthCheckInterval,
		"how long to wait between checks with --follow")
	cmd.Flags().DurationVar(&h.timeout, "health-timeout", rpc.DefaultHealthCheckTimeout,
		"timeout for a single getHealth request")
	cmd.Flags().DurationVar(&h.maxLedgerAge, "max-ledger-age", rpc.DefaultMaxLedgerAge,
		"treat the endpoint as unhealthy when its latest ledger is older than this")
	cmd.Flags().IntVar(&h.failAfter, "fail-after", 0,
		"with --follow, exit after this many consecutive unhealthy checks (0 keeps watching)")
}

func (h *healthFlags) validate() error {
	if h.interval <= 0 {
		return fmt.Errorf("--interval must be greater than zero")
	}
	if h.timeout <= 0 {
		return fmt.Errorf("--health-timeout must be greater than zero")
	}
	if h.maxLedgerAge <= 0 {
		return fmt.Errorf("--max-ledger-age must be greater than zero")
	}
	if h.failAfter < 0 {
		return fmt.Errorf("--fail-after must not be negative")
	}
	return nil
}

// healthState is written by the periodic observer and read after the checker
// has joined, so the mutex documents the only cross-goroutine data.
type healthState struct {
	mu       sync.Mutex
	failures int
	writeErr error
}

func newHealthCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var hf healthFlags

	cmd := &cobra.Command{
		Use:   "health",
		Short: "Check RPC health and ledger latency",
		Long: `Probe a Soroban RPC endpoint with getHealth and report its ledger latency.

One probe runs by default, so the command is safe in a cron job or a CI gate:
it exits 1 when the endpoint is not healthy or its latest ledger is older than
--max-ledger-age.

With --follow it checks every --interval until interrupted. A healthy result
resets the consecutive-failure count; --fail-after N makes the command exit 1
after N unhealthy checks in a row, while the default of 0 keeps watching.

The probe is a single getHealth call. No other endpoint is contacted, and the
command makes no RPC calls at all when validation fails.`,
		Example: `  # One probe, human-readable output
  prism health

  # A CI gate against testnet
  prism health --json

  # Watch a load-balanced endpoint, failing after three bad probes
  prism health --rpc-url $RPC_A --rpc-url $RPC_B \
    --follow --interval 10s --fail-after 3`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := hf.validate(); err != nil {
				return err
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			healthClient, ok := client.(rpc.HealthClient)
			if !ok {
				return fmt.Errorf("%w: %T", rpc.ErrHealthUnsupported, client)
			}

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			options := []rpc.HealthOption{
				rpc.WithHealthInterval(hf.interval),
				rpc.WithHealthTimeout(hf.timeout),
				rpc.WithMaxLedgerAge(hf.maxLedgerAge),
			}

			if !hf.follow {
				checker, err := rpc.NewHealthChecker(healthClient, options...)
				if err != nil {
					return err
				}
				report := checker.Check(ctx)
				if err := writeHealthReport(stdout, report, hf.asJSON); err != nil {
					return err
				}
				if !report.Healthy {
					message := "health check failed"
					if report.Err != nil {
						message = report.Err.Error()
					}
					return &ExitCodeError{Code: 1, Message: message}
				}
				return nil
			}

			// Ctrl-C ends the watch loop cleanly. The checker is stopped and
			// joined below, so an interrupt cannot leave a probe running.
			ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			state := &healthState{}
			options = append(options, rpc.WithHealthObserver(func(report rpc.HealthReport) {
				if err := writeHealthReport(stdout, report, hf.asJSON); err != nil {
					state.mu.Lock()
					if state.writeErr == nil {
						state.writeErr = err
					}
					state.mu.Unlock()
					cancel()
					return
				}

				state.mu.Lock()
				if report.Healthy {
					state.failures = 0
				} else {
					state.failures++
				}
				enough := hf.failAfter > 0 && state.failures >= hf.failAfter
				state.mu.Unlock()

				if enough {
					cancel()
				}
			}))

			checker, err := rpc.NewHealthChecker(healthClient, options...)
			if err != nil {
				return err
			}
			if err := checker.Run(runCtx); err != nil {
				return err
			}

			state.mu.Lock()
			defer state.mu.Unlock()
			if state.writeErr != nil {
				return state.writeErr
			}
			if hf.failAfter > 0 && state.failures >= hf.failAfter {
				return &ExitCodeError{
					Code: 1,
					Message: fmt.Sprintf("health check failed %d consecutive times",
						state.failures),
				}
			}
			return nil
		},
	}

	hf.register(cmd)
	return cmd
}

// healthReportJSON is the stable NDJSON shape of a health check. Durations are
// rendered in both a human-readable string and milliseconds, so a jq filter
// can compare without parsing Go's duration syntax.
type healthReportJSON struct {
	CheckedAt             string `json:"checkedAt"`
	Healthy               bool   `json:"healthy"`
	Status                string `json:"status,omitempty"`
	LatestLedger          uint32 `json:"latestLedger"`
	OldestLedger          uint32 `json:"oldestLedger"`
	LatestLedgerCloseTime string `json:"latestLedgerCloseTime,omitempty"`
	OldestLedgerCloseTime string `json:"oldestLedgerCloseTime,omitempty"`
	LedgerRetentionWindow uint32 `json:"ledgerRetentionWindow"`
	LedgerLatency         string `json:"ledgerLatency"`
	LedgerLatencyMS       int64  `json:"ledgerLatencyMs"`
	ProbeLatency          string `json:"probeLatency"`
	ProbeLatencyMS        int64  `json:"probeLatencyMs"`
	ConsecutiveFailures   int    `json:"consecutiveFailures"`
	Error                 string `json:"error,omitempty"`
}

func writeHealthReport(w io.Writer, report rpc.HealthReport, asJSON bool) error {
	if asJSON {
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(newHealthReportJSON(report)); err != nil {
			return fmt.Errorf("writing health report: %w", err)
		}
		return nil
	}
	if _, err := fmt.Fprintln(w, formatHealthReport(report)); err != nil {
		return fmt.Errorf("writing health report: %w", err)
	}
	return nil
}

func newHealthReportJSON(report rpc.HealthReport) healthReportJSON {
	status := report.Status
	if status == "" {
		status = rpc.HealthStatusUnknown
	}
	return healthReportJSON{
		CheckedAt:             report.CheckedAt.UTC().Format(time.RFC3339),
		Healthy:               report.Healthy,
		Status:                status,
		LatestLedger:          report.LatestLedger,
		OldestLedger:          report.OldestLedger,
		LatestLedgerCloseTime: formatOptionalTime(report.LatestLedgerCloseTime),
		OldestLedgerCloseTime: formatOptionalTime(report.OldestLedgerCloseTime),
		LedgerRetentionWindow: report.LedgerRetentionWindow,
		LedgerLatency:         report.LedgerLatency.Round(time.Millisecond).String(),
		LedgerLatencyMS:       report.LedgerLatency.Milliseconds(),
		ProbeLatency:          report.ProbeLatency.Round(time.Millisecond).String(),
		ProbeLatencyMS:        report.ProbeLatency.Milliseconds(),
		ConsecutiveFailures:   report.ConsecutiveFailures,
		Error:                 healthError(report),
	}
}

func formatHealthReport(report rpc.HealthReport) string {
	status := "healthy"
	if !report.Healthy {
		status = "unhealthy"
	}
	line := fmt.Sprintf("%s  %-9s  status=%s  latest=%d  oldest=%d  ledger-latency=%s  probe-latency=%s",
		report.CheckedAt.UTC().Format(time.RFC3339),
		status,
		displayHealthStatus(report.Status),
		report.LatestLedger,
		report.OldestLedger,
		report.LedgerLatency.Round(time.Millisecond),
		report.ProbeLatency.Round(time.Millisecond),
	)
	if report.LedgerRetentionWindow > 0 {
		line += fmt.Sprintf("  retention=%d", report.LedgerRetentionWindow)
	}
	if report.Err != nil {
		line += fmt.Sprintf("  error=%q", report.Err)
	}
	return line
}

func displayHealthStatus(status string) string {
	if status == "" {
		return rpc.HealthStatusUnknown
	}
	return status
}

func formatOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func healthError(report rpc.HealthReport) string {
	if report.Err == nil {
		return ""
	}
	return report.Err.Error()
}
