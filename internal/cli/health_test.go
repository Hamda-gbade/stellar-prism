package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

func healthyHealthResponse(age time.Duration) protocol.GetHealthResponse {
	return protocol.GetHealthResponse{
		Status:                rpc.HealthStatusHealthy,
		LatestLedger:          500,
		LatestLedgerCloseTime: time.Now().Add(-age).Unix(),
		OldestLedger:          490,
		OldestLedgerCloseTime: time.Now().Add(-time.Hour).Unix(),
		LedgerRetentionWindow: 10,
	}
}

func TestHealthCommandHealthyJSON(t *testing.T) {
	srv := fakerpc.NewServer(nil, fakerpc.WithHealthResponse(healthyHealthResponse(time.Second)))
	withFake(t, srv)

	stdout, _, err := run(t, "health", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d JSON lines, want 1\n%s", len(lines), stdout)
	}

	var got healthReportJSON
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, lines[0])
	}
	if !got.Healthy {
		t.Error("healthy endpoint was reported as unhealthy")
	}
	if got.Status != rpc.HealthStatusHealthy {
		t.Errorf("status = %q, want %q", got.Status, rpc.HealthStatusHealthy)
	}
	if got.LatestLedger != 500 || got.OldestLedger != 490 {
		t.Errorf("ledgers = %d/%d, want 500/490", got.LatestLedger, got.OldestLedger)
	}
	if got.LedgerLatency == "" || got.ProbeLatency == "" {
		t.Errorf("latency metrics missing: %+v", got)
	}
	if got.Error != "" {
		t.Errorf("healthy report has error %q", got.Error)
	}
	if srv.HealthCalls() != 1 {
		t.Errorf("getHealth calls = %d, want 1", srv.HealthCalls())
	}
}

func TestHealthCommandHumanOutput(t *testing.T) {
	srv := fakerpc.NewServer(nil, fakerpc.WithHealthResponse(healthyHealthResponse(time.Second)))
	withFake(t, srv)

	stdout, _, err := run(t, "health", "--color", "never")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{"healthy", "latest=500", "ledger-latency=", "probe-latency="} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output does not contain %q\n%s", want, stdout)
		}
	}
}

func TestHealthCommandUnhealthyExitsOne(t *testing.T) {
	tests := []struct {
		name        string
		response    protocol.GetHealthResponse
		healthErr   error
		wantInError string
	}{
		{
			name: "server reports unhealthy",
			response: protocol.GetHealthResponse{
				Status:                rpc.HealthStatusUnhealthy,
				LatestLedger:          500,
				LatestLedgerCloseTime: time.Now().Unix(),
			},
			wantInError: "unhealthy",
		},
		{
			name:        "stale latest ledger",
			response:    healthyHealthResponse(5 * time.Minute),
			wantInError: "stale",
		},
		{
			name:        "getHealth transport failure",
			healthErr:   fakerpc.ErrDropped,
			wantInError: "health check failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakerpc.NewServer(nil,
				fakerpc.WithHealthResponse(tc.response),
				fakerpc.WithHealthError(tc.healthErr),
			)
			withFake(t, srv)

			stdout, _, err := run(t, "health", "--json")
			if err == nil {
				t.Fatal("Execute() error = nil, want an unhealthy result")
			}
			if code, printable := ExitCode(err); code != 1 || printable {
				t.Errorf("ExitCode() = %d,%v, want 1,false", code, printable)
			}
			if !strings.Contains(stdout, `"healthy":false`) {
				t.Errorf("output does not report unhealthy\n%s", stdout)
			}
			if !strings.Contains(strings.ToLower(stdout), tc.wantInError) {
				t.Errorf("output does not mention %q\n%s", tc.wantInError, stdout)
			}
		})
	}
}

func TestHealthCommandFollowFailsAfterConsecutiveFailures(t *testing.T) {
	srv := fakerpc.NewServer(nil, fakerpc.WithHealthScript([]fakerpc.HealthBehaviour{
		{Response: healthyHealthResponse(time.Second)},
		{Response: protocol.GetHealthResponse{
			Status:                rpc.HealthStatusUnhealthy,
			LatestLedger:          500,
			LatestLedgerCloseTime: time.Now().Unix(),
		}},
		{Response: protocol.GetHealthResponse{
			Status:                rpc.HealthStatusUnhealthy,
			LatestLedger:          500,
			LatestLedgerCloseTime: time.Now().Unix(),
		}},
	}))
	withFake(t, srv)

	var stdout, stderr bytes.Buffer
	cmd := newHealthCmd(&globalFlags{}, &stdout, &stderr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--follow", "--interval", "5ms", "--fail-after", "2", "--json"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute() error = nil, want a failure after two unhealthy checks")
	}
	if code, _ := ExitCode(err); code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}

	lines := nonEmptyLines(stdout.String())
	if len(lines) != 3 {
		t.Fatalf("got %d check lines, want 3\n%s", len(lines), stdout.String())
	}
	if !strings.Contains(lines[0], `"healthy":true`) {
		t.Errorf("first check is not healthy: %s", lines[0])
	}
	for i, line := range lines[1:] {
		if !strings.Contains(line, `"healthy":false`) {
			t.Errorf("check %d is not unhealthy: %s", i+2, line)
		}
	}
}

func TestHealthCommandFollowStopsCleanlyOnCancellation(t *testing.T) {
	srv := fakerpc.NewServer(nil,
		fakerpc.WithHealthResponse(healthyHealthResponse(time.Second)))
	withFake(t, srv)

	var stdout, stderr bytes.Buffer
	cmd := newHealthCmd(&globalFlags{}, &stdout, &stderr)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--follow", "--interval", "5ms", "--json"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v, want a clean cancellation", err)
	}
	if got := len(nonEmptyLines(stdout.String())); got == 0 {
		t.Error("no health reports were emitted before cancellation")
	}
}

func TestHealthCommandValidatesFlagsBeforeNetwork(t *testing.T) {
	srv := fakerpc.NewServer(nil, fakerpc.WithHealthResponse(healthyHealthResponse(time.Second)))
	withFake(t, srv)

	tests := []struct {
		name string
		args []string
	}{
		{name: "zero interval", args: []string{"health", "--interval", "0"}},
		{name: "negative timeout", args: []string{"health", "--health-timeout", "-1s"}},
		{name: "zero max ledger age", args: []string{"health", "--max-ledger-age", "0"}},
		{name: "negative fail-after", args: []string{"health", "--fail-after", "-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
		})
	}
	if srv.HealthCalls() != 0 {
		t.Errorf("getHealth calls = %d, want 0 after validation failures", srv.HealthCalls())
	}
}

// noHealthClient implements rpc.Client without the optional HealthClient
// capability, which is the shape of a third-party or test client.
type noHealthClient struct {
	rpc.Client
}

func TestHealthCommandRejectsClientWithoutCapability(t *testing.T) {
	original := newClient
	newClient = func([]string, time.Duration) (rpc.Client, error) {
		return &noHealthClient{}, nil
	}
	t.Cleanup(func() { newClient = original })

	_, _, err := run(t, "health")
	if !errors.Is(err, rpc.ErrHealthUnsupported) {
		t.Fatalf("Execute() error = %v, want ErrHealthUnsupported", err)
	}
}

func TestHealthHelpAdvertisesFlags(t *testing.T) {
	stdout, _, err := run(t, "health", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{
		"--follow", "--interval", "--health-timeout", "--max-ledger-age",
		"--fail-after", "--json", "getHealth",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("help does not mention %q\n%s", want, stdout)
		}
	}
}
