package cli

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2" // reads the CLI's Parquet output back

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// fakeSinkDB stands in for PostgreSQL so the CLI wiring can be exercised end
// to end without a database.
type fakeSinkDB struct {
	mu sync.Mutex

	execs  []string
	tables []string
	rows   [][]any
}

func (f *fakeSinkDB) Exec(_ context.Context, sql string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, sql)
	return nil
}

func (f *fakeSinkDB) CopyRows(_ context.Context, table string, _ []string, rows [][]any) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tables = append(f.tables, table)
	for _, r := range rows {
		f.rows = append(f.rows, append([]any(nil), r...))
	}
	return int64(len(rows)), nil
}

// withFakeTimescale swaps the sink constructor for one over db, restoring it
// when the test finishes.
func withFakeTimescale(t *testing.T, db *fakeSinkDB) {
	t.Helper()
	original := newTimescaleSink
	newTimescaleSink = func(ctx context.Context, _ string, cfg sink.TimescaleConfig) (stream.Sink, error) {
		return sink.NewTimescaleWithDatabase(ctx, db, cfg)
	}
	t.Cleanup(func() { newTimescaleSink = original })
}

// parquetKinds reads the kind column back out of every part file in dir, which
// is how a user queries the archive.
func parquetKinds(t *testing.T, dir string) []string {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("opening duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	pattern := strings.ReplaceAll(filepath.Join(dir, "*.parquet"), "'", "''")
	rows, err := db.Query("SELECT kind FROM read_parquet('" + pattern + "') ORDER BY id")
	if err != nil {
		t.Fatalf("reading parts: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var kinds []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("scanning kind: %v", err)
		}
		kinds = append(kinds, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating parts: %v", err)
	}
	return kinds
}

// TestEventsCommandParquetSink exercises the flag the way a user would: a
// stream, a directory, and queryable Parquet at the end.
func TestEventsCommandParquetSink(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(1000, 4), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	dir := t.TempDir()
	stdout, _, err := run(t, "events", "--from", "1000", "--parquet-dir", dir,
		"--batch-size", "3", "--rpc-url", "http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if stdout != "" {
		t.Errorf("records were also written to stdout:\n%s", stdout)
	}

	parts, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatalf("globbing parts: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("wrote %d part files, want 2 for 4 records at batch size 3: %v", len(parts), parts)
	}
	kinds := parquetKinds(t, dir)
	if len(kinds) != 4 {
		t.Fatalf("read back %d rows, want 4", len(kinds))
	}
	for _, k := range kinds {
		if k != "event" {
			t.Errorf("row kind = %q, want event", k)
		}
	}
}

// TestEventsCommandTimescaleSink covers the same wiring path for the database
// sink, including the table flag.
func TestEventsCommandTimescaleSink(t *testing.T) {
	db := &fakeSinkDB{}
	withFakeTimescale(t, db)

	srv := fakerpc.NewServer(fakerpc.MakeEvents(2000, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "events", "--from", "2000",
		"--timescale-dsn", "postgres://example/db",
		"--timescale-table", "analytics.prism",
		"--rpc-url", "http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if stdout != "" {
		t.Errorf("records were also written to stdout:\n%s", stdout)
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.rows) != 3 {
		t.Fatalf("wrote %d rows, want 3", len(db.rows))
	}
	if len(db.tables) != 1 || db.tables[0] != "analytics.prism" {
		t.Errorf("copied into %v, want analytics.prism once", db.tables)
	}
	if len(db.execs) != 2 {
		t.Errorf("issued %d schema statements, want 2", len(db.execs))
	}
}

// TestEventsCommandClosesSinkOnGap proves the final batch is not lost when the
// stream ends in an error, which is the path a retention gap takes.
func TestEventsCommandClosesSinkOnGap(t *testing.T) {
	srv := fakerpc.NewServer(fakerpc.MakeEvents(5000, 40),
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 9000000},
		}),
	)
	withFake(t, srv)

	dir := t.TempDir()
	_, _, err := run(t, "events", "--from", "5000", "--parquet-dir", dir,
		"--page-size", "5", "--rpc-url", "http://127.0.0.1:1")
	if err == nil {
		t.Fatal("Execute() error = nil, want a gap failure")
	}
	if code, _ := ExitCode(err); code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}

	kinds := parquetKinds(t, dir)
	if len(kinds) != 5 {
		t.Errorf("read back %d rows after the gap, want the 5 emitted before it", len(kinds))
	}
}

func TestSinkFlagValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "two destinations",
			args: []string{"events", "--from", "1", "--timescale-dsn", "postgres://x", "--parquet-dir", "out"},
			want: "cannot be used together",
		},
		{
			name: "parquet with json",
			args: []string{"events", "--from", "1", "--parquet-dir", "out", "--json"},
			want: "stdout output",
		},
		{
			name: "timescale with raw",
			args: []string{"events", "--from", "1", "--timescale-dsn", "postgres://x", "--raw"},
			want: "stdout output",
		},
		{
			name: "negative batch",
			args: []string{"events", "--from", "1", "--parquet-dir", "out", "--batch-size", "-1"},
			want: "--batch-size",
		},
		{
			name: "negative flush interval",
			args: []string{"events", "--from", "1", "--parquet-dir", "out", "--flush-interval", "-1s"},
			want: "--flush-interval",
		},
		{
			name: "unknown compression",
			args: []string{"events", "--from", "1", "--parquet-dir", t.TempDir(), "--parquet-compression", "brotli"},
			want: "compression",
		},
		{
			name: "unusable table name",
			args: []string{"events", "--from", "1", "--timescale-dsn", "postgres://x", "--timescale-table", "a;drop"},
			want: "identifier",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The unreachable RPC proves validation happens before any call.
			args := append(tc.args, "--rpc-url", "http://127.0.0.1:1")
			_, _, err := run(t, args...)
			if err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestSinkFlagsAppearInHelp keeps the documented flags discoverable.
func TestSinkFlagsAppearInHelp(t *testing.T) {
	stdout, _, err := run(t, "events", "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{
		"--timescale-dsn", "--timescale-table",
		"--parquet-dir", "--parquet-compression",
		"--batch-size", "--flush-interval",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("help does not mention %q", want)
		}
	}
}

// TestSinkEnabledOnLedgerAndTransactionCommands checks the flags are shared
// rather than bolted onto one command.
func TestSinkEnabledOnLedgerAndTransactionCommands(t *testing.T) {
	for _, command := range []string{"ledgers", "transactions", "events"} {
		stdout, _, err := run(t, command, "--help")
		if err != nil {
			t.Fatalf("%s --help error = %v", command, err)
		}
		if !strings.Contains(stdout, "--parquet-dir") {
			t.Errorf("%s --help does not offer --parquet-dir", command)
		}
	}
}
