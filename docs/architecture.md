# Architecture

How prism fits together, and where to make the two changes contributors most
often want to make:

- [Adding a stream source](#adding-a-stream-source)
- [Adding an output sink](#adding-an-output-sink)

## The shape of the problem

Soroban RPC is **poll-only**. There is no subscription method, no websocket,
no server push. So "streaming" means polling on a cursor, and every hard part
of this project follows from that:

- a dropped connection must not skip records;
- a server that replays a page must not produce duplicates;
- a stream that falls behind retention must not silently lose data;
- a slow consumer must not cause unbounded memory growth.

Everything below is in service of those four.

## The pipeline

```
        ┌──────────┐   Batch    ┌────────┐   Record   ┌──────┐
RPC ───►│  Source  ├───────────►│ Tailer ├───────────►│ Sink ├───► stdout,
        └──────────┘            └────────┘            └──────┘     --timescale-dsn,
         events.go               tail.go               ndjson.go   --parquet-dir
         ledgers.go            cursor, dedup,          human.go
      transactions.go          backoff, gaps           (via lens)
                                                       timescale.go
                                                       parquet.go
```

One goroutine, no channels. The tailer fetches a page, emits its records
synchronously, advances the cursor, and repeats. Concurrency would buy nothing
— the bottleneck is the network — and would cost determinism in tests.

## Package layout

```
cmd/prism/            main; maps errors to exit codes
internal/cli/         cobra commands — thin wrappers over pkg/stream
internal/fakerpc/     scriptable in-memory RPC for tests
pkg/rpc/              the four-method Client interface + SDK adapter
pkg/stream/
  record.go           Record: the neutral item every source produces
  source.go           Source and Sink interfaces, GapError   ← the seams
  tail.go             the state machine
  events.go           contract events source
  ledgers.go          ledger close source
  clock.go            Clock and Backoff, both injectable
pkg/sink/
  ndjson.go           NDJSON, one object per line
  human.go            terminal output, payloads via lens
  timescale.go        PostgreSQL/TimescaleDB batch insertion
  parquet.go          DuckDB-encoded Parquet part files
```

## The state machine

```go
cursor = ""            // empty means "start where the Source was configured"
lastID = ""            // high-water mark

for {
    batch, err := fetchWithRetry(cursor)   // retries the SAME cursor
    if gap(cursor, batch.Oldest) { return GapError }

    for r := range batch.Records {
        if r.ID <= lastID { skip }          // replay or reorder
        sink.Emit(r); lastID = r.ID
    }

    cursor = batch.NextCursor               // advance only after a clean emit
    if batch.Full { continue }              // more waiting; drain at network speed
    if !follow { return }
    clock.Sleep(pollInterval)
}
```

Four details carry the weight:

**1. Retry never advances the cursor.** `fetchWithRetry` loops on the same
cursor value. This is why a dropped connection cannot cause a gap, and it is
why `Source.Fetch` must be idempotent — a Source that mutated internal state
on a failed call would break the guarantee.

**2. IDs are fixed-width and zero-padded.** RPC event IDs look like
`0017037362169118720-0000000000`, so *lexicographic order equals chronological
order*. De-duplication is therefore one string comparison against the
high-water mark, with no parsing and no per-ID bookkeeping. `LedgerSource`
preserves the property by synthesising `%010d` from the sequence. **A new
Source must preserve it too** — this is the single most important contract in
the codebase.

**3. The cursor advances only after every record in a page is emitted.** If a
sink fails mid-page the tailer returns with the cursor untouched, so a caller
that retries resumes from before the failure rather than past it.

**4. `Full` drives backfill speed.** A page that hit the server's limit means
more data is already waiting, so the tailer refetches immediately instead of
sleeping. Backfilling a day of history runs at network speed; a caught-up
stream polls once per ledger close.

### Cursors and the mutual-exclusion rule

The RPC rejects a request carrying both a ledger range and a cursor
(`"ledger ranges and cursor cannot both be set"`). So the first `Fetch` uses
`StartLedger` and every subsequent one uses the cursor alone. The fake
enforces the same rule, so a regression here fails in tests rather than only
in production.

### Gap detection

Records can be lost exactly one way: falling behind the RPC's retention window
(roughly 24 hours of events). The server reports `OldestLedger` on every
response, so the condition is directly observable:

```go
if cursorLedger < batch.Oldest { return &GapError{...} }
```

prism reports the missed range and exits 2. It does **not** silently resume
from the oldest available ledger — a stream with an unannounced hole is worse
than one that stops, especially for anything auditing contract activity.

### Backpressure

`Sink.Emit` is synchronous. A slow consumer (`prism … | jq | something-slow`)
blocks the write, which blocks the poll loop. That is correct backpressure
with no queue to grow. It is also why the line-oriented sinks flush per
record: a tail that buffers looks like a tail that has hung.

## Sinks that store rather than print

`NDJSON` and `Human` are line-oriented: a record is visible the moment `Emit`
returns. The two storage sinks batch instead, because a round trip (or a file
rename) per record would be absurd for a backfill. Both keep the same seam and
the same delivery contract; the difference is that records are only durable
once a batch is flushed.

Both sinks own a background goroutine that flushes a partial batch every
`--flush-interval`, and both implement `io.Closer`. The CLI closes whichever
sink it built after the tailer returns — including on the gap error path — so
the last records reach disk before the process exits and no goroutine outlives
the run. `Flush` remains what the tailer calls; `Close` is the stronger command
that also stops the timer and releases the driver.

### TimescaleDB (`--timescale-dsn`)

Rows land in one wide table, `prism_records`, with the neutral `Record` fields
plus kind-specific columns left `NULL` where they do not apply, so a query can
tell "no transaction here" from "a transaction that failed":

```sql
CREATE TABLE prism_records (
  id TEXT NOT NULL, ledger BIGINT NOT NULL, kind TEXT NOT NULL,
  closed_at TIMESTAMPTZ, hash TEXT, contract_id TEXT, tx_hash TEXT,
  tx_index INTEGER, op_index INTEGER, successful BOOLEAN,
  topics_xdr TEXT[], value_xdr TEXT, envelope_xdr TEXT,
  result_xdr TEXT, result_meta_xdr TEXT, header_xdr TEXT, metadata_xdr TEXT,
  PRIMARY KEY (id, ledger)
);
```

The table is promoted to a hypertable partitioned by **ledger** — not by time —
with a chunk every 50,000 ledgers. The ledger sequence is always present, is the
natural dimension for chain data, and does not require inventing a timestamp for
a record the server did not date. On a server without the extension the
`create_hypertable` call is recognised as "TimescaleDB is not installed" and the
sink keeps working against the plain table; anything else is reported.

`COPY` cannot express `ON CONFLICT`, so a batch lands in a `pg_temp` table and
is merged with `INSERT … ON CONFLICT DO NOTHING` inside one transaction. That
matters for the delivery contract: a restart re-emits the last record or two by
design, and the primary key would otherwise turn a legitimate replay into a
fatal duplicate-key error. A failed batch therefore inserts nothing and stays
buffered for retry, while a replayed batch is a no-op.

### Parquet (`--parquet-dir`)

An embedded DuckDB instance is used only as the Parquet encoder. Rows are staged
in an in-memory table and copied out with `COPY (SELECT … ORDER BY id) TO …, so
each flush produces one part file:

```
<dir>/part-000001.parquet
<dir>/part-000002.parquet
```

Part files are written under a `.tmp` name and renamed, so an interrupted COPY
cannot leave a truncated file that a later read would trust. Numbering resumes
after the highest part already in the directory, so a restarted backfill appends
rather than overwrites. The whole archive is one glob away:

```sql
SELECT kind, count(*) FROM read_parquet('parts/*.parquet') GROUP BY kind;
```

Batching bounds memory: the staging table never holds more than one part, so a
multi-day backfill does not grow with the stream.

### What is deliberately *not* handled

**Chain reorganisations.** Stellar ledgers are final on close — SCP gives
immediate finality, and there is no reorg. Code to handle rewritten history
would be untestable and would imply a hazard that does not exist. The
realistic adjacent failure is a load-balanced RPC pool serving from a lagging
backend, which reports an *older* `LatestLedger`; that case is tested.

## Testing the guarantee

`internal/fakerpc` is a scriptable in-memory RPC. Tests describe failures
(`{2: {Err: ErrDropped}}`, `{1: {ReplayLast: true}}`) rather than assembling
responses by hand, and the fake serves from a fixed universe using real cursor
semantics, so a test cannot accidentally pass against behaviour the real
server would never produce.

Every scenario funnels through one helper asserting three invariants:

1. **IDs strictly increasing** — ordering survived.
2. **No duplicates** — de-duplication worked.
3. **Set equality with the expected range** — *this is what proves no gap.*
   Counting records would not.

Covered: clean streams, drops at every position, consecutive drops, replayed
pages, out-of-order pages, retention overrun, lagging backends, permanent
errors, retry exhaustion, cancellation mid-follow, sink failure, and
full-page draining.

Time is injected via `Clock`, so there are no real sleeps and no wall-clock
flakiness. Follow-loop tests carry a sleep budget that cancels the context, so
a regression that stops the tailer terminating **fails** rather than hanging
CI — a distinction worth the few extra lines.

### Mutation-checked

The invariants were verified by breaking the code on purpose:

| Mutation | Result |
|---|---|
| De-duplication disabled | `TestTailSuppressesReplayedPages` and `TestTailRejectsOutOfOrderPages` fail with exact diagnostics |
| Gap detection disabled | `TestTailDetectsRetentionGap` fails in 0.00s |

If you change the state machine, re-run that exercise. A test suite that
cannot fail is not protecting anything.

## Relationship to stellar-xdr-lens

prism does **no XDR decoding of its own**. Every payload goes through
[`lens`](https://github.com/stellar-optics/stellar-xdr-lens):

- `sink/human.go` renders payloads with lens's `TreeFormatter`, so an ScVal in
  a prism stream is identical to the same ScVal under `lens decode`. Only the
  header line is prism's.
- `sink/ndjson.go` embeds lens's `{type, value}` envelope, so `jq` expressions
  transfer between the two tools.
- `cli/tx.go` is a thin wrapper over `lens.ExplainPair`.
- Even topic filters are validated by decoding through `lens.DecodeAs`, so a
  malformed segment produces lens's error wording.

Shared CLI conventions: `--json`, `--color auto|always|never`, and the same
exit-code meanings.

## Adding a stream source

Implement `stream.Source`:

```go
type Source interface {
    Name() string
    Fetch(ctx context.Context, cursor string) (Batch, error)
    CursorLedger(cursor string) (uint32, bool)
}
```

Three rules, all load-bearing:

1. Records in a `Batch` are in ascending ID order.
2. `Record.ID` is fixed-width and zero-padded — see above.
3. `Fetch` is idempotent for a given cursor, because retries reuse it.

Return `Oldest` from whatever the server reports, or gap detection silently
becomes a no-op for your source. Then wire it into a command; the tailer and
every sink work unchanged. `ledgers.go` is about 100 lines and is the shortest
worked example.

## Adding an output sink

Implement `stream.Sink`:

```go
type Sink interface {
    Emit(r Record) error
    Flush() error
}
```

Flush per record if the sink is for live viewing. A batching sink should also
implement `io.Closer`, stop and wait for any goroutine it started, and flush
whatever is buffered there; the CLI closes the sink it built. Decode XDR through
`lens` rather than reaching for `xdr` directly, so output stays consistent with
the rest of the family — and report an undecodable payload inline rather than
returning an error, so one malformed value cannot kill a long-running tail.

A sink that persists records has to decide what a replay means. prism does not
offer exactly-once across restarts, so the destination should treat a repeated
record as a no-op — the Timescale sink's staged `ON CONFLICT DO NOTHING` is the
worked example.

## Adding an RPC method

`pkg/rpc.Client` has four methods. Every one added must also be implemented in
`internal/fakerpc`, so prefer composing the existing calls. The interface is
small on purpose: it is the boundary that keeps the entire streaming core
testable without a network.
