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
RPC ───►│  Source  ├───────────►│ Tailer ├───────────►│ Sink ├───► stdout
        └──────────┘            └────────┘            └──────┘
         events.go               tail.go               ndjson.go
         ledgers.go            cursor, dedup,          human.go
                               backoff, gaps            (via lens)
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

### Durable cursors

By default a cursor lives only in the process, so restarting a stream starts it
over from `StartLedger`. `--pg-dsn` moves that position into PostgreSQL, making
a restart resume where the last run stopped:

```go
prism events tail --start-ledger 50000000 \
    --pg-dsn "$PRISM_PG_DSN" --pg-cursor-key events/tendermint
```

With no `--pg-cursor-key`, prism derives one from the command and its tagged
arguments (`events/tendermint` + start ledger → `events/<sha256-prefix>`), so
two differently-configured streams sharing a database never share a row. Two
runs pointed at the *same* key are two writers on one cursor; see the
mutual-exclusion caveat below.

The table is created on first use:

```sql
CREATE TABLE IF NOT EXISTS "prism_cursors" (
    name       TEXT        PRIMARY KEY,
    cursor     TEXT        NOT NULL,
    last_id    TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
)
```

`EnsureSchema` is a no-op when it already exists, so a role without `CREATE`
can be given a pre-created table instead.

**Why `SELECT … FOR UPDATE`.** Two processes writing one key must not
interleave, or the cursor can move *backwards* and a later restart replays
records that were already emitted. `PGCursorStore.Save` therefore takes the
row's lock before writing:

```sql
SELECT name FROM prism_cursors WHERE name = $1 FOR UPDATE  -- the lock
INSERT … ON CONFLICT (name) DO NOTHING                     -- only if absent
UPDATE prism_cursors SET cursor = $2, … WHERE name = $1    -- the write
```

The lock is held to commit, so concurrent writers queue instead of racing.
Waiting is bounded by a context deadline (`DefaultCursorLockTimeout`, 5s): a
peer holding the lock for longer than that means something is wrong, and the
save fails loudly rather than stalling a stream that looks hung.

**Cost.** The tailer writes once per emitted record, so steady-state traffic is
one round trip plus one small indexed row update per record — cheap next to the
RPC fetch that produced it. Backfilling a large range at full speed does one
write per record rather than per page; if that ever matters, the seam is the
`CursorStore` interface, and batching per page is a bounded change.

**What this does not give you.** Durable position is not exactly-once. A crash
between emitting a record and persisting it replays that record, which is why
de-duplication still runs after a restore — and why the `LastID` high-water mark
is stored alongside the cursor. A second caveat: two processes writing one key
take turns rather than merging, so run one tailer per key. `Save` deliberately
does *not* detect a position moving backwards: a rewind is indistinguishable
legitimately (an operator re-pointing a key) from a bug, and silently refusing
to move would hide the operator's intent. Run one tailer per key and the
question does not arise.

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
with no queue to grow. It is also why both sinks flush per record: a tail that
buffers looks like a tail that has hung.

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

Flush per record if the sink is for live viewing. Decode XDR through `lens`
rather than reaching for `xdr` directly, so output stays consistent with the
rest of the family — and report an undecodable payload inline rather than
returning an error, so one malformed value cannot kill a long-running tail.

## Adding an RPC method

`pkg/rpc.Client` has four methods. Every one added must also be implemented in
`internal/fakerpc`, so prefer composing the existing calls. The interface is
small on purpose: it is the boundary that keeps the entire streaming core
testable without a network.
