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
  transactions.go     transaction source
  drainer.go          concurrent historical backfill + ordered merge
  batcher.go          count/time batching sink wrapper
  gap_recovery.go     GapStrategy, Rewinder and gap handling
  context.go          TaskGroup and cancellation helpers
  clock.go            Clock and Backoff, both injectable
pkg/sink/
  ndjson.go           NDJSON, one object per line
  human.go            terminal output, payloads via lens
  kafka.go            Kafka producer sink, keyed partitioning
  amqp.go             AMQP 0-9-1 sink, durable queue + confirms
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

By default prism reports the missed range and exits 2. It does **not**
silently resume from the oldest available ledger — a stream with an
unannounced hole is worse than one that stops, especially for anything
auditing contract activity.

`--gap-strategy resume` opts into a different policy. The tailer calls
`Rewind(batch.Oldest)` on sources that implement `Rewinder`, clears the
cursor, and fetches again from the oldest retained ledger. The skipped range
is still announced: it reaches `Options.OnGap`, the CLI prints it on stderr,
and `Stats.Gaps` / `Stats.Recovered` count it. `--max-gap-recoveries` bounds
how many times a stream may skip ahead. A source without `Rewinder` cannot
resume and fails exactly as it would under the default policy.

### Backpressure

`Sink.Emit` is synchronous. A slow consumer (`prism … | jq | something-slow`)
blocks the write, which blocks the poll loop. That is correct backpressure
with no queue to grow. It is also why both sinks flush per record: a tail that
buffers looks like a tail that has hung.

`--batch` changes the unit of work, not the backpressure: `Batcher` buffers
records until `--batch-size` is reached or `--batch-timeout` elapses, then
hands the whole slice to the sink. It uses a single timer, not a goroutine,
and `Close` stops the timer and flushes the remainder, so Ctrl-C cannot leave
a buffered record behind. Sinks that implement `BatchSink` (both built-ins do)
flush once per batch; other sinks are driven with one `Emit` per record.

### Parallel historical backfill

`--backfill-workers N` (with an explicit `--from`) uses `Drainer` instead of
the sequential tailer for the historical phase. It probes the source once to
learn the retention bounds, splits the ledger range into N contiguous chunks,
and gives each worker its own `Source` and cursor. Workers stop at their chunk
boundary, so no ledger is fetched twice.

Records from the workers are merged by ID through a small heap before reaching
the sink. That preserves the same ordering and de-duplication guarantee as the
sequential tailer without buffering the whole backfill in memory: at most one
page per worker is live at a time. A full page is fetched ahead by a worker
while the merge consumes the previous one, so fetching is genuinely
concurrent. After the historical phase the CLI either exits (`--follow` is
off) or starts a normal tailer at `Until()+1`.

If `--from` predates the retention window, the probe handles the gap according
to `--gap-strategy`. A gap discovered inside a worker is handled the same way,
or returned as a `*GapError` under the default policy.

### Cancellation and goroutine ownership

`TaskGroup` in `context.go` is the one place that starts concurrent workers.
The first non-cancellation error cancels every sibling; `Wait` blocks until
all workers have returned and then cancels the shared context. A caller
cancellation is a clean stop, not an error. `Drainer.Run` waits for every
worker on every path, including sink failure and limit exhaustion, so a
terminated backfill cannot leak goroutines or leave a worker writing to a
closed sink.

### Broker sinks

`--kafka-brokers`/`--kafka-topic` and `--amqp-url`/`--amqp-queue` replace
stdout with a broker. The stream, the cursor and the delivery guarantee are
unchanged: `Emit` publishes synchronously, so a broker that stops accepting
messages stops the poll loop instead of filling an in-process queue.

Both sinks publish the **same JSON document `--json` prints**, produced by
`sink.EncodeRecord`. A consumer therefore parses one stable shape regardless
of where records came from, and `--raw` still switches the body to base64 XDR.
`--batch` applies too: both sinks implement `BatchSink`, so a batch reaches
the broker as one producer call (Kafka) or one confirmed publish per record
(AMQP, which has no atomic multi-message publish).

Kafka partitions by message key, chosen with `--kafka-partition-by`:

| Value | Key | Effect |
|---|---|---|
| `contract` (default) | event contract ID, else transaction hash, else record ID | all activity for one contract is ordered on one partition |
| `tx` | transaction hash | an event and its transaction land together |
| `record` | record ID | spreads load evenly |

The key is never empty, because an empty key round-robins and would silently
break the ordering the strategy exists to provide. The production producer is
a synchronous kafka-go `Writer` with a hash balancer and `RequiredAcks=all`,
so a successful `Emit` means every in-sync replica has the message.

AMQP declares the queue **durable** (not exclusive, not auto-delete) and
publishes **persistent** messages with the record ID as `message_id`. It
enables publisher confirms and waits for the broker's ack on every publish; a
nack is a stream error, not a dropped record. A queue that already exists
with different durability flags is a startup error rather than a silent
re-declaration, since changing those flags would discard queued messages.

Both sinks open their connections only when the stream starts and implement
`io.Closer`; the CLI closes them on every exit path, including Ctrl-C. When
batching is enabled the closer flushes the buffer **before** closing the
connection, so the last partial batch cannot be lost. A publish is bounded by
a 10-second timeout, so a wedged broker fails the stream rather than hanging
it forever.

`KafkaProducer`, `AMQPPublisher` and `AMQPMessage` are the seams that keep
the suite offline. Sink tests inject recording fakes; CLI tests swap
`newKafkaProducer`/`newAMQPPublisher` and run the real flag parsing, batching
and close path against a fake RPC. Tests against real brokers live in
`internal/cli/broker_integration_test.go` and are skipped unless
`PRISM_KAFKA_BROKERS`/`PRISM_KAFKA_TOPIC` or
`PRISM_AMQP_URL`/`PRISM_AMQP_QUEUE` are set.

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
pages, out-of-order pages, retention overrun with both gap policies, lagging
backends, permanent errors, retry exhaustion, cancellation mid-follow, sink
failure, full-page draining, concurrent backfill, batcher count/time/explicit
flushes, and task-group cancellation. Gap recovery is also unit-tested
without a network through the `Rewinder` seam.

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

If the sink ships records somewhere, use `sink.EncodeRecord` for the body so
it stays byte-identical to `--json`, put the broker client behind a small
interface so tests can run without one, and implement `io.Closer` so the CLI
releases the connection on cancellation. `kafka.go` and `amqp.go` are the
worked examples.

## Adding an RPC method

`pkg/rpc.Client` has four methods. Every one added must also be implemented in
`internal/fakerpc`, so prefer composing the existing calls. The interface is
small on purpose: it is the boundary that keeps the entire streaming core
testable without a network.
