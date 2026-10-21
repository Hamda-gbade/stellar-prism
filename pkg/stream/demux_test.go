package stream_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// contractA and contractB are distinct strkey-shaped contract IDs the demux
// tests route between, taken from the fake server's own fixture so the
// tailer integration test reuses one universe of events.
var (
	contractA = fakerpc.MakeEvents(300, 1)[0].ContractID
	contractB = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
)

// eventRecord builds a minimal event Record routed by contract ID.
func eventRecord(id, contract string) stream.Record {
	return stream.Record{
		ID:   id,
		Kind: stream.KindEvent,
		Event: &stream.Event{
			ContractID: contract,
			TxHash:     contract, // trivially non-empty, mirrors a real event
		},
	}
}

// ledgerRecord builds a record with no event payload at all.
func ledgerRecord(id string) stream.Record {
	return stream.Record{ID: id, Kind: stream.KindLedger, Close: &stream.LedgerClose{}}
}

// TestDemuxRoutesByContract checks fan-out correctness: every contract's
// subscriber sees exactly its own records, in submission order, and nothing
// else's.
func TestDemuxRoutesByContract(t *testing.T) {
	t.Parallel()

	dm := stream.NewDemux(stream.DemuxOptions{})
	defer dm.Close() //nolint:errcheck // asserted below

	sinkA, sinkB := &collector{}, &collector{}
	if err := dm.Subscribe(contractA, sinkA); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", contractA, err)
	}
	if err := dm.Subscribe(contractB, sinkB); err != nil {
		t.Fatalf("Subscribe(%s) error = %v", contractB, err)
	}

	const perContract = 25
	for i := range perContract {
		id := fmt.Sprintf("%03d", i)
		for _, r := range []stream.Record{
			eventRecord(id, contractA),
			eventRecord(id, contractB),
		} {
			if err := dm.Submit(r); err != nil {
				t.Fatalf("Submit(%s) error = %v", r.ID, err)
			}
		}
	}

	if err := dm.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	wantA := make([]string, 0, perContract)
	wantB := make([]string, 0, perContract)
	for i := range perContract {
		id := fmt.Sprintf("%03d", i)
		wantA = append(wantA, id)
		wantB = append(wantB, id)
	}
	// Each id is duplicated across the two contracts' streams, so equality
	// per subscriber is what shows the demux split instead of shared.
	checkOrderedIDs(t, "A", sinkA.ids, wantA)
	checkOrderedIDs(t, "B", sinkB.ids, wantB)

	stats := dm.Stats()
	if stats.Submitted != int64(2*perContract) {
		t.Errorf("Stats().Submitted = %d, want %d", stats.Submitted, 2*perContract)
	}
	if stats.Unrouted != 0 {
		t.Errorf("Stats().Unrouted = %d, want 0", stats.Unrouted)
	}
}

// checkOrderedIDs asserts two ID slices are exactly equal, with diagnostics
// naming which subscriber failed.
func checkOrderedIDs(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("subscriber %s received %d records, want %d: %v", name, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("subscriber %s record %d = %q, want %q", name, i, got[i], want[i])
		}
	}
}

// TestDemuxUnroutedRecordsAreCounted checks the two ways a record can have
// nowhere to go: a non-event kind, and an event for an unsubscribed contract.
func TestDemuxUnroutedRecordsAreCounted(t *testing.T) {
	t.Parallel()

	dm := stream.NewDemux(stream.DemuxOptions{})
	defer dm.Close() //nolint:errcheck // asserted below

	if err := dm.Subscribe(contractA, &collector{}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	for i := range 5 {
		if err := dm.Submit(ledgerRecord(fmt.Sprintf("L%03d", i))); err != nil {
			t.Fatalf("Submit(ledger) error = %v", err)
		}
		if err := dm.Submit(eventRecord(fmt.Sprintf("U%03d", i), "CUNKNOWN123")); err != nil {
			t.Fatalf("Submit(unknown contract) error = %v", err)
		}
		if err := dm.Submit(eventRecord(fmt.Sprintf("R%03d", i), contractA)); err != nil {
			t.Fatalf("Submit(routed) error = %v", err)
		}
	}

	if err := dm.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	stats := dm.Stats()
	if stats.Submitted != 15 {
		t.Errorf("Stats().Submitted = %d, want 15", stats.Submitted)
	}
	if stats.Unrouted != 10 {
		t.Errorf("Stats().Unrouted = %d, want 10 (5 ledgers + 5 unknown contracts)", stats.Unrouted)
	}
}

// TestDemuxRefusals covers the programming-error surface: empty contract,
// nil sink, duplicate registration, and everything after Close.
func TestDemuxRefusals(t *testing.T) {
	t.Parallel()

	t.Run("empty contract ID", func(t *testing.T) {
		t.Parallel()
		dm := stream.NewDemux(stream.DemuxOptions{})
		defer dm.Close() //nolint:errcheck // behaviour under test is Subscribe
		if err := dm.Subscribe("", &collector{}); err == nil {
			t.Error("Subscribe(\"\") error = nil, want refusal")
		}
	})

	t.Run("nil sink", func(t *testing.T) {
		t.Parallel()
		dm := stream.NewDemux(stream.DemuxOptions{})
		defer dm.Close() //nolint:errcheck // behaviour under test is Subscribe
		if err := dm.Subscribe(contractA, nil); err == nil {
			t.Error("Subscribe(contractA, nil) error = nil, want refusal")
		}
	})

	t.Run("duplicate subscriber", func(t *testing.T) {
		t.Parallel()
		dm := stream.NewDemux(stream.DemuxOptions{})
		defer dm.Close() //nolint:errcheck // behaviour under test is Subscribe
		if err := dm.Subscribe(contractA, &collector{}); err != nil {
			t.Fatalf("first Subscribe() error = %v", err)
		}
		if err := dm.Subscribe(contractA, &collector{}); err == nil {
			t.Error("second Subscribe(contractA) error = nil, want duplicate refusal")
		}
	})

	t.Run("after close", func(t *testing.T) {
		t.Parallel()
		dm := stream.NewDemux(stream.DemuxOptions{})
		if err := dm.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if err := dm.Subscribe(contractA, &collector{}); !errors.Is(err, stream.ErrDemuxClosed) {
			t.Errorf("Subscribe() after Close = %v, want ErrDemuxClosed", err)
		}
		if err := dm.Submit(eventRecord("001", contractA)); !errors.Is(err, stream.ErrDemuxClosed) {
			t.Errorf("Submit() after Close = %v, want ErrDemuxClosed", err)
		}
		// A second Close must stay calm rather than doubling up.
		if err := dm.Close(); err != nil {
			t.Errorf("second Close() error = %v, want nil", err)
		}
	})
}

// TestDemuxCloseDeliversEverythingAccepted stresses the drain path: the
// intake queue overflows while Submit still blocks, yet Close must deliver
// every record accepted before it, once, in order.
func TestDemuxCloseDeliversEverythingAccepted(t *testing.T) {
	t.Parallel()

	dm := stream.NewDemux(stream.DemuxOptions{QueueCapacity: 4, SubscriberCapacity: 2})
	sink := &collector{}
	if err := dm.Subscribe(contractA, sink); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	const total = 300
	for i := range total {
		if err := dm.Submit(eventRecord(fmt.Sprintf("%04d", i), contractA)); err != nil {
			t.Fatalf("Submit(%d) error = %v", i, err)
		}
	}
	if err := dm.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	want := make([]string, 0, total)
	for i := range total {
		want = append(want, fmt.Sprintf("%04d", i))
	}
	checkOrderedIDs(t, "A", sink.ids, want)
	if got := dm.Stats().Unrouted; got != 0 {
		t.Errorf("Stats().Unrouted = %d, want 0", got)
	}
}

// gate is a Sink whose first Emit blocks until released, which is how the
// drop test pins the dispatcher to a full subscriber queue deterministically.
type gate struct {
	block chan struct{}
	inner stream.Sink
}

func newGate(block chan struct{}, inner stream.Sink) *gate {
	return &gate{block: block, inner: inner}
}

func (g *gate) Emit(r stream.Record) error {
	<-g.block
	return g.inner.Emit(r)
}

func (g *gate) Flush() error { return g.inner.Flush() }

// TestDemuxDropsRatherThanWedges checks the overload policy: a subscriber
// that cannot drain must cost it drops, not the other contracts' records or
// a wedged dispatcher.
func TestDemuxDropsRatherThanWedges(t *testing.T) {
	t.Parallel()

	dm := stream.NewDemux(stream.DemuxOptions{QueueCapacity: 64, SubscriberCapacity: 8})
	defer dm.Close() //nolint:errcheck // error asserted below

	fastSink, slowSink := &collector{}, &collector{}
	release := make(chan struct{})
	slow := newGate(release, slowSink)
	fast := newGate(release, fastSink)

	// Both gates hold their first Emit until the release channel closes,
	// so queues fill while the test is still submitting and the dispatcher
	// has no choice but to count drops.
	if err := dm.Subscribe(contractA, slow); err != nil {
		t.Fatalf("Subscribe(slow) error = %v", err)
	}
	if err := dm.Subscribe(contractB, fast); err != nil {
		t.Fatalf("Subscribe(fast) error = %v", err)
	}

	const total = 50
	for i := range total {
		for _, cid := range []string{contractA, contractB} {
			if err := dm.Submit(eventRecord(fmt.Sprintf("%03d", i), cid)); err != nil {
				t.Fatalf("Submit(%d, %s) error = %v", i, cid, err)
			}
		}
	}

	// While the gates hold, every record the dispatcher delivers to these
	// subscribers stops inside a blocked Emit, before the collectors are
	// touched — so both collectors must sit empty, and the queues must fill
	// past capacity, which is what counting drops in Stats shows.
	deadline := time.Now().Add(2 * time.Second)
	for dm.Stats().Unrouted == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no overflow counted within the deadline; the drop policy did not engage")
		}
		time.Sleep(time.Millisecond)
	}
	if len(slowSink.ids) != 0 || len(fastSink.ids) != 0 {
		t.Errorf("collectors filled while gated: %d and %d records, want 0, 0",
			len(slowSink.ids), len(fastSink.ids))
	}

	// Release, let Close settle the world, and assert conservation: every
	// submitted record is either delivered exactly once or counted unrouted.
	close(release)
	if err := dm.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	stats := dm.Stats()
	delivered := len(slowSink.ids) + len(fastSink.ids)
	if got, want := stats.Submitted, int64(2*total); got != want {
		t.Errorf("Stats().Submitted = %d, want %d", got, want)
	}
	if got, want := delivered+int(stats.Unrouted), 2*total; got != want {
		t.Errorf("delivered %d + unrouted %d = %d, want %d", delivered, stats.Unrouted, got, want)
	}
}

// TestDemuxPropagatesSinkError checks that a subscriber's failure surfaces
// from Close, without disturbing other subscribers.
func TestDemuxPropagatesSinkError(t *testing.T) {
	t.Parallel()

	dm := stream.NewDemux(stream.DemuxOptions{})
	wantErr := errors.New("sink funeral")
	boom := &collector{failAt: 2, err: wantErr}
	healthy := &collector{}
	if err := dm.Subscribe(contractA, boom); err != nil {
		t.Fatalf("Subscribe(failing) error = %v", err)
	}
	if err := dm.Subscribe(contractB, healthy); err != nil {
		t.Fatalf("Subscribe(healthy) error = %v", err)
	}

	for i := range 10 {
		if err := dm.Submit(eventRecord(fmt.Sprintf("%03d", i), contractA)); err != nil {
			t.Fatalf("Submit(A) error = %v", err)
		}
		if err := dm.Submit(eventRecord(fmt.Sprintf("%03d", i), contractB)); err != nil {
			t.Fatalf("Submit(B) error = %v", err)
		}
	}

	err := dm.Close()
	if !errors.Is(err, wantErr) {
		t.Fatalf("Close() error = %v, want it to surface %v", err, wantErr)
	}
	// The healthy subscriber must have received everything despite its
	// neighbour failing.
	if len(healthy.ids) != 10 {
		t.Errorf("healthy subscriber received %d records, want 10", len(healthy.ids))
	}
	// And the failing one stopped delivering after its error instead of
	// hammering a dead sink.
	if got := len(boom.ids); got != 2 {
		t.Errorf("failing subscriber received %d records, want 2 (emit stops at failure)", got)
	}
}

// TestDemuxConcurrentSubmitters checks routing under racing producers: every
// accepted record arrives on exactly one subscriber, once.
func TestDemuxConcurrentSubmitters(t *testing.T) {
	t.Parallel()

	dm := stream.NewDemux(stream.DemuxOptions{})
	defer dm.Close() //nolint:errcheck // asserted below

	sinkA, sinkB := &collector{}, &collector{}
	if err := dm.Subscribe(contractA, sinkA); err != nil {
		t.Fatalf("Subscribe(A) error = %v", err)
	}
	if err := dm.Subscribe(contractB, sinkB); err != nil {
		t.Fatalf("Subscribe(B) error = %v", err)
	}

	const perProducer = 100
	var wg sync.WaitGroup
	for _, tc := range []struct {
		contract string
		prefix   byte
		to       *collector
	}{
		{contractA, 'A', sinkA},
		{contractB, 'B', sinkB},
	} {
		wg.Add(1)
		go func(contract string, prefix byte, to *collector) {
			defer wg.Done()
			for i := range perProducer {
				id := fmt.Sprintf("%c%03d", prefix, i)
				if err := dm.Submit(eventRecord(id, contract)); err != nil {
					t.Errorf("Submit(%s) error = %v", id, err)
					return
				}
			}
		}(tc.contract, tc.prefix, tc.to)
	}
	wg.Wait()

	if err := dm.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if len(sinkA.ids) != perProducer || len(sinkB.ids) != perProducer {
		t.Errorf("subscribers received %d and %d records, want %d each",
			len(sinkA.ids), len(sinkB.ids), perProducer)
	}
}

// TestDemuxIntegrationWithTailer wires a demux behind a real tailer run over
// a fake RPC server: the demux becomes the tailer's sink, so the whole
// stream flows through dispatch rather than through a test stand-in.
func TestDemuxIntegrationWithTailer(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(700, 40)
	contract := events[0].ContractID
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(7))
	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 700, PageLimit: 7})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	dm := stream.NewDemux(stream.DemuxOptions{})
	subscriber := &collector{}
	if err := dm.Subscribe(contract, subscriber); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	tl := stream.NewTailer(src, stream.SinkFunc(dm.Submit), stream.Options{Clock: newFakeClock()})
	if err := tl.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := dm.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	assertStreamInvariants(t, subscriber.ids, idsOf(events))
}
