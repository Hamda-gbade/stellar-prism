package stream

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrDemuxClosed is returned by Submit and Subscribe after Close has begun.
var ErrDemuxClosed = errors.New("demux is closed")

// DefaultQueueCapacity and DefaultSubscriberCapacity bound the demux's
// internal buffering. They are small on purpose: the demux sits directly
// behind a synchronous Sink whose backpressure is the design (see
// docs/architecture.md), so a deep queue would only defer a stall, not absorb
// it safely.
const (
	DefaultQueueCapacity      = 256
	DefaultSubscriberCapacity = 128
	// DefaultDemuxDropAfter is how long the dispatcher waits on a full
	// subscriber queue before counting the record dropped.
	DefaultDemuxDropAfter = 100 * time.Millisecond
)

// DemuxOptions tunes a Demux. Zero fields fall back to the defaults above.
type DemuxOptions struct {
	// QueueCapacity bounds how many records Submit accepts before it blocks
	// until the dispatcher makes progress. Zero uses DefaultQueueCapacity.
	QueueCapacity int
	// SubscriberCapacity bounds each subscriber's queue. Once it is full the
	// dispatcher stops waiting on that subscriber and counts drops instead,
	// so one wedged consumer cannot stall the other contracts' output.
	// Zero uses DefaultSubscriberCapacity.
	SubscriberCapacity int
	// DropAfter bounds how long the dispatcher waits on a full subscriber
	// queue before counting the record as dropped. It is what separates a
	// subscriber that is merely busy — one that catches up within the grace
	// loses nothing — from a wedged one, which cannot hold the dispatcher,
	// and the other contracts, hostage beyond the grace. Zero uses
	// DefaultDemuxDropAfter.
	DropAfter time.Duration
}

// DemuxStats is a snapshot of what a Demux has routed so far.
type DemuxStats struct {
	// Submitted counts records accepted from the caller.
	Submitted int64
	// Unrouted counts records with nowhere to go: non-event records, events
	// for contracts with no subscriber, or events dropped because a
	// subscriber's queue was full.
	Unrouted int64
}

// Demux routes Records for different contract IDs to independent subscribers,
// each with its own queue and goroutine, so one polling stream can fan out to
// one sink per contract without callers hand-rolling dispatch plumbing.
//
// A record is routed by its Record.Event.ContractID, and only KindEvent
// records carry a contract; anything else is counted as unrouted and dropped,
// so ledger records behind the same tailer cannot invent phantom subscribers.
// Each contract has at most one subscriber; a duplicate registration is an
// error rather than an ambiguous second consumer.
//
// Concretely:
//
//   - Ordering. Records accepted through Submit are dispatched in the order
//     accepted, and a subscriber sees its contract's records in that same
//     order. De-duplication is the tailer's job upstream of Submit; the demux
//     adds none of its own.
//   - Backpressure. Submit blocks while the intake queue is full. A slow,
//     still-reading subscriber first fills its queue and is then given a
//     bounded grace before being skipped with a counted drop, so a wedged
//     consumer cannot stall the other contracts and nothing grows without
//     bound.
//   - Termination. Submit never panics on a closed channel, because the
//     dispatcher never closes the intake. Close drains everything accepted
//     before it, closes each per-subscriber queue from the dispatcher (the
//     only closer, after its last send), lets subscribers flush, and returns
//     once every goroutine it spawned has exited.
type Demux struct {
	opts DemuxOptions

	// in is the intake queue. Only the dispatcher reads it and nothing
	// closes it, so a Submit racing Close can never send on a closed
	// channel; records it accepted either drain or sit for the GC.
	in chan Record
	// closer signals the dispatcher to wind down.
	closer chan struct{}
	// dispatchWait tracks only the dispatcher goroutine.
	dispatchWait sync.WaitGroup
	// drainWait tracks only subscriber goroutines, which are Add'ed before
	// Close can reach the corresponding Wait: Subscribe refuses after the
	// closed flag is set, and Wait starts after that flag.
	drainWait sync.WaitGroup
	// closeOnce brackets Close idempotently.
	closeOnce sync.Once

	mu sync.Mutex
	// subs holds the one subscriber per contract; reads and writes all
	// happen under mu.
	subs map[string]*subscriber
	// closed is set under mu before Close's first Wait, which is the edge
	// Subscribe relies on for safe accounting.
	closed bool

	submitted atomic.Int64
	unrouted  atomic.Int64
}

// subscriber couples one Sink to its queue and drain goroutine.
type subscriber struct {
	sink Sink
	// queue is closed by the dispatcher's wind-down — the only closer, and
	// only after its final send.
	queue chan Record
	// drop counts records the dispatcher skipped because the queue was full.
	drop atomic.Int64
	// err is written before the drain goroutine exits and read by Close only
	// after drainWait has synchronised on that exit, so no further guard is
	// needed to read it there.
	err error
}

// NewDemux returns a Demux with its dispatcher running.
func NewDemux(opts DemuxOptions) *Demux {
	if opts.QueueCapacity <= 0 {
		opts.QueueCapacity = DefaultQueueCapacity
	}
	if opts.SubscriberCapacity <= 0 {
		opts.SubscriberCapacity = DefaultSubscriberCapacity
	}
	if opts.DropAfter <= 0 {
		opts.DropAfter = DefaultDemuxDropAfter
	}
	d := &Demux{
		opts:   opts,
		in:     make(chan Record, opts.QueueCapacity),
		closer: make(chan struct{}),
		subs:   make(map[string]*subscriber),
	}
	d.dispatchWait.Add(1)
	go d.dispatch()
	return d
}

// dispatch accepts records and routes them one by one; after closer fires it
// drains everything already accepted, closes the per-subscriber queues — the
// last sender closing, so no send on a closed queue can ever race — and exits.
func (d *Demux) dispatch() {
	defer d.dispatchWait.Done()
	for {
		select {
		case r := <-d.in:
			d.route(r)
			continue
		case <-d.closer:
		}
		break
	}
	for {
		select {
		case r := <-d.in:
			d.route(r)
		default:
			d.releaseSubscribers()
			return
		}
	}
}

// route delivers r to the subscriber for its contract, or counts it unrouted.
// A full subscriber queue costs the dispatcher a bounded wait rather than an
// immediate drop: a slow but progressing consumer keeps every record, while
// a wedged one is skipped after the grace, so one stalled contract cannot
// hold the others behind it.
func (d *Demux) route(r Record) {
	if r.Kind != KindEvent || r.Event == nil {
		d.unrouted.Add(1)
		return
	}
	d.mu.Lock()
	s := d.subs[r.Event.ContractID]
	d.mu.Unlock()
	if s == nil {
		d.unrouted.Add(1)
		return
	}
	// Fast path: the common case costs no timer.
	select {
	case s.queue <- r:
		return
	default:
	}
	timer := time.NewTimer(d.opts.DropAfter)
	defer timer.Stop()
	select {
	case s.queue <- r:
	case <-timer.C:
		s.drop.Add(1)
		d.unrouted.Add(1)
	}
}

// Subscribe routes every record for contractID to sink on its own goroutine.
// The goroutine terminates only via Close, which flushes the sink, records a
// first flush error for Close to surface, and exits. An empty contract ID, a
// nil sink, a duplicate registration, or a call after Close is refused with
// an error naming which.
func (d *Demux) Subscribe(contractID string, sink Sink) error {
	if contractID == "" {
		return errors.New("demux: empty contract ID")
	}
	if sink == nil {
		return errors.New("demux: nil sink")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDemuxClosed
	}
	if _, exists := d.subs[contractID]; exists {
		return errors.New("demux: contract " + contractID + " already has a subscriber")
	}
	s := &subscriber{sink: sink, queue: make(chan Record, d.opts.SubscriberCapacity)}
	d.subs[contractID] = s
	d.drainWait.Add(1)
	go d.drain(s)
	return nil
}

// drain moves records from s.queue into s.sink until the queue closes, then
// flushes and exits with a first error, if any, recorded for Close.
//
// After a sink error it drains and discards without emitting further, so a
// dead consumer can never block the dispatcher on a send that would never be
// read.
func (d *Demux) drain(s *subscriber) {
	defer d.drainWait.Done()
	var emitErr error
	for r := range s.queue {
		if emitErr == nil {
			emitErr = s.sink.Emit(r)
		}
		// emitErr, once set, makes the loop just drain; see above.
	}
	if emitErr == nil {
		emitErr = s.sink.Flush()
	}
	if emitErr != nil {
		s.err = emitErr
	}
}

// releaseSubscribers closes every queue exactly once, from the dispatcher
// after its final drain.
func (d *Demux) releaseSubscribers() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.subs {
		close(s.queue)
	}
}

// Submit accepts one record for routing, blocking while the intake queue is
// full — the same natural backpressure a synchronous Sink applies. After
// Close it returns ErrDemuxClosed instead of sending.
func (d *Demux) Submit(r Record) error {
	// The closed channel is checked first so a Submit after Close is refused
	// deterministically: the intake is never closed (that is what makes a
	// racing Submit panic-free), so its buffer stays sendable and a bare
	// select would take the send case whenever the buffer had room.
	select {
	case <-d.closer:
		return ErrDemuxClosed
	default:
	}
	select {
	case d.in <- r:
		d.submitted.Add(1)
		return nil
	case <-d.closer:
		return ErrDemuxClosed
	}
}

// Stats returns a snapshot of the routing counters so far.
func (d *Demux) Stats() DemuxStats {
	return DemuxStats{
		Submitted: d.submitted.Load(),
		Unrouted:  d.unrouted.Load(),
	}
}

// Close winds the demux down: everything accepted before Close is dispatched
// or counted, every queue is flushed, and every goroutine the demux spawned
// has exited by the time it returns. It is idempotent, and returns the first
// sink error seen during delivery, if any.
func (d *Demux) Close() error {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		d.mu.Unlock()
		close(d.closer)
		d.dispatchWait.Wait()
		d.drainWait.Wait()
	})
	return d.firstError()
}

// firstError surfaces the first sink failure recorded by any drain goroutine.
// Every drain goroutine has exited by the time it runs — Close's Waits order
// their writes before this read — so the plain field read is race-free.
func (d *Demux) firstError() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.subs {
		if s.err != nil {
			return s.err
		}
	}
	return nil
}
