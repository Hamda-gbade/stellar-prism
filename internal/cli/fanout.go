package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// fanoutSink routes every event through a stream.Demux, giving each contract
// its own NDJSON file under a directory, while the teeSink above it keeps
// the combined stream flowing to the command's main output.
//
// Per-contract files are created lazily on each contract's first record, and
// each file is owned by a single demux drain goroutine that only ever writes
// it until the demux's wind-down — so file lifetime needs no extra locking.
type fanoutSink struct {
	dm  *stream.Demux
	dir string

	mu    sync.Mutex
	files map[string]*fanoutFile
}

// fanoutFile couples one NDJSON sink to the file it writes, so the demux's
// end-of-drain Flush closes the file as well as flushing the buffer.
type fanoutFile struct {
	sink stream.Sink
	file *os.File
}

// newFanoutSink validates the directory up front, so a bad --fanout-dir
// fails before any streaming starts rather than per record later.
func newFanoutSink(dir string) (*fanoutSink, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating fanout directory %s: %w", dir, err)
	}
	return &fanoutSink{
		dm:    stream.NewDemux(stream.DemuxOptions{}),
		dir:   dir,
		files: make(map[string]*fanoutFile),
	}, nil
}

// Emit implements stream.Sink by handing events to the demux; non-event
// records pass through silently, because per-contract ledgers make no sense
// and the demux would only count them unrouted.
func (f *fanoutSink) Emit(r stream.Record) error {
	if r.Kind != stream.KindEvent || r.Event == nil || r.Event.ContractID == "" {
		return nil
	}
	if err := f.ensure(r.Event.ContractID); err != nil {
		return err
	}
	return f.dm.Submit(r)
}

// ensure subscribes the contract's file sink before any record for it is
// submitted, so the first record is routed rather than dropped.
func (f *fanoutSink) ensure(contractID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.files[contractID]; exists {
		return nil
	}

	file, err := os.Create(filepath.Join(f.dir, contractID+".ndjson"))
	if err != nil {
		return fmt.Errorf("creating fanout file for %s: %w", contractID, err)
	}
	fan := &fanoutFile{sink: sink.NewNDJSON(file, true), file: file}
	if err := f.dm.Subscribe(contractID, fan); err != nil {
		closeErr := file.Close()
		return errors.Join(
			fmt.Errorf("subscribing %s to the fanout: %w", contractID, err),
			closeErr,
		)
	}
	f.files[contractID] = fan
	return nil
}

// Flush implements stream.Sink as a no-op: each per-contract sink already
// flushes per record, and finish does the durable wind-down.
func (f *fanoutSink) Flush() error { return nil }

// finish winds the demux down — drains everything accepted, flushes and
// closes every per-contract file, waits for every drain goroutine — and is
// called exactly once per run, after the tailer has stopped.
func (f *fanoutSink) finish() error {
	return f.dm.Close()
}

// Emit implements stream.Sink by writing into the file bound at
// construction; it runs solely on the demux's per-contract drain goroutine,
// which is why it needs no guard.
func (x *fanoutFile) Emit(r stream.Record) error { return x.sink.Emit(r) }

// Flush implements stream.Sink: the buffered NDJSON sink flushes first, then
// the file closes, and both failures are joined rather than masked.
func (x *fanoutFile) Flush() error {
	return errors.Join(x.sink.Flush(), x.file.Close())
}

// teeSink delivers every record to two sinks and joins their errors, so the
// main output and the fanout share one backing sink chain without either
// being able to silently mask the other's failure.
type teeSink struct {
	main   stream.Sink
	second stream.Sink
}

// Emit implements stream.Sink. errors.Join drops nils and collapses the all-nil
// case, so a clean pass through both sinks stays a nil error.
func (t *teeSink) Emit(r stream.Record) error {
	return errors.Join(t.main.Emit(r), t.second.Emit(r))
}

// Flush implements stream.Sink likewise across both sinks.
func (t *teeSink) Flush() error {
	return errors.Join(t.main.Flush(), t.second.Flush())
}
