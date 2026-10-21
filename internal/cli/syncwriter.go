package cli

import (
	"io"
	"sync"
)

// syncWriter serialises writes to an underlying writer.
//
// The streaming commands write diagnostics from more than one goroutine: the
// stream itself reports retries and gaps, and the background cursor pruner
// reports what it compacted. A bare io.Writer is not safe for that. A
// bytes.Buffer — which is what the tests use to capture stderr — would race,
// and two lines on a terminal would interleave mid-sentence.
//
// Each Write is serialised on its own, so a formatted line is never split by
// another writer. fmt.Fprintf formats into its own buffer and hands the
// result over in a single Write, so one lock acquisition covers one whole
// line.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// newSyncWriter returns a writer that serialises writes to w.
//
// A nil writer is replaced by io.Discard, so a diagnostic can never panic
// the stream it is reporting on.
func newSyncWriter(w io.Writer) io.Writer {
	if w == nil {
		return io.Discard
	}
	return &syncWriter{w: w}
}

// Write implements io.Writer.
func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}
