package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// TestSyncWriterSerialisesConcurrentWrites checks that diagnostics from
// several goroutines cannot interleave.
//
// The streaming commands write to stderr from the stream itself and from the
// background cursor pruner. A bare bytes.Buffer is not safe for that, and the
// race detector says so.
func TestSyncWriterSerialisesConcurrentWrites(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := newSyncWriter(&buf)

	const writers = 8
	const lines = 50

	var wg sync.WaitGroup
	wg.Add(writers)
	for i := range writers {
		go func(id int) {
			defer wg.Done()
			for j := range lines {
				fmt.Fprintf(w, "writer %d line %d\n", id, j)
			}
		}(i)
	}
	wg.Wait()

	got := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(got) != writers*lines {
		t.Fatalf("lines = %d, want %d", len(got), writers*lines)
	}
	for _, line := range got {
		if !strings.HasPrefix(line, "writer ") || !strings.Contains(line, " line ") {
			t.Errorf("line is interleaved: %q", line)
		}
	}
}

// TestSyncWriterHandlesANilWriter checks that a diagnostic can never panic
// the stream it is reporting on.
func TestSyncWriterHandlesANilWriter(t *testing.T) {
	t.Parallel()

	w := newSyncWriter(nil)
	n, err := w.Write([]byte("prism: nothing to report\n"))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != len("prism: nothing to report\n") {
		t.Errorf("Write() wrote %d bytes, want %d", n, len("prism: nothing to report\n"))
	}
}

// TestSyncWriterPropagatesWriteErrors checks that a failing underlying
// writer is reported rather than swallowed.
func TestSyncWriterPropagatesWriteErrors(t *testing.T) {
	t.Parallel()

	want := io.ErrClosedPipe
	w := newSyncWriter(errWriter{err: want})

	if _, err := w.Write([]byte("prism: boom\n")); !errors.Is(err, want) {
		t.Errorf("Write() error = %v, want %v", err, want)
	}
}

// errWriter fails every write with a fixed error.
type errWriter struct {
	err error
}

func (e errWriter) Write([]byte) (int, error) {
	return 0, e.err
}
