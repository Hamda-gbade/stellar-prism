package sink

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ParquetSink stores the stable record envelope in date partitions. Rows are
// written incrementally; Flush closes the current row group without retaining
// stream data in memory.
type ParquetSink struct {
	dir    string
	mu     sync.Mutex
	day    string
	file   *os.File
	writer *parquet.GenericWriter[parquetRow]
}
type parquetRow struct {
	ID      string `parquet:"id"`
	Kind    string `parquet:"kind"`
	Ledger  uint32 `parquet:"ledger"`
	Time    int64  `parquet:"time"`
	Payload string `parquet:"payload"`
}

func NewParquetSink(dir string) (*ParquetSink, error) {
	if dir == "" {
		return nil, fmt.Errorf("parquet: directory is required")
	}
	return &ParquetSink{dir: dir}, nil
}
func (s *ParquetSink) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	day := r.Time.UTC().Format("2006-01-02")
	if day == "0001-01-01" {
		day = time.Now().UTC().Format("2006-01-02")
	}
	if day != s.day {
		if err := s.closeLocked(); err != nil {
			return err
		}
		path := filepath.Join(s.dir, "dt="+day)
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(path, "records.parquet"))
		if err != nil {
			return err
		}
		s.file = f
		s.writer = parquet.NewGenericWriter[parquetRow](f)
		s.day = day
	}
	b, err := MarshalRecord(r, false)
	if err != nil {
		return err
	}
	_, err = s.writer.Write([]parquetRow{{r.ID, string(r.Kind), r.Sequence, r.Time.UnixMilli(), string(b)}})
	return err
}
func (s *ParquetSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writer != nil {
		return s.writer.Flush()
	}
	return nil
}
func (s *ParquetSink) Close() error { s.mu.Lock(); defer s.mu.Unlock(); return s.closeLocked() }
func (s *ParquetSink) closeLocked() error {
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			return err
		}
		s.writer = nil
	}
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}
