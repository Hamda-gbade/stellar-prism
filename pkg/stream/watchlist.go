package stream

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/stellar/go-stellar-sdk/strkey"
)

// ContractWatchlist provides a concurrency-safe snapshot of watched IDs.
type ContractWatchlist interface{ ContractIDs() []string }

// WatchlistManager reloads a contract list when its file changes. Addresses
// are trimmed, comments and blank lines are ignored, and duplicates collapse.
type WatchlistManager struct {
	path string
	mu   sync.RWMutex
	ids  []string
	errs chan error
}

// NewWatchlistManager reads and validates the initial watchlist before the
// event stream starts.
func NewWatchlistManager(path string) (*WatchlistManager, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("watchlist path is empty")
	}
	m := &WatchlistManager{path: path, errs: make(chan error, 1)}
	ids, err := readWatchlist(path)
	if err != nil {
		return nil, err
	}
	m.ids = ids
	return m, nil
}

// ContractIDs returns a stable copy of the active subscription set.
func (m *WatchlistManager) ContractIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]string(nil), m.ids...)
}

// Errors receives file reload errors. The active set stays unchanged until a
// valid replacement has been read completely.
func (m *WatchlistManager) Errors() <-chan error { return m.errs }

// Start watches the containing directory so atomic file replacement is also
// observed. It returns after installing the watch and reloads in the background.
func (m *WatchlistManager) Start(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("watching contract file: %w", err)
	}
	dir := filepath.Dir(m.path)
	if dir == "" {
		dir = "."
	}
	if err := watcher.Add(dir); err != nil {
		_ = watcher.Close()
		return fmt.Errorf("watching contract file directory: %w", err)
	}
	go func() {
		defer close(m.errs)
		defer watcher.Close()
		base := filepath.Base(m.path)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if filepath.Base(event.Name) != base || event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Chmod) == 0 {
					continue
				}
				ids, err := readWatchlist(m.path)
				if err != nil {
					select {
					case m.errs <- err:
					default:
					}
					continue
				}
				m.mu.Lock()
				m.ids = ids
				m.mu.Unlock()
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				select {
				case m.errs <- fmt.Errorf("watching contract file: %w", err):
				default:
				}
			}
		}
	}()
	return nil
}

func readWatchlist(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("reading contract watchlist %q: %w", path, err)
	}
	defer f.Close()
	seen := map[string]struct{}{}
	var ids []string
	scanner := bufio.NewScanner(f)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		value := strings.TrimSpace(scanner.Text())
		if comment := strings.IndexByte(value, '#'); comment >= 0 {
			value = strings.TrimSpace(value[:comment])
		}
		if value == "" {
			continue
		}
		if _, err := strkey.Decode(strkey.VersionByteContract, value); err != nil {
			return nil, fmt.Errorf("contract watchlist %q line %d has an invalid contract ID %q: %w", path, lineNumber, value, err)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		ids = append(ids, value)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading contract watchlist %q: %w", path, err)
	}
	return ids, nil
}
