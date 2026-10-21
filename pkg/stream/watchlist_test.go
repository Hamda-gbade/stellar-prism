package stream

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
)

func testContractID(suffix byte) string {
	data := make([]byte, 32)
	data[31] = suffix
	return strkey.MustEncode(strkey.VersionByteContract, data)
}

func TestWatchlistManagerReloadsAdditionsAndRemovals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchlist.txt")
	c1, c2, c3 := testContractID(1), testContractID(2), testContractID(3)
	if err := os.WriteFile(path, []byte(c1+"\n"+c2+" # second\n"+c1+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	manager, err := NewWatchlistManager(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := manager.ContractIDs(), []string{c1, c2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial IDs %v, want %v", got, want)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatal(err)
	}
	updated := []byte("# updated\n" + c2 + "\n" + c3 + "\n")
	if err := os.WriteFile(path, updated, 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		if got, want := manager.ContractIDs(), []string{c2, c3}; reflect.DeepEqual(got, want) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("watchlist did not update, got %v", manager.ContractIDs())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestReadWatchlistAllowsEmptyContractSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.txt")
	if err := os.WriteFile(path, []byte("# comment\n  \n"), 0600); err != nil {
		t.Fatal(err)
	}
	manager, err := NewWatchlistManager(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := manager.ContractIDs(); len(got) != 0 {
		t.Fatalf("empty watchlist IDs = %v, want no IDs", got)
	}
}

func TestReadWatchlistRejectsMalformedContractID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.txt")
	if err := os.WriteFile(path, []byte("not-a-contract\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWatchlistManager(path); err == nil {
		t.Fatal("malformed contract ID was accepted")
	}
}
