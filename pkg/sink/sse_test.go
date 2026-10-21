package sink

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func broadcastRecord(id, contract, topic string) stream.Record {
	return stream.Record{Kind: stream.KindEvent, ID: id, Sequence: 10, Event: &stream.Event{ContractID: contract, TopicXDR: []string{topic}}}
}
func readSSEFrame(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	var b strings.Builder
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		b.WriteString(line)
		if line == "\n" {
			return b.String()
		}
	}
}
func openSSE(t *testing.T, url, last string) (*http.Response, *bufio.Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Last-Event-ID", last)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp, bufio.NewReader(resp.Body)
}
func TestSSELiveReplayAndKeepalive(t *testing.T) {
	s := NewSSE()
	s.pingInterval = 10 * time.Millisecond
	server := httptest.NewServer(s)
	defer server.Close()
	defer s.Close()
	for _, id := range []string{"0001", "0002", "0003"} {
		if err := s.Emit(broadcastRecord(id, "C", "T")); err != nil {
			t.Fatal(err)
		}
	}
	resp, replay := openSSE(t, server.URL, "0001")
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("bad response %+v", resp)
	}
	readSSEFrame(t, replay)
	for _, id := range []string{"0002", "0003"} {
		if got := readSSEFrame(t, replay); !strings.Contains(got, "id: "+id+"\n") || !strings.Contains(got, `"kind":"event"`) {
			t.Fatal(got)
		}
	}
	liveResp, live := openSSE(t, server.URL, "")
	defer liveResp.Body.Close()
	readSSEFrame(t, live)
	if err := s.Emit(broadcastRecord("0004", "C", "T")); err != nil {
		t.Fatal(err)
	}
	for _, r := range []*bufio.Reader{replay, live} {
		for {
			got := readSSEFrame(t, r)
			if strings.HasPrefix(got, ": ping") {
				continue
			}
			if !strings.Contains(got, "id: 0004\n") {
				t.Fatal(got)
			}
			break
		}
	}
	if got := readSSEFrame(t, live); got != ": ping\n\n" {
		t.Fatalf("missing ping %q", got)
	}
	_ = resp.Body.Close()
}
func TestSSERejectExpiredIDAndUnsafeID(t *testing.T) {
	s := NewSSE()
	s.hub.capacity = 2
	for _, id := range []string{"1", "2", "3"} {
		if err := s.Emit(broadcastRecord(id, "C", "T")); err != nil {
			t.Fatal(err)
		}
	}
	for _, last := range []string{"1", "unknown"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/events", nil)
		r.Header.Set("Last-Event-ID", last)
		s.ServeHTTP(w, r)
		if w.Code != http.StatusConflict {
			t.Fatalf("got status %d", w.Code)
		}
	}
	if err := s.Emit(broadcastRecord("1\ndata: injection", "C", "T")); err == nil {
		t.Fatal("unsafe id accepted")
	}
	_ = s.Close()
	if err := s.Emit(broadcastRecord("4", "C", "T")); err == nil {
		t.Fatal("emit after close")
	}
}
func TestBroadcastSlowClientDisconnectAndReplay(t *testing.T) {
	h := newBroadcastHub(128)
	c, err := h.subscribe("", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 65; i++ {
		if err := h.emit(broadcastRecord(fmt.Sprintf("%04d", i), "C", "T")); err != nil {
			t.Fatal(err)
		}
	}
	if len(h.clients) != 0 {
		t.Fatal("slow client retained")
	}
	count := 0
	for range c.queue {
		count++
	}
	if count != 64 {
		t.Fatalf("queued %d", count)
	}
	resumed, err := h.subscribe("0063", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := <-resumed.queue; got.id != "0064" {
		t.Fatal(got.id)
	}
	h.remove(resumed)
	if len(h.clients) != 0 {
		t.Fatal("client not cleaned up")
	}
}
func TestSSEDisconnectCleansRegistry(t *testing.T) {
	s := NewSSE()
	server := httptest.NewServer(s)
	defer server.Close()
	defer s.Close()
	resp, r := openSSE(t, server.URL, "")
	readSSEFrame(t, r)
	_ = resp.Body.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.hub.mu.Lock()
		count := len(s.hub.clients)
		s.hub.mu.Unlock()
		if count == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("disconnected client retained")
}
