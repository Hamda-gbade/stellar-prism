package sink

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func dialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(url, "http")+"/events", nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func readWS(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var m map[string]any
	if err := c.ReadJSON(&m); err != nil {
		t.Fatal(err)
	}
	return m
}
func subscribeWS(t *testing.T, c *websocket.Conn, kind, id string) {
	t.Helper()
	if err := c.WriteJSON(map[string]string{"subscribe": kind, "id": id}); err != nil {
		t.Fatal(err)
	}
	if got := readWS(t, c); got["subscribed"] != kind || got["id"] != id {
		t.Fatalf("bad acknowledgement %v", got)
	}
}
func TestWebSocketSubscriptionsAndShutdown(t *testing.T) {
	s := NewWebSocket()
	server := httptest.NewServer(s)
	defer server.Close()
	defer s.Close()
	a := dialWS(t, server.URL)
	b := dialWS(t, server.URL)
	subscribeWS(t, a, "contract", "CA")
	subscribeWS(t, a, "topic", "TA")
	subscribeWS(t, b, "contract", "CB")
	for _, r := range []struct{ id, c, topic string }{{"1", "CA", "wrong"}, {"2", "other", "TA"}, {"3", "CB", "TA"}, {"4", "CA", "TA"}} {
		if err := s.Emit(broadcastRecord(r.id, r.c, r.topic)); err != nil {
			t.Fatal(err)
		}
	}
	if got := readWS(t, a); got["id"] != "4" {
		t.Fatalf("wrong filtered record %v", got)
	}
	if got := readWS(t, b); got["id"] != "3" {
		t.Fatalf("wrong contract record %v", got)
	}
	subscribeWS(t, a, "contract", "CB")
	if err := s.Emit(broadcastRecord("5", "CB", "TA")); err != nil {
		t.Fatal(err)
	}
	if got := readWS(t, a); got["id"] != "5" {
		t.Fatal(got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	_ = a.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := a.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseGoingAway) {
		t.Fatalf("shutdown error %v", err)
	}
	if err := s.Emit(broadcastRecord("6", "C", "T")); err == nil {
		t.Fatal("emit after shutdown")
	}
}
func TestWebSocketMalformedSubscriptionAndOrigin(t *testing.T) {
	s := NewWebSocket()
	server := httptest.NewServer(s)
	defer server.Close()
	defer s.Close()
	conn, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/events", http.Header{"Origin": []string{"https://untrusted.example"}})
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatal("cross-origin request accepted")
	}
	c := dialWS(t, server.URL)
	if err := c.WriteJSON(map[string]string{"subscribe": "unknown", "id": "C"}); err != nil {
		t.Fatal(err)
	}
	if got := readWS(t, c); got["error"] == nil {
		t.Fatal(got)
	}
	if err := s.Emit(broadcastRecord("1", "C", "T")); err != nil {
		t.Fatal(err)
	}
	if got := readWS(t, c); got["id"] != "1" {
		t.Fatal(got)
	}
}
