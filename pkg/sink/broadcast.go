package sink

import (
	"bytes"
	"errors"
	"sync"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

var errBroadcastClosed = errors.New("broadcast sink closed")
var errReplayExpired = errors.New("Last-Event-ID is not in replay history")

type broadcastMessage struct {
	id     string
	data   []byte
	record stream.Record
}
type broadcastClient struct {
	queue chan broadcastMessage
	match func(stream.Record) bool
}

// broadcastHub serializes replay registration and live emission under one lock,
// so reconnecting cannot miss records between replay and subscription.
type broadcastHub struct {
	mu       sync.Mutex
	clients  map[*broadcastClient]struct{}
	history  []broadcastMessage
	capacity int
	closed   bool
}

func newBroadcastHub(capacity int) *broadcastHub {
	return &broadcastHub{clients: make(map[*broadcastClient]struct{}), capacity: capacity}
}
func recordMessage(r stream.Record) (broadcastMessage, error) {
	var b bytes.Buffer
	if err := NewNDJSON(&b, false).Emit(r); err != nil {
		return broadcastMessage{}, err
	}
	return broadcastMessage{id: r.ID, data: bytes.TrimSpace(b.Bytes()), record: r}, nil
}
func (h *broadcastHub) emit(r stream.Record) error {
	m, err := recordMessage(r)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errBroadcastClosed
	}
	if h.capacity > 0 {
		if len(h.history) == h.capacity {
			copy(h.history, h.history[1:])
			h.history[len(h.history)-1] = m
		} else {
			h.history = append(h.history, m)
		}
	}
	for c := range h.clients {
		if c.match != nil && !c.match(r) {
			continue
		}
		select {
		case c.queue <- m:
		default:
			close(c.queue)
			delete(h.clients, c)
		}
	}
	return nil
}
func (h *broadcastHub) subscribe(last string, match func(stream.Record) bool) (*broadcastClient, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, errBroadcastClosed
	}
	start := len(h.history)
	if last != "" {
		found := false
		for i, m := range h.history {
			if m.id == last {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, errReplayExpired
		}
	}
	c := &broadcastClient{queue: make(chan broadcastMessage, len(h.history)-start+64), match: match}
	for _, m := range h.history[start:] {
		c.queue <- m
	}
	h.clients[c] = struct{}{}
	return c, nil
}
func (h *broadcastHub) remove(c *broadcastClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.queue)
	}
}
func (h *broadcastHub) close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		for c := range h.clients {
			close(c.queue)
			delete(h.clients, c)
		}
	}
	return nil
}
