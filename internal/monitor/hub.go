package monitor

import (
	"encoding/json"
	"sync"
	"time"
)

const DefaultHistoryLimit = 100

// Event is the UI-safe envelope for one Kafka message.
type Event struct {
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	Received  time.Time       `json:"received_at"`
	Partition int             `json:"partition"`
	Offset    int64           `json:"offset"`
	Key       string          `json:"key,omitempty"`
}

// Hub broadcasts live monitor events and keeps a short per-topic history for
// browsers that connect after Kafka messages have already arrived.
type Hub struct {
	mu           sync.RWMutex
	subscribers  map[chan Event]struct{}
	topics       []string
	history      map[string][]Event
	historyLimit int
}

func NewHub(topics []string, historyLimit int) *Hub {
	if historyLimit <= 0 {
		historyLimit = DefaultHistoryLimit
	}

	h := &Hub{
		subscribers:  make(map[chan Event]struct{}),
		topics:       append([]string(nil), topics...),
		history:      make(map[string][]Event, len(topics)),
		historyLimit: historyLimit,
	}

	for _, topic := range topics {
		h.history[topic] = nil
	}

	return h
}

// Subscribe returns a channel that receives published events and a function to
// stop receiving.
func (h *Hub) Subscribe() (chan Event, func()) {
	ch := make(chan Event, 64)
	h.mu.Lock()
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()

	unsub := func() {
		h.mu.Lock()
		if _, ok := h.subscribers[ch]; ok {
			delete(h.subscribers, ch)
			close(ch)
		}
		h.mu.Unlock()
	}

	return ch, unsub
}

// Topics returns the ordered list of monitored Kafka topics.
func (h *Hub) Topics() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return append([]string(nil), h.topics...)
}

// Snapshot returns recent events for every known topic, including topics with
// no messages yet.
func (h *Hub) Snapshot() map[string][]Event {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make(map[string][]Event, len(h.history))
	for _, topic := range h.topics {
		events := h.history[topic]
		if events == nil {
			out[topic] = []Event{}
			continue
		}
		out[topic] = append([]Event(nil), events...)
	}

	return out
}

// Publish stores the event and sends it to subscribers without blocking Kafka
// consumers on slow browsers.
func (h *Hub) Publish(event Event) {
	h.mu.Lock()
	h.history[event.Topic] = append(h.history[event.Topic], event)
	if len(h.history[event.Topic]) > h.historyLimit {
		h.history[event.Topic] = h.history[event.Topic][len(h.history[event.Topic])-h.historyLimit:]
	}

	for ch := range h.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
	h.mu.Unlock()
}
