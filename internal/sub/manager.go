package sub

import (
	"encoding/json"
	"sync"
)

// Subscriber is anything that can receive a push payload.
// Implemented by ws.Client — kept as interface to avoid circular imports.
type Subscriber interface {
	Send(payload json.RawMessage)
}

type Manager struct {
	mu   sync.RWMutex
	subs map[string]map[Subscriber]struct{}
}

func NewManager() *Manager {
	return &Manager{subs: make(map[string]map[Subscriber]struct{})}
}

func (m *Manager) Subscribe(topic string, s Subscriber) {
	m.mu.Lock()
	if m.subs[topic] == nil {
		m.subs[topic] = make(map[Subscriber]struct{})
	}
	m.subs[topic][s] = struct{}{}
	m.mu.Unlock()
}

func (m *Manager) Unsubscribe(topic string, s Subscriber) {
	m.mu.Lock()
	delete(m.subs[topic], s)
	if len(m.subs[topic]) == 0 {
		delete(m.subs, topic)
	}
	m.mu.Unlock()
}

// UnsubscribeAll removes a subscriber from every topic it joined.
func (m *Manager) UnsubscribeAll(s Subscriber) {
	m.mu.Lock()
	for topic, set := range m.subs {
		delete(set, s)
		if len(set) == 0 {
			delete(m.subs, topic)
		}
	}
	m.mu.Unlock()
}

// Publish sends payload to all subscribers of topic.
// Non-blocking: slow clients drop frames.
func (m *Manager) Publish(topic string, payload json.RawMessage) {
	m.mu.RLock()
	set := m.subs[topic]
	m.mu.RUnlock()
	for s := range set {
		s.Send(payload)
	}
}

func (m *Manager) HasSubscribers(topic string) bool {
	m.mu.RLock()
	n := len(m.subs[topic])
	m.mu.RUnlock()
	return n > 0
}

// Topics returns all topics that have at least one subscriber.
func (m *Manager) Topics() []string {
	m.mu.RLock()
	out := make([]string, 0, len(m.subs))
	for t := range m.subs {
		out = append(out, t)
	}
	m.mu.RUnlock()
	return out
}

// Stats returns the number of active topics and total connected clients.
func (m *Manager) Stats() (topics int, clients int) {
	m.mu.RLock()
	topics = len(m.subs)
	for _, set := range m.subs {
		clients += len(set)
	}
	m.mu.RUnlock()
	return
}
