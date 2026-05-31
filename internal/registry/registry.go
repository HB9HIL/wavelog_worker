package registry

import "sync"

// TopicMeta holds the metadata PHP sends when registering a topic.
type TopicMeta struct {
	RequireToken bool `json:"require_token"`
}

// Registry is a thread-safe in-memory store of registered topics.
// State is lost on Worker restart; PHP re-registers via publish 404-retry or page reload.
type Registry struct {
	mu     sync.RWMutex
	topics map[string]TopicMeta
}

func New() *Registry {
	return &Registry{topics: make(map[string]TopicMeta)}
}

func (r *Registry) Register(topic string, meta TopicMeta) {
	r.mu.Lock()
	r.topics[topic] = meta
	r.mu.Unlock()
}

func (r *Registry) Unregister(topic string) {
	r.mu.Lock()
	delete(r.topics, topic)
	r.mu.Unlock()
}

// Lookup returns (meta, true) if the topic is known, (zero, false) otherwise.
func (r *Registry) Lookup(topic string) (TopicMeta, bool) {
	r.mu.RLock()
	meta, ok := r.topics[topic]
	r.mu.RUnlock()
	return meta, ok
}

// Topics returns all currently registered topic names.
func (r *Registry) Topics() []string {
	r.mu.RLock()
	out := make([]string, 0, len(r.topics))
	for t := range r.topics {
		out = append(out, t)
	}
	r.mu.RUnlock()
	return out
}
