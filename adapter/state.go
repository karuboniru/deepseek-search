package adapter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
)

// Options bounds work and in-memory conversation state. Zero values use defaults.
// MaxRounds and MaxCalls accept -1 to disable their respective execution limits.
type Options struct {
	MaxRounds       int
	MaxCalls        int
	MaxStates       int
	MaxHistoryItems int
	MaxHistoryBytes int
	// OnSearchCacheRestore reports at most one cache fallback per request.
	// It may be called concurrently by requests and receives no private data.
	OnSearchCacheRestore func(context.Context)
}

func (o Options) defaults() Options {
	if o.MaxRounds == 0 || o.MaxRounds < -1 {
		o.MaxRounds = 8
	}
	if o.MaxCalls == 0 || o.MaxCalls < -1 {
		o.MaxCalls = 16
	}
	if o.MaxStates <= 0 {
		o.MaxStates = 128
	}
	if o.MaxHistoryItems <= 0 {
		o.MaxHistoryItems = 256
	}
	if o.MaxHistoryBytes <= 0 {
		o.MaxHistoryBytes = 4 << 20
	}
	return o
}

type conversation struct {
	tenant        string
	history       []json.RawMessage
	pending       map[string]json.RawMessage
	tools         json.RawMessage
	toolChoice    json.RawMessage
	choicePresent bool
	model         json.RawMessage
	searchEnabled bool
	response      json.RawMessage
}

func (c conversation) clone() conversation {
	out := c
	out.history = append([]json.RawMessage(nil), c.history...)
	out.pending = make(map[string]json.RawMessage, len(c.pending))
	for id, item := range c.pending {
		out.pending[id] = item
	}
	return out
}

type stateStore struct {
	mu       sync.Mutex
	items    map[string]conversation
	order    []string
	webItems map[string]webRecord
	webOrder []string
	limit    int
	webLimit int
}

type webRecord struct {
	tenant string
	round  string
	items  map[string]bool
	tail   []string
	call   json.RawMessage
	result json.RawMessage
}

func newStateStore(limit, webLimit int) *stateStore {
	return &stateStore{items: make(map[string]conversation), webItems: make(map[string]webRecord), limit: limit, webLimit: webLimit}
}

func (s *stateStore) getWeb(tenant, id string) (webRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.webItems[id]
	if !ok || item.tenant != tenant {
		return webRecord{}, false
	}
	return item, true
}

func (s *stateStore) putWeb(id string, item webRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.webItems[id] = item
	s.webOrder = append(s.webOrder, id)
	for len(s.webOrder) > s.webLimit {
		delete(s.webItems, s.webOrder[0])
		s.webOrder = s.webOrder[1:]
	}
}

func (s *stateStore) deleteWeb(tenant, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.webItems[id]
	if !ok || item.tenant != tenant {
		return
	}
	delete(s.webItems, id)
	order := s.webOrder[:0]
	for _, savedID := range s.webOrder {
		if savedID != id {
			order = append(order, savedID)
		}
	}
	clear(s.webOrder[len(order):])
	s.webOrder = order
}

func (s *stateStore) get(tenant, id string) (conversation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.items[id]
	if !ok || c.tenant != tenant {
		return conversation{}, false
	}
	return c.clone(), true
}

func (s *stateStore) put(id string, c conversation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[id] = c.clone()
	s.order = append(s.order, id)
	for len(s.order) > s.limit {
		delete(s.items, s.order[0])
		s.order = s.order[1:]
	}
}

func responseID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", errors.New("generate response ID: " + err.Error())
	}
	return "resp_" + hex.EncodeToString(random[:]), nil
}
