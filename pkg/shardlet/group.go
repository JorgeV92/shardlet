package shardlet

import (
	"sync"
	"time"
)

type shard struct {
	id      int
	groupID string
	mu      sync.RWMutex
	items   map[string]Entry
}

func newShard(id int, groupID string) *shard {
	return &shard{
		id:      id,
		groupID: groupID,
		items:   make(map[string]Entry),
	}
}

func (s *shard) get(key string, now time.Time) (Entry, error) {
	s.mu.RLock()
	entry, ok := s.items[key]
	s.mu.RUnlock()
	if !ok {
		return Entry{}, ErrKeyNotFound
	}
	if entry.Expired(now) {
		s.mu.Lock()
		if current, ok := s.items[key]; ok && current.Version == entry.Version && current.Expired(now) {
			delete(s.items, key)
		}
		s.mu.Unlock()
		return Entry{}, ErrKeyExpired
	}
	return entry, nil
}

func (s *shard) put(key, value string, expiresAt time.Time) Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.items[key]
	next.Value = value
	next.ExpiresAt = expiresAt
	next.Version++
	s.items[key] = next
	return next
}

func (s *shard) delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.items[key]; !ok {
		return false
	}
	delete(s.items, key)
	return true
}

func (s *shard) snapshot(now time.Time) map[string]Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]Entry, len(s.items))
	for key, entry := range s.items {
		if entry.Expired(now) {
			delete(s.items, key)
			continue
		}
		out[key] = entry
	}
	return out
}

func (s *shard) replaceGroup(groupID string) {
	s.mu.Lock()
	s.groupID = groupID
	s.mu.Unlock()
}

func (s *shard) currentGroup() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.groupID
}

func (s *shard) stats(now time.Time) ShardStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := 0
	for key, entry := range s.items {
		if entry.Expired(now) {
			delete(s.items, key)
			continue
		}
		keys++
	}
	return ShardStats{ID: s.id, GroupID: s.groupID, Keys: keys}
}
