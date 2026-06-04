package shardlet

import (
	"cmp"
	"slices"
	"sync"
	"time"
)

type Store struct {
	mu     sync.RWMutex
	shards []*shard
	groups map[string]struct{}
}

func NewStore(shardCount int, groups []string) (*Store, error) {
	if shardCount <= 0 {
		shardCount = 32
	}
	if len(groups) == 0 {
		return nil, ErrNoGroups
	}

	normalized := normalizeGroups(groups)
	s := &Store{
		shards: make([]*shard, shardCount),
		groups: make(map[string]struct{}, len(normalized)),
	}
	for _, groupID := range normalized {
		s.groups[groupID] = struct{}{}
	}
	for i := range s.shards {
		s.shards[i] = newShard(i, normalized[i%len(normalized)])
	}
	return s, nil
}

func MustNewStore(shardCount int, groups []string) *Store {
	s, err := NewStore(shardCount, groups)
	if err != nil {
		panic(err)
	}
	return s
}

func normalizeGroups(groups []string) []string {
	seen := make(map[string]struct{}, len(groups))
	out := make([]string, 0, len(groups))
	for _, groupID := range groups {
		if groupID == "" {
			continue
		}
		if _, ok := seen[groupID]; ok {
			continue
		}
		seen[groupID] = struct{}{}
		out = append(out, groupID)
	}
	slices.Sort(out)
	return out
}

func (s *Store) Put(key, value string, opts PutOptions) (Value, error) {
	sh := s.shardForKey(key)
	if sh == nil {
		return Value{}, ErrNoGroups
	}

	var expiresAt time.Time
	if opts.TTL > 0 {
		expiresAt = time.Now().Add(opts.TTL)
	}
	entry := sh.put(key, value, expiresAt)
	return Value{
		Key:       key,
		Value:     entry.Value,
		Version:   entry.Version,
		ShardID:   sh.id,
		GroupID:   sh.currentGroup(),
		ExpiresAt: entry.ExpiresAt,
	}, nil
}

func (s *Store) Get(key string) (Value, error) {
	sh := s.shardForKey(key)
	if sh == nil {
		return Value{}, ErrNoGroups
	}

	entry, err := sh.get(key, time.Now())
	if err != nil {
		return Value{}, err
	}
	return Value{
		Key:       key,
		Value:     entry.Value,
		Version:   entry.Version,
		ShardID:   sh.id,
		GroupID:   sh.currentGroup(),
		ExpiresAt: entry.ExpiresAt,
	}, nil
}

func (s *Store) Delete(key string) (bool, error) {
	sh := s.shardForKey(key)
	if sh == nil {
		return false, ErrNoGroups
	}
	return sh.delete(key), nil
}

func (s *Store) Snapshot() map[string]Value {
	now := time.Now()
	s.mu.RLock()
	shards := slices.Clone(s.shards)
	s.mu.RUnlock()

	out := make(map[string]Value)
	for _, sh := range shards {
		for key, entry := range sh.snapshot(now) {
			out[key] = Value{
				Key:       key,
				Value:     entry.Value,
				Version:   entry.Version,
				ShardID:   sh.id,
				GroupID:   sh.currentGroup(),
				ExpiresAt: entry.ExpiresAt,
			}
		}
	}
	return out
}

func (s *Store) Stats() ClusterStats {
	now := time.Now()
	s.mu.RLock()
	shards := slices.Clone(s.shards)
	groupCount := len(s.groups)
	s.mu.RUnlock()

	stats := ClusterStats{
		ShardCount: len(shards),
		GroupCount: groupCount,
		Shards:     make([]ShardStats, 0, len(shards)),
	}
	for _, sh := range shards {
		shardStats := sh.stats(now)
		stats.KeyCount += shardStats.Keys
		stats.Shards = append(stats.Shards, shardStats)
	}
	return stats
}

func (s *Store) Rebalance(groups []string) error {
	normalized := normalizeGroups(groups)
	if len(normalized) == 0 {
		return ErrNoGroups
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.groups = make(map[string]struct{}, len(normalized))
	for _, groupID := range normalized {
		s.groups[groupID] = struct{}{}
	}
	for _, sh := range s.shards {
		nextGroup := normalized[sh.id%len(normalized)]
		sh.replaceGroup(nextGroup)
	}
	return nil
}

func (s *Store) Range(low, high string) []Value {
	now := time.Now()
	s.mu.RLock()
	shards := slices.Clone(s.shards)
	s.mu.RUnlock()

	values := make([]Value, 0)
	for _, sh := range shards {
		for key, entry := range sh.snapshot(now) {
			if key < low || (high != "" && key >= high) {
				continue
			}
			values = append(values, Value{
				Key:       key,
				Value:     entry.Value,
				Version:   entry.Version,
				ShardID:   sh.id,
				GroupID:   sh.currentGroup(),
				ExpiresAt: entry.ExpiresAt,
			})
		}
	}
	slices.SortFunc(values, func(a, b Value) int {
		return cmp.Compare(a.Key, b.Key)
	})
	return values
}

func (s *Store) shardForKey(key string) *shard {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.shards) == 0 || len(s.groups) == 0 {
		return nil
	}
	return s.shards[shardFor(key, len(s.shards))]
}
