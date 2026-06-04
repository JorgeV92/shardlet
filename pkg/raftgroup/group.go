package raftgroup

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"shardlet/pkg/shardlet"
)

var (
	ErrNoQuorum       = errors.New("raft group has no write quorum")
	ErrReplicaOffline = errors.New("replica is offline")
	ErrUnknownReplica = errors.New("unknown replica")
	ErrEmptyGroup     = errors.New("raft group needs at least one replica")
)

type Operation string

const (
	OpPut       Operation = "put"
	OpDelete    Operation = "delete"
	OpRebalance Operation = "rebalance"
)

type LogEntry struct {
	Index  uint64
	Term   uint64
	Op     Operation
	Key    string
	Value  string
	TTL    time.Duration
	Groups []string
}

type ReplicaState struct {
	ID           string
	Online       bool
	Leader       bool
	Term         uint64
	LogLength    int
	AppliedIndex uint64
	KeyCount     int
}

type GroupStats struct {
	ID          string
	Term        uint64
	LeaderID    string
	CommitIndex uint64
	Replicas    []ReplicaState
}

type Replica struct {
	id      string
	online  bool
	store   *shardlet.Store
	log     []LogEntry
	applied uint64
}

type Group struct {
	mu          sync.RWMutex
	id          string
	term        uint64
	leaderID    string
	commitIndex uint64
	replicas    map[string]*Replica
	order       []string
}

func NewGroup(id string, replicaIDs []string, shardCount int) (*Group, error) {
	if len(replicaIDs) == 0 {
		return nil, ErrEmptyGroup
	}

	ids := normalizeReplicaIDs(replicaIDs)
	if len(ids) == 0 {
		return nil, ErrEmptyGroup
	}

	g := &Group{
		id:       id,
		term:     1,
		leaderID: ids[0],
		replicas: make(map[string]*Replica, len(ids)),
		order:    ids,
	}
	for _, replicaID := range ids {
		g.replicas[replicaID] = &Replica{
			id:     replicaID,
			online: true,
			store:  shardlet.MustNewStore(shardCount, []string{id}),
		}
	}
	return g, nil
}

func MustNewGroup(id string, replicaIDs []string, shardCount int) *Group {
	g, err := NewGroup(id, replicaIDs, shardCount)
	if err != nil {
		panic(err)
	}
	return g
}

func normalizeReplicaIDs(replicaIDs []string) []string {
	seen := make(map[string]struct{}, len(replicaIDs))
	ids := make([]string, 0, len(replicaIDs))
	for _, id := range replicaIDs {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (g *Group) Put(key, value string, opts shardlet.PutOptions) (shardlet.Value, error) {
	entry := LogEntry{Op: OpPut, Key: key, Value: value, TTL: opts.TTL}
	if err := g.commit(entry); err != nil {
		return shardlet.Value{}, err
	}
	return g.Get(key)
}

func (g *Group) Get(key string) (shardlet.Value, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	leader := g.replicas[g.leaderID]
	if leader == nil {
		return shardlet.Value{}, ErrUnknownReplica
	}
	if !leader.online {
		return shardlet.Value{}, ErrReplicaOffline
	}
	return leader.store.Get(key)
}

func (g *Group) Delete(key string) (bool, error) {
	before, getErr := g.Get(key)
	err := g.commit(LogEntry{Op: OpDelete, Key: key})
	if err != nil {
		return false, err
	}
	return getErr == nil && before.Key != "", nil
}

func (g *Group) Rebalance(groups []string) error {
	return g.commit(LogEntry{Op: OpRebalance, Groups: groups})
}

func (g *Group) SetOnline(replicaID string, online bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	replica := g.replicas[replicaID]
	if replica == nil {
		return ErrUnknownReplica
	}
	replica.online = online
	if online {
		g.catchUpLocked(replica)
	}
	return nil
}

func (g *Group) PromoteLeader(replicaID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	replica := g.replicas[replicaID]
	if replica == nil {
		return ErrUnknownReplica
	}
	if !replica.online {
		return ErrReplicaOffline
	}
	g.catchUpLocked(replica)
	g.term++
	g.leaderID = replicaID
	return nil
}

func (g *Group) LeaderID() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.leaderID
}

func (g *Group) Stats() GroupStats {
	g.mu.RLock()
	defer g.mu.RUnlock()

	stats := GroupStats{
		ID:          g.id,
		Term:        g.term,
		LeaderID:    g.leaderID,
		CommitIndex: g.commitIndex,
		Replicas:    make([]ReplicaState, 0, len(g.order)),
	}
	for _, id := range g.order {
		replica := g.replicas[id]
		storeStats := replica.store.Stats()
		stats.Replicas = append(stats.Replicas, ReplicaState{
			ID:           id,
			Online:       replica.online,
			Leader:       id == g.leaderID,
			Term:         g.term,
			LogLength:    len(replica.log),
			AppliedIndex: replica.applied,
			KeyCount:     storeStats.KeyCount,
		})
	}
	return stats
}

func (g *Group) ReplicaGet(replicaID, key string) (shardlet.Value, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	replica := g.replicas[replicaID]
	if replica == nil {
		return shardlet.Value{}, ErrUnknownReplica
	}
	if !replica.online {
		return shardlet.Value{}, ErrReplicaOffline
	}
	return replica.store.Get(key)
}

func (g *Group) commit(entry LogEntry) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	leader := g.replicas[g.leaderID]
	if leader == nil {
		return ErrUnknownReplica
	}
	if !leader.online {
		return fmt.Errorf("%w: leader %s", ErrReplicaOffline, g.leaderID)
	}

	entry.Index = g.commitIndex + 1
	entry.Term = g.term

	acked := make([]*Replica, 0, len(g.replicas))
	for _, id := range g.order {
		replica := g.replicas[id]
		if !replica.online {
			continue
		}
		replica.log = append(replica.log, entry)
		acked = append(acked, replica)
	}
	if len(acked) < g.majority() {
		for _, replica := range acked {
			replica.log = replica.log[:len(replica.log)-1]
		}
		return ErrNoQuorum
	}

	g.commitIndex = entry.Index
	for _, replica := range acked {
		g.applyLocked(replica, entry)
	}
	return nil
}

func (g *Group) majority() int {
	return len(g.replicas)/2 + 1
}

func (g *Group) catchUpLocked(replica *Replica) {
	for _, id := range g.order {
		source := g.replicas[id]
		if source == replica || len(source.log) < int(g.commitIndex) {
			continue
		}
		replica.log = slices.Clone(source.log)
		break
	}
	for replica.applied < g.commitIndex {
		next := replica.log[replica.applied]
		g.applyLocked(replica, next)
	}
}

func (g *Group) applyLocked(replica *Replica, entry LogEntry) {
	if replica.applied >= entry.Index {
		return
	}

	switch entry.Op {
	case OpPut:
		_, _ = replica.store.Put(entry.Key, entry.Value, shardlet.PutOptions{TTL: entry.TTL})
	case OpDelete:
		_, _ = replica.store.Delete(entry.Key)
	case OpRebalance:
		_ = replica.store.Rebalance(entry.Groups)
	}
	replica.applied = entry.Index
}
