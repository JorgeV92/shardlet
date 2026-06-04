package shardlet

import (
	"errors"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyExpired  = errors.New("key expired")
	ErrNoGroups    = errors.New("no shard groups configured")
	ErrBadShard    = errors.New("bad shard id")
)

type Entry struct {
	Value     string
	ExpiresAt time.Time
	Version   uint64
}

func (e Entry) Expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt)
}

type PutOptions struct {
	TTL time.Duration
}

type Value struct {
	Key       string
	Value     string
	Version   uint64
	ShardID   int
	GroupID   string
	ExpiresAt time.Time
}

type ShardStats struct {
	ID      int
	GroupID string
	Keys    int
}

type ClusterStats struct {
	ShardCount int
	GroupCount int
	KeyCount   int
	Shards     []ShardStats
}
