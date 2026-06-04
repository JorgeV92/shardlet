package shardletnet

import (
	"errors"
	"time"

	"shardlet/pkg/shardlet"
)

const ProtocolVersion = "shardlet.v1"

var ErrRemote = errors.New("remote shardlet error")

type Operation string

const (
	OpPut       Operation = "put"
	OpGet       Operation = "get"
	OpDelete    Operation = "delete"
	OpRange     Operation = "range"
	OpStats     Operation = "stats"
	OpRebalance Operation = "rebalance"
)

type Request struct {
	Version string        `json:"version"`
	ID      string        `json:"id"`
	Op      Operation     `json:"op"`
	Key     string        `json:"key,omitempty"`
	Value   string        `json:"value,omitempty"`
	TTL     time.Duration `json:"ttl,omitempty"`
	Low     string        `json:"low,omitempty"`
	High    string        `json:"high,omitempty"`
	Groups  []string      `json:"groups,omitempty"`
}

type Response struct {
	Version string                 `json:"version"`
	ID      string                 `json:"id"`
	OK      bool                   `json:"ok"`
	Error   string                 `json:"error,omitempty"`
	Value   *shardlet.Value        `json:"value,omitempty"`
	Values  []shardlet.Value       `json:"values,omitempty"`
	Deleted bool                   `json:"deleted,omitempty"`
	Stats   *shardlet.ClusterStats `json:"stats,omitempty"`
}
