# Architecture

Shardlet is organized as three layers: a local sharded storage engine, a TCP client/server API, and an in-memory Raft-style replication layer for shard groups.

## Package Layout

| Package | Responsibility |
| --- | --- |
| `pkg/shardlet` | Owns key placement, shard locks, TTL cleanup, range scans, snapshots, and cluster stats. |
| `pkg/shardletnet` | Exposes a TCP JSON protocol for remote clients. |
| `pkg/raftgroup` | Replicates writes across multiple shard-group replicas using a leader log and majority commit. |
| `cmd/shardlet` | Runs a local concurrency demo or starts the TCP server. |

## Storage Layer

`Store` divides keys across a fixed number of logical shards. Each shard owns:

- a shard id
- a group owner label
- a `map[string]Entry`
- an independent `sync.RWMutex`

The store has a small topology lock for finding shards and updating shard ownership. Once an operation finds the correct shard, it uses that shard's own lock. This keeps unrelated keys from contending on a single global mutex.

Key placement uses FNV-1a:

```text
shardID = fnv64a(key) % shardCount
```

`Rebalance` updates shard owner labels while preserving the existing local key/value data. In a distributed version, this is where shard freeze, install, and delete RPCs would move data between shard groups.

## TCP Layer

`pkg/shardletnet` uses newline-delimited JSON over TCP.

Supported operations:

- `put`
- `get`
- `delete`
- `range`
- `stats`
- `rebalance`

The server accepts connections in a loop and starts one goroutine per connection. A single client serializes requests on its TCP connection, while multiple clients can operate concurrently through separate connections.

The protocol is intentionally plain text so it is easy to inspect and debug. A future gRPC/protobuf API can be added without changing the storage layer.

## Replication Layer

`pkg/raftgroup.Group` models a replicated shard group. Each replica owns an independent `shardlet.Store` and a copy of the replicated log.

Write flow:

1. The client calls `Put`, `Delete`, or `Rebalance` on the group.
2. The current leader creates a `LogEntry` with the next index and current term.
3. The entry is appended to every online replica.
4. The group commits the entry only if a majority of replicas acknowledges it.
5. Committed entries are applied to each acknowledging replica's local store.
6. Offline replicas catch up from the committed log when they return.

Read flow:

1. `Get` reads from the current leader's local store.
2. `ReplicaGet` can inspect a specific replica in tests.

Failover is deterministic today: tests explicitly call `PromoteLeader`. Promotion increments the term and catches the new leader up to the committed index before it accepts writes.

## Concurrency Model

Shardlet uses coarse locks only where shared topology or replication metadata is involved:

- `Store.mu` protects the slice of shards and group metadata.
- Each shard has its own `sync.RWMutex` for key/value data.
- `raftgroup.Group.mu` serializes log replication, commit index updates, leader changes, and replica liveness.
- `shardletnet.Server` uses goroutines for connection-level concurrency.
- `shardletnet.Client` serializes requests over a single connection with a mutex.

This design keeps the critical sections clear and makes the code straightforward to validate with `go test -race`.

## Correctness Guarantees

Current guarantees:

- writes to a local shard are protected by that shard's lock
- expired keys are removed lazily on `Get` and `Snapshot`
- range scans return keys in lexical order
- `Rebalance` rejects empty group sets
- replicated writes require a majority before commit
- writes fail when the leader is offline
- writes fail when a quorum is unavailable
- returning replicas catch up to the committed log

Current limitations:

- replication is in-memory
- replica-to-replica communication is simulated inside one process
- leader election is deterministic, not randomized
- there are no heartbeat timers
- there is no durable WAL or snapshot persistence
- there is no controller yet for multi-group shard movement

## Test Coverage

The test suite covers:

- local `Put`, `Get`, `Delete`
- TTL expiration
- sorted range scans
- concurrent local operations during rebalancing
- TCP client/server operations
- concurrent TCP clients
- TCP TTL error propagation
- majority commit
- no-quorum rejection
- offline follower catch-up
- deterministic leader failover
- concurrent replicated writes

Run:

```bash
make test
make race
make bench
```
