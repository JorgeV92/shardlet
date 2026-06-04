# Architecture

Shardlet is intentionally small, but it is organized around concepts used by larger distributed key/value stores.

## Components

- `Store`: public API for client operations and cluster metadata.
- `shard`: owns a subset of keys and protects its state with an independent `sync.RWMutex`.
- `raftgroup.Group`: replicated shard group with a leader log, majority commit, follower catch-up, and failover hooks.
- `shardletnet.Server`: TCP shard group API that accepts concurrent client connections.
- `shardletnet.Client`: reusable Go client for remote shard group operations.
- shard group: a logical owner label for a shard. Today this is local metadata; later it can map to a replicated server group.
- demo command: starts many goroutines, writes and reads keys, and rebalances shard ownership in parallel.

## Concurrency Model

The store has one coarse lock for shard topology and one lock per shard for data. Operations briefly read the topology to find the shard, then operate on that shard's lock. This keeps unrelated keys independent and makes the hot path easy to inspect with `go test -race`.

The TCP server uses a goroutine per connection. Each connection receives newline-delimited JSON requests and writes JSON responses. A single client serializes requests on its connection, while multiple clients can operate concurrently through independent TCP connections.

The Raft group layer serializes writes through the current leader. Each write becomes a log entry, is appended to online replicas, and is committed only if a majority acknowledges it. Committed entries are applied to each replica's local `Store`. Offline replicas catch up from the committed log when they return.

`Rebalance` updates ownership labels without moving key/value data. In a distributed implementation, this is the place where shard freeze, install, and delete RPCs would be added.

## Correctness Notes

- `Get` lazily removes expired keys.
- `Snapshot` removes expired keys while copying live entries.
- `Range` returns keys in lexical order.
- `Rebalance` rejects empty group sets.
- Network tests exercise remote reads, writes, deletes, range scans, stats, TTL errors, and concurrent TCP clients during rebalancing.
- Raft group tests exercise majority commit, no-quorum rejection, follower catch-up, leader failover, and concurrent replicated writes.

## Raft Scope

Shardlet's `raftgroup` package implements the replication path that matters for a portfolio-sized shard group: leader log append, majority commit, deterministic failover, and replica catch-up. It does not yet implement randomized elections, heartbeat timers, durable WAL storage, or networked AppendEntries RPCs. Those are the next steps toward production-grade Raft.
