# Architecture

Shardlet is intentionally small, but it is organized around concepts used by larger distributed key/value stores.

## Components

- `Store`: public API for client operations and cluster metadata.
- `shard`: owns a subset of keys and protects its state with an independent `sync.RWMutex`.
- shard group: a logical owner label for a shard. Today this is local metadata; later it can map to a replicated server group.
- demo command: starts many goroutines, writes and reads keys, and rebalances shard ownership in parallel.

## Concurrency Model

The store has one coarse lock for shard topology and one lock per shard for data. Operations briefly read the topology to find the shard, then operate on that shard's lock. This keeps unrelated keys independent and makes the hot path easy to inspect with `go test -race`.

`Rebalance` updates ownership labels without moving key/value data. In a distributed implementation, this is the place where shard freeze, install, and delete RPCs would be added.

## Correctness Notes

- `Get` lazily removes expired keys.
- `Snapshot` removes expired keys while copying live entries.
- `Range` returns keys in lexical order.
- `Rebalance` rejects empty group sets.
- Tests exercise concurrent readers, writers, and rebalancing.
