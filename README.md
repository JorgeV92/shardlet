# Shardlet

Shardlet is a small Go project for experimenting with the building blocks behind a sharded key/value store: logical shards, shard-group ownership, concurrent clients, TTL cleanup, range scans, snapshots, and live rebalancing.

The project is inspired by MIT 6.5840's sharded key/value service lab, but it is written as a portfolio-friendly Go codebase with a focused API, tests, benchmarks, and a concurrent demo workload.

## Why this project exists

Shardlet is meant to show practical Go experience beyond syntax:

- `sync.RWMutex` based per-shard concurrency
- goroutine-heavy client workloads
- race-detector friendly shared-state design
- deterministic shard placement
- live shard-group rebalancing while clients read and write
- table-driven tests and benchmarks

## Quick Start

```bash
go test ./...
go test -race ./...
go run ./cmd/shardlet -workers 32 -ops 100000
```

Or use the Makefile:

```bash
make test
make race
make demo
make bench
```

## Example

```go
store := shardlet.MustNewStore(64, []string{"g1", "g2", "g3"})

_, _ = store.Put("user:42", "Jorge", shardlet.PutOptions{})

value, err := store.Get("user:42")
if err != nil {
    // handle missing or expired key
}

fmt.Println(value.Value, value.ShardID, value.GroupID)
```

## Design

Shardlet splits keys across a fixed number of logical shards using FNV-1a hashing. Each shard owns its own map and lock, so unrelated keys can be read and written concurrently without a global bottleneck.

Shard groups are represented as ownership labels. `Rebalance` updates shard ownership while clients continue operating. The current implementation keeps storage local and in-memory, which makes it easy to test the concurrency model before adding RPC replication.

## Current Features

- Concurrent `Put`, `Get`, and `Delete`
- TTL-based expiration
- Sorted range scans
- Point-in-time snapshots
- Shard and cluster stats
- Concurrent rebalancing demo
- Unit tests, race tests, and benchmark target

## Roadmap

These are natural next steps if this project grows toward the full distributed systems version:

- TCP or gRPC API between clients and shard groups
- Raft-backed replication per shard group
- controller process with persisted current and next configs
- idempotent shard migration protocol
- exactly-once client request handling
- Prometheus metrics for latency, shard movement, and lock contention

## Resume Bullet

Built **Shardlet**, a Go-based sharded key/value store prototype with per-shard locking, concurrent client workloads, live shard-group rebalancing, TTL expiration, snapshots, range scans, tests, benchmarks, and race-detector validation.
