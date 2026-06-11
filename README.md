# Shardlet

Shardlet is a compact Go project that models the core pieces of a sharded key/value store: concurrent shards, shard-group ownership, TCP clients, Raft-style replicated shard groups, TTL expiration, range scans, snapshots, and live rebalancing.


## Highlights

- **Go concurrency:** per-shard `sync.RWMutex` locking, goroutine-heavy workloads, concurrent TCP clients, and race-detector validation.
- **Sharding:** deterministic FNV-1a key placement across logical shards.
- **TCP API:** newline-delimited JSON protocol between remote clients and shard groups.
- **Raft-style replication:** leader log, majority commit, follower catch-up, no-quorum rejection, and deterministic leader promotion.
- **Storage features:** `Put`, `Get`, `Delete`, TTL expiration, sorted range scans, snapshots, and cluster stats.
- **Verification:** unit tests, network tests, concurrent replication tests, race tests, and benchmarks.

## Quick Start

```bash
make test
make race
make bench
make demo
```

Run the local concurrent workload:

```bash
go run ./cmd/shardlet -workers 32 -ops 100000
```

Start a TCP shard group API:

```bash
go run ./cmd/shardlet -listen 127.0.0.1:7070
```

## Packages

| Package | Purpose |
| --- | --- |
| `pkg/shardlet` | In-memory sharded key/value store with per-shard locking. |
| `pkg/shardletnet` | TCP JSON client/server API for remote shard group operations. |
| `pkg/raftgroup` | Raft-style replicated shard group with pluggable in-memory or file-backed logs. |
| `cmd/shardlet` | Demo workload runner and TCP server entrypoint. |

## Walkthrough: How Shardlet Works

This example shows the main pieces working together. It starts with the local sharded store, moves shard ownership, then uses a replicated shard group to demonstrate majority commit, failover, and follower catch-up.

```go
package main

import (
    "errors"
    "fmt"

    "shardlet/pkg/raftgroup"
    "shardlet/pkg/shardlet"
)

func main() {
    // 1. A Store spreads keys across logical shards. Each shard has its own lock,
    // so unrelated keys can be read and written concurrently.
    store := shardlet.MustNewStore(8, []string{"g1", "g2"})

    alice, _ := store.Put("user:alice", "online", shardlet.PutOptions{})
    bob, _ := store.Put("user:bob", "offline", shardlet.PutOptions{})

    fmt.Printf("alice shard=%d group=%s\n", alice.ShardID, alice.GroupID)
    fmt.Printf("bob   shard=%d group=%s\n", bob.ShardID, bob.GroupID)

    // 2. Rebalance changes shard ownership metadata while preserving data.
    // In the full distributed version, this is where shard migration RPCs belong.
    _ = store.Rebalance([]string{"g1", "g2", "g3"})

    stats := store.Stats()
    fmt.Printf("local store: shards=%d groups=%d keys=%d\n",
        stats.ShardCount, stats.GroupCount, stats.KeyCount)

    // 3. A raftgroup models one replicated shard group. Writes go to the
    // current leader, are appended to online replicas, and commit only after
    // a majority acknowledges the log entry.
    group := raftgroup.MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8)

    _, _ = group.Put("cart:42", "created", shardlet.PutOptions{})

    // 4. A follower can go offline. With 2 of 3 replicas still online, writes
    // continue because the group still has a majority.
    _ = group.SetOnline("n3", false)
    _, _ = group.Put("cart:42", "paid", shardlet.PutOptions{})

    // 5. If the leader is lost, writes fail until another caught-up replica
    // is promoted. The current implementation uses deterministic promotion
    // instead of randomized Raft elections.
    _ = group.SetOnline("n1", false)
    if _, err := group.Put("cart:42", "shipped", shardlet.PutOptions{}); err != nil {
        fmt.Println("write blocked:", err)
    }

    _ = group.SetOnline("n3", true)
    _ = group.PromoteLeader("n2")
    _, _ = group.Put("cart:42", "shipped", shardlet.PutOptions{})

    // 6. If quorum is lost, writes are rejected. This prevents a minority from
    // committing divergent state.
    _ = group.SetOnline("n3", false)
    if _, err := group.Put("cart:42", "delivered", shardlet.PutOptions{}); errors.Is(err, raftgroup.ErrNoQuorum) {
        fmt.Println("write rejected without quorum")
    }

    // 7. When replicas return, they catch up from the committed log.
    _ = group.SetOnline("n1", true)
    _ = group.SetOnline("n3", true)

    value, _ := group.Get("cart:42")
    raftStats := group.Stats()
    fmt.Printf("replicated value=%q leader=%s commitIndex=%d\n",
        value.Value, raftStats.LeaderID, raftStats.CommitIndex)
}
```

The important idea is that `pkg/shardlet` owns local concurrency and key placement, while `pkg/raftgroup` owns replicated write ordering. `pkg/shardletnet` can expose the store over TCP for remote clients, and future work can connect those network boundaries to replica-to-replica Raft RPCs.

## Shardlet Examples

Use the examples below based on what you want to exercise:

| Example | Use when |
| --- | --- |
| Local store | You want shard placement, TTLs, range scans, and local concurrency. |
| TCP client | You want to access a shardlet store from another process. |
| Replicated shard group | You want majority commit, follower catch-up, and failover behavior. |
| Durable raftgroup log | You want committed replicated writes to survive process restart. |

### Local Store

Runnable version: `go run ./examples/local_store`

```go
store := shardlet.MustNewStore(64, []string{"g1", "g2", "g3"})

_, _ = store.Put("user:42", "Jorge", shardlet.PutOptions{})

value, err := store.Get("user:42")
if err != nil {
    // handle missing or expired key
}

fmt.Println(value.Value, value.ShardID, value.GroupID)
```

### TCP Client

Shardlet's TCP protocol uses one newline-delimited JSON request per operation. The Go client serializes requests on a connection, while the server handles many client connections concurrently.

```go
client, err := shardletnet.Dial(ctx, "127.0.0.1:7070", shardletnet.ClientOptions{})
if err != nil {
    // handle dial failure
}
defer client.Close()

_, _ = client.Put("user:42", "Jorge", shardlet.PutOptions{})
value, _ := client.Get("user:42")

fmt.Println(value.Value)
```

### Replicated Shard Group

`pkg/raftgroup` wraps `pkg/shardlet.Store` with a Raft-style replicated log. Writes go through the current leader, append to online replicas, commit after a majority acknowledges, and then apply to each replica's local store.

Runnable version: `go run ./examples/replicated_group`

```go
group := raftgroup.MustNewGroup("g1", []string{"n1", "n2", "n3"}, 64)

_, _ = group.Put("user:42", "Jorge", shardlet.PutOptions{})

_ = group.SetOnline("n1", false)
_ = group.PromoteLeader("n2")

value, _ := group.Get("user:42")
fmt.Println(value.Value)
```

### Durable Raftgroup Log

By default, `raftgroup` uses an in-memory log store. For restartable examples, use `NewFileLogStore` and pass it with `WithLogStore`.

Runnable version: `go run ./examples/durable_log`

```go
logStore, err := raftgroup.NewFileLogStore("./data/g1")
if err != nil {
    // handle log directory setup failure
}

group := raftgroup.MustNewGroup(
    "g1",
    []string{"n1", "n2", "n3"},
    64,
    raftgroup.WithLogStore(logStore),
)

_, _ = group.Put("order:100", "created", shardlet.PutOptions{})
_, _ = group.Put("order:100", "paid", shardlet.PutOptions{})

// Later, recreate the group with the same replica IDs and log directory.
restoredLogs, _ := raftgroup.NewFileLogStore("./data/g1")
restored := raftgroup.MustNewGroup(
    "g1",
    []string{"n1", "n2", "n3"},
    64,
    raftgroup.WithLogStore(restoredLogs),
)

value, _ := restored.Get("order:100")
fmt.Println(value.Value) // paid
```

## Testing

```bash
go test ./...
go test -race ./...
go test -bench=. -benchmem ./pkg/shardlet
```

The tests cover:

- concurrent local reads, writes, deletes, and rebalancing
- TTL expiration and cleanup
- sorted range scans
- TCP `Put`, `Get`, `Delete`, `Range`, `Stats`, and `Rebalance`
- concurrent TCP clients
- majority commit and no-quorum rejection
- offline follower catch-up
- in-memory and file-backed raftgroup log restore
- deterministic leader failover
- concurrent replicated writes

## Learning Resources

See [docs/resources.md](docs/resources.md) for papers, courses, production repositories, and testing references that map to Shardlet's storage, networking, Raft-style replication, sharding, and roadmap work.

## Scope

Shardlet currently focuses on the core mechanics that are useful for demonstrating Go and distributed-systems fundamentals.

Implemented:

- local sharded storage engine
- TCP client/server protocol
- in-memory replicated shard group
- pluggable raftgroup log storage
- file-backed committed log restore
- majority-based write commit
- deterministic failover hooks
- testable concurrency behavior

Not yet implemented:

- randomized Raft elections
- heartbeat timers
- compacted snapshot persistence
- networked AppendEntries RPCs between replicas
- controller process with persisted current and next configs
- idempotent shard migration protocol

## Roadmap

- Add compacted snapshots for `raftgroup`.
- Replace deterministic leader promotion with Raft elections and heartbeats.
- Add networked replica-to-replica AppendEntries.
- Add a controller process that stores current and next shard configurations.
- Implement idempotent shard freeze, install, and delete migration.
- Add exactly-once client request handling.
- Add Prometheus metrics for latency, lock contention, quorum failures, and shard movement.
