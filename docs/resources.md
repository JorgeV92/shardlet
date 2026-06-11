# Learning Resources

This project is a compact learning implementation, so the most useful resources are the ones that map directly to its current code and roadmap: local concurrency, partitioned key/value storage, consensus, replicated logs, shard movement, and testing under failure.

## Start Here

- [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/) - the closest course match for Shardlet. The Raft, fault-tolerant key/value, and sharded key/value labs line up with `pkg/raftgroup`, `pkg/shardlet`, and the roadmap items around controllers and migration.
- [Raft: In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) - the main paper behind the replicated log model used by `pkg/raftgroup`. Focus first on leader election, log replication, commitment rules, and safety.
- [Designing Data-Intensive Applications](https://dataintensive.net/) - the best broad companion for replication, partitioning, consistency, transactions, and the tradeoffs behind distributed databases.
- [Jepsen: Consistency Models](https://jepsen.io/consistency/models) - a practical map of consistency guarantees. Read [Linearizability](https://jepsen.io/consistency/models/linearizable) when deciding what reads and writes should guarantee.

## Papers

- [Raft extended paper](https://raft.github.io/raft.pdf) - use for leader terms, log matching, majority commit, leader completeness, snapshots, and cluster membership changes.
- [Dynamo: Amazon's Highly Available Key-value Store](https://www.amazon.science/publications/dynamo-amazons-highly-available-key-value-store) - useful contrast to Shardlet's majority-commit path. Covers consistent hashing, sloppy quorum, hinted handoff, vector clocks, and anti-entropy.
- [Bigtable: A Distributed Storage System for Structured Data](https://research.google/pubs/pub27898) - shows how tablets, metadata, range partitioning, compaction, and distributed storage fit together in a production system.
- [Spanner: Google's Globally-Distributed Database](https://research.google/pubs/spanner-googles-globally-distributed-database-2/) - read after Raft basics. Useful for thinking about global transactions, externally consistent reads, and what changes when a key/value store becomes transactional.
- [Chain Replication for Supporting High Throughput and Availability](https://www.cs.cornell.edu/fbs/publications/ChainReplicOSDI.html) - a different replication design that is easier to compare against leader-based Raft replication.

## Courses And Notes

- [MIT 6.5840 schedule and labs](https://pdos.csail.mit.edu/6.824/schedule.html) - lecture notes, papers, and lab specs for MapReduce, Raft, fault-tolerant KV, and sharded KV.
- [MIT 6.824 OpenCourseWare](https://ocw.mit.edu/courses/6-824-distributed-computer-systems-engineering-spring-2006/) - older but still useful distributed-systems lectures and reading structure.
- [Distributed Systems lecture notes by Martin Kleppmann](https://www.cl.cam.ac.uk/teaching/2425/ConcDisSys/dist-sys-slides.pdf) - compact lecture material on replication, clocks, transactions, consensus, and distributed databases.

## Production Systems To Study

- [etcd](https://github.com/etcd-io/etcd) - production Go key/value store using Raft. Good reference for client APIs, watch streams, persistence, cluster membership, and operational testing.
- [etcd Raft](https://github.com/etcd-io/raft) - focused Go Raft library. Compare its `Ready` loop, log storage, snapshots, message passing, and membership changes to Shardlet's simpler in-memory model.
- [CockroachDB architecture overview](https://www.cockroachlabs.com/docs/stable/architecture/overview) - practical reference for ranges, replicas, leaseholders, Raft leaders, splitting, and rebalancing.
- [TiKV architecture](https://tikv.org/docs/3.0/concepts/architecture/) - a distributed key/value store with stores, regions, placement driver, and Raft replication.
- [Deep Dive TiKV: Multi-Raft](https://tikv.github.io/deep-dive-tikv/scalability/multi-raft.html) - directly relevant to Shardlet's roadmap if each shard or range eventually becomes its own Raft group.

## Go Concurrency

- [Go Data Race Detector](https://go.dev/doc/articles/race_detector) - practical guide for `go test -race`, which is already part of the project workflow.
- [Introducing the Go Race Detector](https://go.dev/blog/race-detector) - background on what the race detector can and cannot catch.
- [Go Concurrency Patterns: Pipelines and Cancellation](https://go.dev/blog/pipelines) - useful once the TCP server, replication loop, or shard migration work grows beyond simple mutex-protected state.
- [Go Memory Model](https://go.dev/ref/mem) - read when changing lock boundaries or mixing atomics, mutexes, and goroutines.

## Testing And Verification

- [Jepsen](https://jepsen.io/) - the standard source for learning how distributed databases fail under partitions, pauses, crashes, and clock issues.
- [Jepsen analyses](https://jepsen.io/analyses) - case studies to mine for test ideas, especially stale reads, split brain, lost writes, and partial failures.
- [Porcupine](https://github.com/anishathalye/porcupine) - Go linearizability checker that can be useful if Shardlet grows a history-recording integration test.
- [TLA+ Hyperbook](https://learntla.com/core/) - approachable route into modeling safety properties before implementing complicated shard migration or Raft membership changes.

## Implementation Roadmap Reading

Use these pairings when working through future project features:

| Feature | Read First |
| --- | --- |
| Randomized Raft elections and heartbeats | Raft paper sections 5.2 and 5.6 |
| Durable WAL and snapshots | Raft paper sections 5.3, 5.4, and 7; etcd Raft storage examples |
| Networked AppendEntries RPCs | Raft paper section 5; etcd Raft message loop |
| Controller process and shard configs | MIT 6.5840 sharded KV lab; TiKV placement driver docs |
| Idempotent shard migration | MIT 6.5840 sharded KV lab; Bigtable tablet movement; CockroachDB range rebalancing |
| Exactly-once client requests | Raft paper client interaction notes; MIT fault-tolerant KV lab |
| Consistency tests | Jepsen consistency models; Porcupine |
