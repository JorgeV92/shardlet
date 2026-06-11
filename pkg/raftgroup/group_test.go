package raftgroup

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"shardlet/pkg/shardlet"
)

func TestReplicatesCommittedWriteToMajority(t *testing.T) {
	group := MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8)

	written, err := group.Put("user:1", "jorge", shardlet.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if written.Value != "jorge" {
		t.Fatalf("value = %q, want jorge", written.Value)
	}

	for _, id := range []string{"n1", "n2", "n3"} {
		value, err := group.ReplicaGet(id, "user:1")
		if err != nil {
			t.Fatalf("ReplicaGet(%s): %v", id, err)
		}
		if value.Value != "jorge" {
			t.Fatalf("ReplicaGet(%s) = %q, want jorge", id, value.Value)
		}
	}

	stats := group.Stats()
	if stats.CommitIndex != 1 {
		t.Fatalf("commit index = %d, want 1", stats.CommitIndex)
	}
}

func TestOfflineFollowerCatchesUp(t *testing.T) {
	group := MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8)

	if err := group.SetOnline("n3", false); err != nil {
		t.Fatal(err)
	}
	if _, err := group.Put("k1", "v1", shardlet.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := group.ReplicaGet("n3", "k1"); !errors.Is(err, ErrReplicaOffline) {
		t.Fatalf("ReplicaGet offline err = %v, want %v", err, ErrReplicaOffline)
	}

	if err := group.SetOnline("n3", true); err != nil {
		t.Fatal(err)
	}
	value, err := group.ReplicaGet("n3", "k1")
	if err != nil {
		t.Fatal(err)
	}
	if value.Value != "v1" {
		t.Fatalf("caught-up value = %q, want v1", value.Value)
	}
}

func TestRejectsWriteWithoutQuorum(t *testing.T) {
	group := MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8)

	if err := group.SetOnline("n2", false); err != nil {
		t.Fatal(err)
	}
	if err := group.SetOnline("n3", false); err != nil {
		t.Fatal(err)
	}

	if _, err := group.Put("k1", "v1", shardlet.PutOptions{}); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("Put err = %v, want %v", err, ErrNoQuorum)
	}
	if _, err := group.ReplicaGet("n1", "k1"); !errors.Is(err, shardlet.ErrKeyNotFound) {
		t.Fatalf("leader value err = %v, want %v", err, shardlet.ErrKeyNotFound)
	}
}

func TestLeaderFailover(t *testing.T) {
	group := MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8)

	if _, err := group.Put("before", "leader-n1", shardlet.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := group.SetOnline("n1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := group.Put("blocked", "old-leader-down", shardlet.PutOptions{}); !errors.Is(err, ErrReplicaOffline) {
		t.Fatalf("Put with offline leader err = %v, want %v", err, ErrReplicaOffline)
	}

	if err := group.PromoteLeader("n2"); err != nil {
		t.Fatal(err)
	}
	if group.LeaderID() != "n2" {
		t.Fatalf("leader = %q, want n2", group.LeaderID())
	}
	if _, err := group.Put("after", "leader-n2", shardlet.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	value, err := group.Get("before")
	if err != nil {
		t.Fatal(err)
	}
	if value.Value != "leader-n1" {
		t.Fatalf("before = %q, want leader-n1", value.Value)
	}
	value, err = group.Get("after")
	if err != nil {
		t.Fatal(err)
	}
	if value.Value != "leader-n2" {
		t.Fatalf("after = %q, want leader-n2", value.Value)
	}
}

func TestConcurrentReplicatedWrites(t *testing.T) {
	group := MustNewGroup("g1", []string{"n1", "n2", "n3", "n4", "n5"}, 16)

	const workers = 16
	const opsPerWorker = 200
	var wg sync.WaitGroup
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("worker:%02d:key:%03d", workerID, i)
				if _, err := group.Put(key, "value", shardlet.PutOptions{}); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}(workerID)
	}
	wg.Wait()

	stats := group.Stats()
	wantKeys := workers * opsPerWorker
	if stats.CommitIndex != uint64(wantKeys) {
		t.Fatalf("commit index = %d, want %d", stats.CommitIndex, wantKeys)
	}
	for _, replica := range stats.Replicas {
		if replica.KeyCount != wantKeys {
			t.Fatalf("replica %s key count = %d, want %d", replica.ID, replica.KeyCount, wantKeys)
		}
	}
}

func TestRestoresCommittedLogFromMemoryStore(t *testing.T) {
	logs := NewMemoryLogStore()
	group := MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8, WithLogStore(logs))

	if _, err := group.Put("user:1", "jorge", shardlet.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := group.Put("user:2", "ana", shardlet.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	restored := MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8, WithLogStore(logs))
	value, err := restored.Get("user:1")
	if err != nil {
		t.Fatal(err)
	}
	if value.Value != "jorge" {
		t.Fatalf("restored user:1 = %q, want jorge", value.Value)
	}
	value, err = restored.Get("user:2")
	if err != nil {
		t.Fatal(err)
	}
	if value.Value != "ana" {
		t.Fatalf("restored user:2 = %q, want ana", value.Value)
	}
	if stats := restored.Stats(); stats.CommitIndex != 2 {
		t.Fatalf("restored commit index = %d, want 2", stats.CommitIndex)
	}
}

func TestRestoresCommittedLogFromFileStore(t *testing.T) {
	dir := t.TempDir()
	logs, err := NewFileLogStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	group := MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8, WithLogStore(logs))

	if _, err := group.Put("cart:42", "created", shardlet.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := group.Put("cart:42", "paid", shardlet.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if deleted, err := group.Delete("cart:42"); err != nil || !deleted {
		t.Fatalf("Delete = %v, %v; want true, nil", deleted, err)
	}

	restoredLogs, err := NewFileLogStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	restored := MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8, WithLogStore(restoredLogs))

	if _, err := restored.Get("cart:42"); !errors.Is(err, shardlet.ErrKeyNotFound) {
		t.Fatalf("restored deleted key err = %v, want %v", err, shardlet.ErrKeyNotFound)
	}
	if stats := restored.Stats(); stats.CommitIndex != 3 {
		t.Fatalf("restored commit index = %d, want 3", stats.CommitIndex)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("file log count = %d, want 3", len(entries))
	}
}

func TestNoQuorumWriteDoesNotPersistLogEntry(t *testing.T) {
	logs := NewMemoryLogStore()
	group := MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8, WithLogStore(logs))

	if err := group.SetOnline("n2", false); err != nil {
		t.Fatal(err)
	}
	if err := group.SetOnline("n3", false); err != nil {
		t.Fatal(err)
	}
	if _, err := group.Put("k1", "v1", shardlet.PutOptions{}); !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("Put err = %v, want %v", err, ErrNoQuorum)
	}

	for _, replicaID := range []string{"n1", "n2", "n3"} {
		entries, err := logs.Load(replicaID)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("replica %s persisted %d entries, want 0", replicaID, len(entries))
		}
	}
}
