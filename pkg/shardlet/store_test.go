package shardlet

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStorePutGetDelete(t *testing.T) {
	store := MustNewStore(8, []string{"a", "b"})

	written, err := store.Put("user:1", "jorge", PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if written.Version != 1 {
		t.Fatalf("version = %d, want 1", written.Version)
	}

	read, err := store.Get("user:1")
	if err != nil {
		t.Fatal(err)
	}
	if read.Value != "jorge" || read.ShardID != written.ShardID {
		t.Fatalf("read = %+v, written = %+v", read, written)
	}

	deleted, err := store.Delete("user:1")
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("delete returned false")
	}
	if _, err := store.Get("user:1"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get after Delete err = %v, want %v", err, ErrKeyNotFound)
	}
}

func TestTTLExpiration(t *testing.T) {
	store := MustNewStore(4, []string{"a"})
	if _, err := store.Put("session", "live", PutOptions{TTL: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	if _, err := store.Get("session"); !errors.Is(err, ErrKeyExpired) {
		t.Fatalf("Get expired err = %v, want %v", err, ErrKeyExpired)
	}
	if _, err := store.Get("session"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get cleaned expired err = %v, want %v", err, ErrKeyNotFound)
	}
}

func TestRangeReturnsSortedKeys(t *testing.T) {
	store := MustNewStore(8, []string{"a", "b"})
	for _, key := range []string{"user:3", "user:1", "user:2", "audit:1"} {
		if _, err := store.Put(key, key+"-value", PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	values := store.Range("user:", "user;")
	got := make([]string, 0, len(values))
	for _, value := range values {
		got = append(got, value.Key)
	}
	want := []string{"user:1", "user:2", "user:3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Range keys = %v, want %v", got, want)
		}
	}
}

func TestConcurrentAccessAndRebalance(t *testing.T) {
	store := MustNewStore(32, []string{"g1", "g2"})

	const workers = 24
	const opsPerWorker = 500
	var wg sync.WaitGroup
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("worker:%02d:key:%03d", workerID, i%100)
				if _, err := store.Put(key, fmt.Sprintf("%d", i), PutOptions{}); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if _, err := store.Get(key); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}(workerID)
	}

	for i := 0; i < 10; i++ {
		if err := store.Rebalance([]string{"g1", "g2", "g3"}); err != nil {
			t.Fatal(err)
		}
		if err := store.Rebalance([]string{"g1", "g2"}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()

	stats := store.Stats()
	if stats.KeyCount == 0 {
		t.Fatal("expected keys after concurrent workload")
	}
}

func BenchmarkConcurrentPutGet(b *testing.B) {
	store := MustNewStore(64, []string{"g1", "g2", "g3", "g4"})

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key:%d", i%4096)
			if _, err := store.Put(key, "value", PutOptions{}); err != nil {
				b.Fatal(err)
			}
			if _, err := store.Get(key); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
