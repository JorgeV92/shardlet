package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JorgeV92/shardlet/pkg/shardlet"
)

func main() {
	shards := flag.Int("shards", 64, "number of logical shards")
	workers := flag.Int("workers", runtime.NumCPU()*4, "concurrent client workers")
	ops := flag.Int("ops", 100_000, "total put/get operations")
	flag.Parse()

	store := shardlet.MustNewStore(*shards, []string{"g1", "g2", "g3"})
	start := time.Now()

	var done atomic.Int64
	var wg sync.WaitGroup
	for workerID := 0; workerID < *workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID) + time.Now().UnixNano()))
			for {
				n := int(done.Add(1))
				if n > *ops {
					return
				}
				key := fmt.Sprintf("tenant:%02d:key:%05d", workerID%16, rng.Intn(20_000))
				value := fmt.Sprintf("worker=%d op=%d", workerID, n)
				if _, err := store.Put(key, value, shardlet.PutOptions{}); err != nil {
					log.Fatal(err)
				}
				if _, err := store.Get(key); err != nil {
					log.Fatal(err)
				}
			}
		}(workerID)
	}

	go func() {
		ticker := time.NewTicker(15 * time.Millisecond)
		defer ticker.Stop()
		for done.Load() < int64(*ops) {
			<-ticker.C
			if err := store.Rebalance([]string{"g1", "g2", "g3", "g4"}); err != nil {
				log.Fatal(err)
			}
			if err := store.Rebalance([]string{"g1", "g2", "g3"}); err != nil {
				log.Fatal(err)
			}
		}
	}()

	wg.Wait()
	elapsed := time.Since(start)
	stats := store.Stats()
	fmt.Printf("Shardlet completed %d put/get pairs with %d workers in %s\n", *ops, *workers, elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput: %.0f ops/sec\n", float64(*ops*2)/elapsed.Seconds())
	fmt.Printf("Shards: %d  Groups: %d  Keys: %d\n", stats.ShardCount, stats.GroupCount, stats.KeyCount)
}
