package main

import (
	"fmt"
	"log"
	"time"

	"shardlet/pkg/shardlet"
)

func main() {
	store := shardlet.MustNewStore(8, []string{"g1", "g2"})

	written, err := store.Put("user:42", "online", shardlet.PutOptions{TTL: time.Minute})
	if err != nil {
		log.Fatal(err)
	}

	value, err := store.Get("user:42")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("key=%s value=%s shard=%d group=%s version=%d\n",
		value.Key,
		value.Value,
		written.ShardID,
		written.GroupID,
		value.Version,
	)
}
