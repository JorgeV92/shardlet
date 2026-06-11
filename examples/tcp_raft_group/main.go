package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"shardlet/pkg/raftgroup"
	"shardlet/pkg/shardlet"
	"shardlet/pkg/shardletnet"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group := raftgroup.MustNewGroup("g1", []string{"n1", "n2", "n3"}, 16)
	server, err := shardletnet.ListenRaftGroup(ctx, "127.0.0.1:0", group, shardletnet.ServerOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	client, err := shardletnet.Dial(ctx, server.Addr().String(), shardletnet.ClientOptions{Timeout: time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Put("cart:42", "paid", shardlet.PutOptions{}); err != nil {
		log.Fatal(err)
	}
	value, err := client.Get("cart:42")
	if err != nil {
		log.Fatal(err)
	}
	stats, err := client.RaftStats()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("remote value=%s leader=%s commitIndex=%d\n",
		value.Value,
		stats.LeaderID,
		stats.CommitIndex,
	)
}
