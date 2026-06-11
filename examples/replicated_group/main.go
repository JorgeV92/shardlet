package main

import (
	"errors"
	"fmt"
	"log"

	"shardlet/pkg/raftgroup"
	"shardlet/pkg/shardlet"
)

func main() {
	group := raftgroup.MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8)

	if _, err := group.Put("cart:42", "created", shardlet.PutOptions{}); err != nil {
		log.Fatal(err)
	}
	if err := group.SetOnline("n3", false); err != nil {
		log.Fatal(err)
	}
	if _, err := group.Put("cart:42", "paid", shardlet.PutOptions{}); err != nil {
		log.Fatal(err)
	}
	if err := group.SetOnline("n2", false); err != nil {
		log.Fatal(err)
	}
	if _, err := group.Put("cart:42", "shipped", shardlet.PutOptions{}); errors.Is(err, raftgroup.ErrNoQuorum) {
		fmt.Println("write rejected without quorum")
	}

	stats := group.Stats()
	fmt.Printf("leader=%s commitIndex=%d replicas=%d\n",
		stats.LeaderID,
		stats.CommitIndex,
		len(stats.Replicas),
	)
}
