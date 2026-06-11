package main

import (
	"fmt"
	"log"
	"os"

	"shardlet/pkg/raftgroup"
	"shardlet/pkg/shardlet"
)

func main() {
	dir, err := os.MkdirTemp("", "shardlet-raftlog-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	logStore, err := raftgroup.NewFileLogStore(dir)
	if err != nil {
		log.Fatal(err)
	}
	group := raftgroup.MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8, raftgroup.WithLogStore(logStore))

	if _, err := group.Put("order:100", "created", shardlet.PutOptions{}); err != nil {
		log.Fatal(err)
	}
	if _, err := group.Put("order:100", "paid", shardlet.PutOptions{}); err != nil {
		log.Fatal(err)
	}

	restoredLogs, err := raftgroup.NewFileLogStore(dir)
	if err != nil {
		log.Fatal(err)
	}
	restored := raftgroup.MustNewGroup("g1", []string{"n1", "n2", "n3"}, 8, raftgroup.WithLogStore(restoredLogs))

	value, err := restored.Get("order:100")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("restored value=%s commitIndex=%d\n", value.Value, restored.Stats().CommitIndex)
}
