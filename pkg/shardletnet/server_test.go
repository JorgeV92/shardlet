package shardletnet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"shardlet/pkg/shardlet"
)

func startTestServer(t *testing.T) (*Server, *Client) {
	t.Helper()

	store := shardlet.MustNewStore(16, []string{"g1", "g2"})
	srv, err := Listen(context.Background(), "127.0.0.1:0", store, ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Logf("server close: %v", err)
		}
	})

	client, err := Dial(context.Background(), srv.Addr().String(), ClientOptions{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("client close: %v", err)
		}
	})
	return srv, client
}

func TestTCPPutGetDelete(t *testing.T) {
	_, client := startTestServer(t)

	written, err := client.Put("user:1", "jorge", shardlet.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	read, err := client.Get("user:1")
	if err != nil {
		t.Fatal(err)
	}
	if read.Value != "jorge" || read.ShardID != written.ShardID {
		t.Fatalf("read = %+v, written = %+v", read, written)
	}

	deleted, err := client.Delete("user:1")
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("Delete returned false")
	}
	if _, err := client.Get("user:1"); !errors.Is(err, ErrRemote) {
		t.Fatalf("Get missing err = %v, want %v", err, ErrRemote)
	}
}

func TestTCPRangeStatsAndRebalance(t *testing.T) {
	_, client := startTestServer(t)

	for _, key := range []string{"user:3", "user:1", "user:2", "audit:1"} {
		if _, err := client.Put(key, "value", shardlet.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Rebalance([]string{"g1", "g2", "g3"}); err != nil {
		t.Fatal(err)
	}

	values, err := client.Range("user:", "user;")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || values[0].Key != "user:1" || values[2].Key != "user:3" {
		t.Fatalf("Range = %+v", values)
	}

	stats, err := client.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.GroupCount != 3 || stats.KeyCount != 4 {
		t.Fatalf("Stats = %+v", stats)
	}
}

func TestTCPConcurrentClients(t *testing.T) {
	srv, _ := startTestServer(t)

	const clients = 12
	const opsPerClient = 200
	var wg sync.WaitGroup
	for clientID := 0; clientID < clients; clientID++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			client, err := Dial(context.Background(), srv.Addr().String(), ClientOptions{Timeout: time.Second})
			if err != nil {
				t.Errorf("Dial: %v", err)
				return
			}
			defer client.Close()

			for i := 0; i < opsPerClient; i++ {
				key := fmt.Sprintf("client:%02d:key:%03d", clientID, i%50)
				if _, err := client.Put(key, fmt.Sprintf("%d", i), shardlet.PutOptions{}); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if _, err := client.Get(key); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if i%25 == 0 {
					if err := client.Rebalance([]string{"g1", "g2", "g3"}); err != nil {
						t.Errorf("Rebalance: %v", err)
						return
					}
				}
			}
		}(clientID)
	}
	wg.Wait()
}

func TestTCPTTLRemoteError(t *testing.T) {
	_, client := startTestServer(t)

	if _, err := client.Put("session", "live", shardlet.PutOptions{TTL: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	if _, err := client.Get("session"); !errors.Is(err, ErrRemote) {
		t.Fatalf("Get expired err = %v, want %v", err, ErrRemote)
	}
}
