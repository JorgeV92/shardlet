package shardletnet

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"shardlet/pkg/raftgroup"
	"shardlet/pkg/shardlet"
)

type Client struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
	timeout time.Duration
	mu      sync.Mutex
	seq     atomic.Uint64
}

type ClientOptions struct {
	Timeout time.Duration
}

func Dial(ctx context.Context, addr string, opts ClientOptions) (*Client, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 2 * time.Second
	}

	return &Client{
		conn:    conn,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(bufio.NewReader(conn)),
		timeout: timeout,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Put(key, value string, opts shardlet.PutOptions) (shardlet.Value, error) {
	resp, err := c.roundTrip(Request{Op: OpPut, Key: key, Value: value, TTL: opts.TTL})
	if err != nil {
		return shardlet.Value{}, err
	}
	return *resp.Value, nil
}

func (c *Client) Get(key string) (shardlet.Value, error) {
	resp, err := c.roundTrip(Request{Op: OpGet, Key: key})
	if err != nil {
		return shardlet.Value{}, err
	}
	return *resp.Value, nil
}

func (c *Client) Delete(key string) (bool, error) {
	resp, err := c.roundTrip(Request{Op: OpDelete, Key: key})
	if err != nil {
		return false, err
	}
	return resp.Deleted, nil
}

func (c *Client) Range(low, high string) ([]shardlet.Value, error) {
	resp, err := c.roundTrip(Request{Op: OpRange, Low: low, High: high})
	if err != nil {
		return nil, err
	}
	return resp.Values, nil
}

func (c *Client) Stats() (shardlet.ClusterStats, error) {
	resp, err := c.roundTrip(Request{Op: OpStats})
	if err != nil {
		return shardlet.ClusterStats{}, err
	}
	return *resp.Stats, nil
}

func (c *Client) RaftStats() (raftgroup.GroupStats, error) {
	resp, err := c.roundTrip(Request{Op: OpRaftStats})
	if err != nil {
		return raftgroup.GroupStats{}, err
	}
	return *resp.Raft, nil
}

func (c *Client) Rebalance(groups []string) error {
	_, err := c.roundTrip(Request{Op: OpRebalance, Groups: groups})
	return err
}

func (c *Client) roundTrip(req Request) (Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req.Version = ProtocolVersion
	req.ID = fmt.Sprintf("req-%d", c.seq.Add(1))

	if timeout := deadline(c.timeout); !timeout.IsZero() {
		if err := c.conn.SetDeadline(timeout); err != nil {
			return Response{}, err
		}
		defer c.conn.SetDeadline(time.Time{})
	}

	if err := c.encoder.Encode(req); err != nil {
		return Response{}, err
	}

	var resp Response
	if err := c.decoder.Decode(&resp); err != nil {
		return Response{}, err
	}
	if resp.ID != req.ID {
		return Response{}, fmt.Errorf("response id %q does not match request id %q", resp.ID, req.ID)
	}
	if !resp.OK {
		return Response{}, fmt.Errorf("%w: %s", ErrRemote, resp.Error)
	}
	return resp, nil
}
