package shardletnet

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"shardlet/pkg/raftgroup"
	"shardlet/pkg/shardlet"
)

type Backend interface {
	Put(key, value string, opts shardlet.PutOptions) (shardlet.Value, error)
	Get(key string) (shardlet.Value, error)
	Delete(key string) (bool, error)
	Range(low, high string) ([]shardlet.Value, error)
	Stats() (shardlet.ClusterStats, error)
	Rebalance(groups []string) error
}

type RaftStatsBackend interface {
	Backend
	RaftStats() raftgroup.GroupStats
}

type Server struct {
	backend  Backend
	listener net.Listener
	logger   *slog.Logger

	done     chan struct{}
	closeMux sync.Once
	wg       sync.WaitGroup
	connMu   sync.Mutex
	conns    map[net.Conn]struct{}
}

type ServerOptions struct {
	Logger *slog.Logger
}

func Listen(ctx context.Context, addr string, store *shardlet.Store, opts ServerOptions) (*Server, error) {
	if store == nil {
		return nil, errors.New("nil store")
	}
	return ListenBackend(ctx, addr, storeBackend{store: store}, opts)
}

func ListenRaftGroup(ctx context.Context, addr string, group *raftgroup.Group, opts ServerOptions) (*Server, error) {
	if group == nil {
		return nil, errors.New("nil raft group")
	}
	return ListenBackend(ctx, addr, raftGroupBackend{group: group}, opts)
}

func ListenBackend(ctx context.Context, addr string, backend Backend, opts ServerOptions) (*Server, error) {
	if backend == nil {
		return nil, errors.New("nil backend")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	srv := &Server{
		backend:  backend,
		listener: listener,
		logger:   logger,
		done:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
	srv.wg.Add(1)
	go srv.acceptLoop()

	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	return srv, nil
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) Close() error {
	var err error
	s.closeMux.Do(func() {
		close(s.done)
		err = s.listener.Close()
		s.connMu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.connMu.Unlock()
		s.wg.Wait()
	})
	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				s.logger.Debug("accept failed", "err", err)
				continue
			}
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	s.trackConn(conn)
	defer s.untrackConn(conn)

	reader := bufio.NewReader(conn)
	encoder := json.NewEncoder(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.logger.Debug("read failed", "remote", conn.RemoteAddr(), "err", err)
			}
			return
		}

		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			_ = encoder.Encode(Response{
				Version: ProtocolVersion,
				OK:      false,
				Error:   fmt.Sprintf("decode request: %v", err),
			})
			continue
		}
		_ = encoder.Encode(s.apply(req))
	}
}

func (s *Server) trackConn(conn net.Conn) {
	s.connMu.Lock()
	s.conns[conn] = struct{}{}
	s.connMu.Unlock()
}

func (s *Server) untrackConn(conn net.Conn) {
	s.connMu.Lock()
	delete(s.conns, conn)
	s.connMu.Unlock()
}

func (s *Server) apply(req Request) Response {
	resp := Response{
		Version: ProtocolVersion,
		ID:      req.ID,
		OK:      true,
	}
	if req.Version != ProtocolVersion {
		resp.OK = false
		resp.Error = fmt.Sprintf("unsupported protocol version %q", req.Version)
		return resp
	}

	switch req.Op {
	case OpPut:
		value, err := s.backend.Put(req.Key, req.Value, shardlet.PutOptions{TTL: req.TTL})
		if err != nil {
			return fail(req, err)
		}
		resp.Value = &value
	case OpGet:
		value, err := s.backend.Get(req.Key)
		if err != nil {
			return fail(req, err)
		}
		resp.Value = &value
	case OpDelete:
		deleted, err := s.backend.Delete(req.Key)
		if err != nil {
			return fail(req, err)
		}
		resp.Deleted = deleted
	case OpRange:
		values, err := s.backend.Range(req.Low, req.High)
		if err != nil {
			return fail(req, err)
		}
		resp.Values = values
	case OpStats:
		stats, err := s.backend.Stats()
		if err != nil {
			return fail(req, err)
		}
		resp.Stats = &stats
	case OpRaftStats:
		backend, ok := s.backend.(RaftStatsBackend)
		if !ok {
			return fail(req, errors.New("raft stats unavailable"))
		}
		stats := backend.RaftStats()
		resp.Raft = &stats
	case OpRebalance:
		if err := s.backend.Rebalance(req.Groups); err != nil {
			return fail(req, err)
		}
	default:
		return fail(req, fmt.Errorf("unknown operation %q", req.Op))
	}

	return resp
}

func fail(req Request, err error) Response {
	return Response{
		Version: ProtocolVersion,
		ID:      req.ID,
		OK:      false,
		Error:   err.Error(),
	}
}

func deadline(timeout time.Duration) time.Time {
	if timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(timeout)
}

type storeBackend struct {
	store *shardlet.Store
}

func (b storeBackend) Put(key, value string, opts shardlet.PutOptions) (shardlet.Value, error) {
	return b.store.Put(key, value, opts)
}

func (b storeBackend) Get(key string) (shardlet.Value, error) {
	return b.store.Get(key)
}

func (b storeBackend) Delete(key string) (bool, error) {
	return b.store.Delete(key)
}

func (b storeBackend) Range(low, high string) ([]shardlet.Value, error) {
	return b.store.Range(low, high), nil
}

func (b storeBackend) Stats() (shardlet.ClusterStats, error) {
	return b.store.Stats(), nil
}

func (b storeBackend) Rebalance(groups []string) error {
	return b.store.Rebalance(groups)
}

type raftGroupBackend struct {
	group *raftgroup.Group
}

func (b raftGroupBackend) Put(key, value string, opts shardlet.PutOptions) (shardlet.Value, error) {
	return b.group.Put(key, value, opts)
}

func (b raftGroupBackend) Get(key string) (shardlet.Value, error) {
	return b.group.Get(key)
}

func (b raftGroupBackend) Delete(key string) (bool, error) {
	return b.group.Delete(key)
}

func (b raftGroupBackend) Range(low, high string) ([]shardlet.Value, error) {
	return b.group.Range(low, high)
}

func (b raftGroupBackend) Stats() (shardlet.ClusterStats, error) {
	return b.group.StoreStats()
}

func (b raftGroupBackend) Rebalance(groups []string) error {
	return b.group.Rebalance(groups)
}

func (b raftGroupBackend) RaftStats() raftgroup.GroupStats {
	return b.group.Stats()
}
