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

	"shardlet/pkg/shardlet"
)

type Server struct {
	store    *shardlet.Store
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

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	srv := &Server{
		store:    store,
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
		value, err := s.store.Put(req.Key, req.Value, shardlet.PutOptions{TTL: req.TTL})
		if err != nil {
			return fail(req, err)
		}
		resp.Value = &value
	case OpGet:
		value, err := s.store.Get(req.Key)
		if err != nil {
			return fail(req, err)
		}
		resp.Value = &value
	case OpDelete:
		deleted, err := s.store.Delete(req.Key)
		if err != nil {
			return fail(req, err)
		}
		resp.Deleted = deleted
	case OpRange:
		resp.Values = s.store.Range(req.Low, req.High)
	case OpStats:
		stats := s.store.Stats()
		resp.Stats = &stats
	case OpRebalance:
		if err := s.store.Rebalance(req.Groups); err != nil {
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
