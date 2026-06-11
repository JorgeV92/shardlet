package raftgroup

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

var ErrNilLogStore = errors.New("nil log store")

type LogStore interface {
	Load(replicaID string) ([]LogEntry, error)
	Append(replicaID string, entry LogEntry) error
}

type MemoryLogStore struct {
	mu      sync.Mutex
	entries map[string][]LogEntry
}

func NewMemoryLogStore() *MemoryLogStore {
	return &MemoryLogStore{entries: make(map[string][]LogEntry)}
}

func (s *MemoryLogStore) Load(replicaID string) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.entries[replicaID]), nil
}

func (s *MemoryLogStore) Append(replicaID string, entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[replicaID] = append(s.entries[replicaID], entry)
	return nil
}

type FileLogStore struct {
	dir string
	mu  sync.Mutex
}

func NewFileLogStore(dir string) (*FileLogStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileLogStore{dir: dir}, nil
}

func (s *FileLogStore) Load(replicaID string) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.path(replicaID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []LogEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *FileLogStore) Append(replicaID string, entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.OpenFile(s.path(replicaID), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(entry)
}

func (s *FileLogStore) path(replicaID string) string {
	name := base64.RawURLEncoding.EncodeToString([]byte(replicaID)) + ".jsonl"
	return filepath.Join(s.dir, name)
}
