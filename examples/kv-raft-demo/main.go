package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/dominikszczepaniak/distributed-cache/pkg/api"
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft"
)

type SimpleKVStore struct {
	mu   sync.RWMutex
	data map[int]int
}

func NewSimpleKVStore() *SimpleKVStore {
	return &SimpleKVStore{
		data: make(map[int]int),
	}
}

func (s *SimpleKVStore) AppendMessage(msg raft.Message) (bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch msg.MsgType {
	case "PUT":
		if msg.Value == nil {
			return false, 0
		}
		s.data[msg.Key] = *msg.Value
		slog.Info(fmt.Sprintf("PUT key=%d value=%d", msg.Key, *msg.Value))
		return true, 0
	case "GET":
		val := s.data[msg.Key]
		slog.Info(fmt.Sprintf("GET key=%d value=%d", msg.Key, val))
		return true, val
	case "DELETE":
		delete(s.data, msg.Key)
		slog.Info(fmt.Sprintf("DELETE key=%d", msg.Key))
		return true, 0
	default:
		slog.Warn(fmt.Sprintf("Unknown message type: %s", msg.MsgType))
		return false, 0
	}
}

func (s *SimpleKVStore) GetSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var buf bytes.Buffer
	byteOrder := binary.LittleEndian

	mapLen := int32(len(s.data))
	if err := binary.Write(&buf, byteOrder, mapLen); err != nil {
		return nil, err
	}

	for k, v := range s.data {
		if err := binary.Write(&buf, byteOrder, int64(k)); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, byteOrder, int64(v)); err != nil {
			return nil, err
		}
	}

	slog.Info(fmt.Sprintf("Created snapshot with %d entries", len(s.data)))
	return buf.Bytes(), nil
}

func (s *SimpleKVStore) RestoreFromSnapshot(data []byte) (error, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reader := bytes.NewReader(data)
	byteOrder := binary.LittleEndian

	var mapLen int32
	if err := binary.Read(reader, byteOrder, &mapLen); err != nil {
		return err, 0
	}

	newMap := make(map[int]int)
	var lastKey int

	for i := 0; i < int(mapLen); i++ {
		var k64, v64 int64
		if err := binary.Read(reader, byteOrder, &k64); err != nil {
			return err, 0
		}
		if err := binary.Read(reader, byteOrder, &v64); err != nil {
			return err, 0
		}
		lastKey = int(k64)
		newMap[int(k64)] = int(v64)
	}

	s.data = newMap
	slog.Info(fmt.Sprintf("Restored snapshot with %d entries", len(s.data)))
	return nil, lastKey
}

func (s *SimpleKVStore) GetValue(key int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[key]
}

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	slog.Info("Starting Raft node...")

	cfg := raft.LoadConfig()

	slog.Info("Node configuration loaded from environment")

	app := NewSimpleKVStore()

	r := raft.NewRaft(app, cfg)

	slog.Info("Raft node started successfully")

	apiAddr := os.Getenv("API_ADDR")
	if apiAddr == "" {
		apiAddr = ":8080"
	}

	apiServer := api.NewServer(r, apiAddr)
	go func() {
		if err := apiServer.Start(); err != nil && err != http.ErrServerClosed {
			slog.Error(fmt.Sprintf("API server error: %v", err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	slog.Info(fmt.Sprintf("Received signal %v, shutting down...", sig))

	if err := apiServer.Stop(); err != nil {
		slog.Error(fmt.Sprintf("Error stopping API server: %v", err))
	}

	slog.Info("Raft node stopped")
}
