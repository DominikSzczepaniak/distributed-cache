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
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

type SimpleKVStore struct {
	mu             sync.RWMutex
	data           map[int]int
	partitionTable *sharding.PartitionTable
}

func NewSimpleKVStore() *SimpleKVStore {
	return &SimpleKVStore{
		data:           make(map[int]int),
		partitionTable: sharding.NewPartitionTable(),
	}
}

func (s *SimpleKVStore) AppendMessage(msg raft.Message) (bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch msg.MsgType {
	case "UPDATE_PARTITION_TABLE":
		if msg.PartitionTableUpdate == nil {
			slog.Warn("Received UPDATE_PARTITION_TABLE with nil payload")
			return false, 0
		}
		s.partitionTable.ApplyUpdate(
			msg.PartitionTableUpdate.Assignments,
			msg.PartitionTableUpdate.Version,
		)
		slog.Info(fmt.Sprintf("Updated partition table to version %d (%d assignments)",
			msg.PartitionTableUpdate.Version,
			len(msg.PartitionTableUpdate.Assignments)))
		return true, 0
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

	// Serialize data map (existing logic)
	var dataBuf bytes.Buffer
	byteOrder := binary.LittleEndian

	mapLen := int32(len(s.data))
	if err := binary.Write(&dataBuf, byteOrder, mapLen); err != nil {
		return nil, fmt.Errorf("failed to write map length: %w", err)
	}

	for k, v := range s.data {
		if err := binary.Write(&dataBuf, byteOrder, int64(k)); err != nil {
			return nil, fmt.Errorf("failed to write key %d: %w", k, err)
		}
		if err := binary.Write(&dataBuf, byteOrder, int64(v)); err != nil {
			return nil, fmt.Errorf("failed to write value for key %d: %w", k, err)
		}
	}

	// Serialize partition table
	partitionTableData, err := s.partitionTable.Serialize()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize partition table: %w", err)
	}

	// Combine both into a single snapshot
	combined, err := sharding.CombineSnapshot(partitionTableData, dataBuf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to combine snapshot: %w", err)
	}

	slog.Info(fmt.Sprintf("Created snapshot with %d data entries and %d partition assignments",
		len(s.data), s.partitionTable.GetAssignmentCount()))

	return combined, nil
}

func (s *SimpleKVStore) RestoreFromSnapshot(data []byte) (error, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Split combined snapshot into partition table and data
	partitionTableData, dataSnapshot, err := sharding.SplitSnapshot(data)
	if err != nil {
		return fmt.Errorf("failed to split snapshot: %w", err), 0
	}

	// Restore partition table
	if err := s.partitionTable.Deserialize(partitionTableData); err != nil {
		return fmt.Errorf("failed to deserialize partition table: %w", err), 0
	}

	// Restore data map (existing logic)
	reader := bytes.NewReader(dataSnapshot)
	byteOrder := binary.LittleEndian

	var mapLen int32
	if err := binary.Read(reader, byteOrder, &mapLen); err != nil {
		return fmt.Errorf("failed to read map length: %w", err), 0
	}

	newMap := make(map[int]int)
	var lastKey int

	for i := 0; i < int(mapLen); i++ {
		var k64, v64 int64
		if err := binary.Read(reader, byteOrder, &k64); err != nil {
			return fmt.Errorf("failed to read key at index %d: %w", i, err), 0
		}
		if err := binary.Read(reader, byteOrder, &v64); err != nil {
			return fmt.Errorf("failed to read value at index %d: %w", i, err), 0
		}
		lastKey = int(k64)
		newMap[int(k64)] = int(v64)
	}

	s.data = newMap
	slog.Info(fmt.Sprintf("Restored snapshot with %d data entries and %d partition assignments",
		len(s.data), s.partitionTable.GetAssignmentCount()))

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
