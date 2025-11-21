package sharding

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Snapshot file format constants
const (
	// Magic bytes to identify partition table snapshots
	MAGIC = "DCPT" // Distributed Cache Partition Table
)

// Serialize converts the partition table to binary format for Raft snapshots
// Format:
//   - Version (8 bytes, uint64)
//   - NumAssignments (4 bytes, uint32)
//   - For each assignment:
//     - PartitionID (2 bytes, uint16)
//     - PrimaryNode (4 bytes, int32)
//     - BackupNode (4 bytes, int32)
//
// This format is designed for efficient storage and fast deserialization
func (pt *PartitionTable) Serialize() ([]byte, error) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	var buf bytes.Buffer
	byteOrder := binary.LittleEndian

	// Write version
	if err := binary.Write(&buf, byteOrder, pt.version); err != nil {
		return nil, fmt.Errorf("failed to write version: %w", err)
	}

	// Write number of assignments
	numAssignments := uint32(len(pt.assignments))
	if err := binary.Write(&buf, byteOrder, numAssignments); err != nil {
		return nil, fmt.Errorf("failed to write assignment count: %w", err)
	}

	// Write each assignment
	for partitionID, entry := range pt.assignments {
		if err := binary.Write(&buf, byteOrder, uint16(partitionID)); err != nil {
			return nil, fmt.Errorf("failed to write partition ID %d: %w", partitionID, err)
		}
		if err := binary.Write(&buf, byteOrder, int32(entry.PrimaryNode)); err != nil {
			return nil, fmt.Errorf("failed to write primary node ID %d: %w", entry.PrimaryNode, err)
		}
		if err := binary.Write(&buf, byteOrder, int32(entry.BackupNode)); err != nil {
			return nil, fmt.Errorf("failed to write backup node ID %d: %w", entry.BackupNode, err)
		}
	}

	return buf.Bytes(), nil
}

// Deserialize restores the partition table from binary format
// This completely replaces the current state of the partition table
func (pt *PartitionTable) Deserialize(data []byte) error {
	if len(data) == 0 {
		// Empty data means empty partition table
		pt.mu.Lock()
		pt.assignments = make(map[PartitionID]*PartitionEntry)
		pt.version = 0
		pt.mu.Unlock()
		return nil
	}

	reader := bytes.NewReader(data)
	byteOrder := binary.LittleEndian

	// Read version
	var version uint64
	if err := binary.Read(reader, byteOrder, &version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}

	// Read number of assignments
	var numAssignments uint32
	if err := binary.Read(reader, byteOrder, &numAssignments); err != nil {
		return fmt.Errorf("failed to read assignment count: %w", err)
	}

	// Read each assignment
	assignments := make(map[PartitionID]*PartitionEntry, numAssignments)
	for i := uint32(0); i < numAssignments; i++ {
		var partitionID uint16
		if err := binary.Read(reader, byteOrder, &partitionID); err != nil {
			return fmt.Errorf("failed to read partition ID at index %d: %w", i, err)
		}

		var primaryNode int32
		if err := binary.Read(reader, byteOrder, &primaryNode); err != nil {
			return fmt.Errorf("failed to read primary node ID at index %d: %w", i, err)
		}

		var backupNode int32
		if err := binary.Read(reader, byteOrder, &backupNode); err != nil {
			return fmt.Errorf("failed to read backup node ID at index %d: %w", i, err)
		}

		assignments[PartitionID(partitionID)] = &PartitionEntry{
			PartitionID:  PartitionID(partitionID),
			PrimaryNode:  NodeID(primaryNode),
			BackupNode:   NodeID(backupNode),
			Version:      version,
		}
	}

	// Atomically update the partition table
	pt.mu.Lock()
	pt.assignments = assignments
	pt.version = version
	pt.mu.Unlock()

	return nil
}

// SerializeWithMagic adds magic bytes for validation
// This is used when the partition table is stored standalone
func (pt *PartitionTable) SerializeWithMagic() ([]byte, error) {
	data, err := pt.Serialize()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.WriteString(MAGIC)
	buf.Write(data)

	return buf.Bytes(), nil
}

// DeserializeWithMagic validates magic bytes before deserializing
func (pt *PartitionTable) DeserializeWithMagic(data []byte) error {
	if len(data) < len(MAGIC) {
		return fmt.Errorf("data too short to contain magic bytes")
	}

	magic := string(data[:len(MAGIC)])
	if magic != MAGIC {
		return fmt.Errorf("invalid magic bytes: expected %s, got %s", MAGIC, magic)
	}

	return pt.Deserialize(data[len(MAGIC):])
}

// CombineSnapshot combines data snapshot and partition table into a single byte array
// Format:
//   - Magic (4 bytes: "DCSH")
//   - PartitionTableSize (4 bytes, uint32)
//   - PartitionTable (variable)
//   - DataSize (4 bytes, uint32)
//   - Data (variable)
func CombineSnapshot(partitionTableData, dataSnapshot []byte) ([]byte, error) {
	var buf bytes.Buffer
	byteOrder := binary.LittleEndian

	// Write magic
	buf.WriteString("DCSH") // Distributed Cache SHarded

	// Write partition table size
	ptSize := uint32(len(partitionTableData))
	if err := binary.Write(&buf, byteOrder, ptSize); err != nil {
		return nil, fmt.Errorf("failed to write partition table size: %w", err)
	}

	// Write partition table
	buf.Write(partitionTableData)

	// Write data size
	dataSize := uint32(len(dataSnapshot))
	if err := binary.Write(&buf, byteOrder, dataSize); err != nil {
		return nil, fmt.Errorf("failed to write data size: %w", err)
	}

	// Write data
	buf.Write(dataSnapshot)

	return buf.Bytes(), nil
}

// SplitSnapshot separates a combined snapshot into partition table and data
func SplitSnapshot(combined []byte) (partitionTableData, dataSnapshot []byte, err error) {
	if len(combined) < 12 { // 4 (magic) + 4 (ptSize) + 4 (dataSize)
		return nil, nil, fmt.Errorf("snapshot too short: %d bytes", len(combined))
	}

	reader := bytes.NewReader(combined)
	byteOrder := binary.LittleEndian

	// Read and validate magic
	magic := make([]byte, 4)
	if _, err := reader.Read(magic); err != nil {
		return nil, nil, fmt.Errorf("failed to read magic: %w", err)
	}
	if string(magic) != "DCSH" {
		return nil, nil, fmt.Errorf("invalid snapshot magic: %s", string(magic))
	}

	// Read partition table size
	var ptSize uint32
	if err := binary.Read(reader, byteOrder, &ptSize); err != nil {
		return nil, nil, fmt.Errorf("failed to read partition table size: %w", err)
	}

	// Read partition table
	partitionTableData = make([]byte, ptSize)
	if _, err := reader.Read(partitionTableData); err != nil {
		return nil, nil, fmt.Errorf("failed to read partition table: %w", err)
	}

	// Read data size
	var dataSize uint32
	if err := binary.Read(reader, byteOrder, &dataSize); err != nil {
		return nil, nil, fmt.Errorf("failed to read data size: %w", err)
	}

	// Read data
	dataSnapshot = make([]byte, dataSize)
	if _, err := reader.Read(dataSnapshot); err != nil {
		return nil, nil, fmt.Errorf("failed to read data: %w", err)
	}

	return partitionTableData, dataSnapshot, nil
}
