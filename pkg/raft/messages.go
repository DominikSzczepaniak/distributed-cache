package raft

import (
	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"github.com/dominikszczepaniak/distributed-cache/pkg/sharding"
)

type Message struct {
	MsgType MessageType
	Key     int
	Value   *int

	// Partition table update payload (only used for UPDATE_PARTITION_TABLE messages)
	PartitionTableUpdate *PartitionTableUpdate

	ResponseChan chan<- BroadcastResponse

	IdempotencyToken string
	ClientID         string
}

type MessageType string

const (
	get                  MessageType = "GET"
	put                  MessageType = "PUT"
	deleteMsg            MessageType = "DELETE"
	updatePartitionTable MessageType = "UPDATE_PARTITION_TABLE"
)

// PartitionTableUpdate contains the data for updating the partition table
type PartitionTableUpdate struct {
	Assignments map[sharding.PartitionID]sharding.NodeID
	Version     uint64
}

func toProtoMsgType(m MessageType) raftpb.Message_Type {
	switch m {
	case get:
		return raftpb.Message_GET
	case put:
		return raftpb.Message_PUT
	case deleteMsg:
		return raftpb.Message_DELETE
	case updatePartitionTable:
		return raftpb.Message_UPDATE_PARTITION_TABLE
	default:
		panic("unknown MessageType: " + string(m))
	}
}
