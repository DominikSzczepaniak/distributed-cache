package raft

import "github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"

type Message struct {
	MsgType MessageType
	Key     int
	Value   *int

	// Response channel for synchronous operations
	ResponseChan chan<- BroadcastResponse

	// Idempotency support for retry logic
	IdempotencyToken string
	ClientID         string
}

type MessageType string

const (
	get    MessageType = "GET"
	put    MessageType = "PUT"
	delete MessageType = "DELETE"
)

func toProtoMsgType(m MessageType) raftpb.Message_Type {
	switch m {
	case get:
		return raftpb.Message_GET
	case put:
		return raftpb.Message_PUT
	case delete:
		return raftpb.Message_DELETE
	default:
		panic("unknown MessageType: " + string(m))
	}
}
