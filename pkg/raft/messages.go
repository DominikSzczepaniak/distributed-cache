package raft

import "github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"

type MessageType string

const (
	GetMsg    MessageType = "GET"
	PutMsg    MessageType = "PUT"
	DeleteMsg MessageType = "DELETE"

	CommandMsg MessageType = "COMMAND"
)

type Message struct {
	MsgType MessageType
	Key     int
	Value   *int

	Data []byte

	ResponseChan     chan<- BroadcastResponse
	IdempotencyToken string
	ClientID         string
}

func toProtoMsgType(m MessageType) raftpb.Message_Type {
	switch m {
	case GetMsg:
		return raftpb.Message_GET
	case PutMsg:
		return raftpb.Message_PUT
	case DeleteMsg:
		return raftpb.Message_DELETE
	default:
		panic("unknown MessageType: " + string(m))
	}
}
