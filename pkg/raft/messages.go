package raft

import "github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"

type Message struct {
	msgType MessageType
	key     int
	value   *int
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
