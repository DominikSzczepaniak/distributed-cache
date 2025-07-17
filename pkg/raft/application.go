package raft

type Application interface {
	AppendMessage(message Message) (success bool, value int)
}
