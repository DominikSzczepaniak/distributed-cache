package raft

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
