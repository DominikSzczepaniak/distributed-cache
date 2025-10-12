package raft

type Application interface {
	AppendMessage(message Message) (success bool, value int)
	GetSnapshot() ([]byte, error)
	RestoreFromSnapshot(data []byte) (error, int)
	GetValue(key int) int
}
