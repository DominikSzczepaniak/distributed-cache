package raft

// Application is the interface that a state machine must implement to be managed by Raft.
// It provides methods for applying log entries, creating snapshots, and restoring state.
type Application interface {
	// AppendMessage is called when a log entry is committed.
	// The implementation should apply the command to the local state.
	AppendMessage(message Message) (success bool, value int)
	// GetSnapshot returns a byte slice representing the current state of the application.
	GetSnapshot() ([]byte, error)
	// RestoreFromSnapshot updates the application state from a previously saved snapshot.
	RestoreFromSnapshot(data []byte) (int, error)
	// GetValue is a helper to retrieve values from the state machine (if applicable).
	GetValue(key int) int
}
