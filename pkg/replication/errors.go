package replication

import "fmt"

// ReplicationError represents a replication failure
type ReplicationError struct {
	BackupNodeID int
	Operation    string
	Cause        error
}

func (e *ReplicationError) Error() string {
	return fmt.Sprintf("replication to backup node %d failed (%s): %v", e.BackupNodeID, e.Operation, e.Cause)
}

// CircuitBreakerOpenError represents a circuit breaker being open
type CircuitBreakerOpenError struct {
	NodeID int
}

func (e *CircuitBreakerOpenError) Error() string {
	return fmt.Sprintf("circuit breaker open for node %d", e.NodeID)
}
