package sharding

import "fmt"

// WrongNodeError indicates the request was sent to the wrong node
// Similar to Redis MOVED error, contains redirect information
type WrongNodeError struct {
	Key         string
	PartitionID PartitionID
	CurrentNode NodeID
	CorrectNode NodeID
	CorrectAddr string
}

func (e *WrongNodeError) Error() string {
	return fmt.Sprintf("MOVED: key '%s' (partition %d) is owned by node %d at %s, not %d",
		e.Key, e.PartitionID, e.CorrectNode, e.CorrectAddr, e.CurrentNode)
}

// HTTPStatusCode returns the HTTP status code for this error (307 Temporary Redirect)
func (e *WrongNodeError) HTTPStatusCode() int {
	return 307 // Temporary Redirect - allows future rebalancing without client caching
}

// RedirectLocation returns the redirect URL for HTTP Location header
func (e *WrongNodeError) RedirectLocation() string {
	return e.CorrectAddr
}

// NewWrongNodeError creates a new WrongNodeError with all required fields
func NewWrongNodeError(key string, partitionID PartitionID, currentNode, correctNode NodeID, correctAddr string) *WrongNodeError {
	return &WrongNodeError{
		Key:         key,
		PartitionID: partitionID,
		CurrentNode: currentNode,
		CorrectNode: correctNode,
		CorrectAddr: correctAddr,
	}
}
