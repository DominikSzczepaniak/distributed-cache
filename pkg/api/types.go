package api

// PutRequest represents a client's request to store or update a value.
type PutRequest struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

// PutResponse is the response sent after a PUT operation.
type PutResponse struct {
	// Success indicates if the operation was committed.
	Success bool `json:"success"`
	// Message contains a status or error message.
	Message string `json:"message,omitempty"`
}

// GetResponse contains the value retrieved for a specific key.
type GetResponse struct {
	// Key is the requested key ID.
	Key int `json:"key"`
	// Value is the retrieved integer value.
	Value int `json:"value"`
	// Found is true if the key exists in the cache.
	Found bool `json:"found"`
}

// DeleteRequest represents a client's request to remove a key.
type DeleteRequest struct {
	// Key is the ID of the key to delete.
	Key int `json:"key"`
}

// DeleteResponse is the response sent after a DELETE operation.
type DeleteResponse struct {
	// Success indicates if the deletion was successful.
	Success bool `json:"success"`
	// Message contains a status or error message.
	Message string `json:"message,omitempty"`
}

// StatusResponse provides a summary of the cluster's current status.
type StatusResponse struct {
	NodeID     int    `json:"node_id"`
	Role       string `json:"role"`
	Term       int    `json:"term"`
	LeaderID   int    `json:"leader_id"`
	TotalNodes int    `json:"total_nodes"`
}

// LeaderResponse provides information about the current cluster leader.
type LeaderResponse struct {
	IsLeader bool `json:"is_leader"`
	LeaderID int  `json:"leader_id"`
}

// HealthResponse provides simple health check information.
type HealthResponse struct {
	Status string `json:"status"`
}
