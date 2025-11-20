package api

type PutRequest struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

type PutResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type GetResponse struct {
	Key   int  `json:"key"`
	Value int  `json:"value"`
	Found bool `json:"found"`
}

type DeleteRequest struct {
	Key int `json:"key"`
}

type DeleteResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type StatusResponse struct {
	NodeID     int    `json:"node_id"`
	Role       string `json:"role"`
	Term       int    `json:"term"`
	LeaderID   int    `json:"leader_id"`
	TotalNodes int    `json:"total_nodes"`
}

type LeaderResponse struct {
	IsLeader bool `json:"is_leader"`
	LeaderID int  `json:"leader_id"`
}

type HealthResponse struct {
	Status string `json:"status"`
}

type RedirectResponse struct {
	Error       string `json:"error"`
	Message     string `json:"message"`
	NodeID      string `json:"node_id"`
	Address     string `json:"address"`
	PartitionID uint16 `json:"partition_id"`
}
