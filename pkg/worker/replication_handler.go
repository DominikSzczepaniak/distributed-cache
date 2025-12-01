package worker

import (
	"context"
	"log/slog"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ReplicationHandler implements the gRPC Raft service for receiving replication requests
// This is called by primary workers to replicate data to backup workers
type ReplicationHandler struct {
	raftpb.UnimplementedRaftServer
	store *WorkerStore
}

// NewReplicationHandler creates a new replication handler
func NewReplicationHandler(store *WorkerStore) *ReplicationHandler {
	return &ReplicationHandler{
		store: store,
	}
}

// Replicate handles replication requests from primary workers
// Implements raftpb.RaftServer.Replicate
func (rh *ReplicationHandler) Replicate(ctx context.Context, req *raftpb.ReplicateRequest) (*raftpb.ReplicateResponse, error) {
	slog.Info("Received replication request",
		"worker_id", rh.store.GetWorkerID(),
		"key", req.Key,
		"operation", req.Operation)

	// Validate request
	if req.Operation != "PUT" && req.Operation != "DELETE" {
		return &raftpb.ReplicateResponse{
			Success: false,
			Error:   "invalid operation: must be PUT or DELETE",
		}, nil
	}

	// Apply replication to local storage
	err := rh.store.Replicate(int(req.Key), int(req.Value), req.Operation)
	if err != nil {
		slog.Error("Replication failed",
			"worker_id", rh.store.GetWorkerID(),
			"key", req.Key,
			"error", err)
		return &raftpb.ReplicateResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &raftpb.ReplicateResponse{
		Success: true,
	}, nil
}

// The following methods are part of the RaftServer interface but not used by workers
// They return "not implemented" errors as workers don't participate in Raft consensus

func (rh *ReplicationHandler) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "workers do not participate in Raft consensus")
}

func (rh *ReplicationHandler) ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "workers do not participate in Raft consensus")
}

func (rh *ReplicationHandler) VoteRequest(ctx context.Context, args *raftpb.VoteRequestArgs) (*raftpb.VoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "workers do not participate in Raft consensus")
}

func (rh *ReplicationHandler) LogRequest(ctx context.Context, args *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "workers do not participate in Raft consensus")
}

func (rh *ReplicationHandler) Heartbeat(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	// Workers can respond to health checks
	return &emptypb.Empty{}, nil
}

func (rh *ReplicationHandler) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "workers do not participate in Raft consensus")
}
