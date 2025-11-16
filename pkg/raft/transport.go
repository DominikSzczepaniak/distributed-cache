package raft

import (
	"context"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/grpc"
)

type PeerClient interface {
	Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error)
	ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error)
	LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error)
	VoteRequest(ctx context.Context, in *raftpb.VoteRequestArgs) (*raftpb.VoteResponse, error)
	Heartbeat(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error)
	InstallSnapshot(ctx context.Context, in *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

type GRPCPeerClient struct {
	cli raftpb.RaftClient
}

func NewGRPCPeerClient(conn *grpc.ClientConn) *GRPCPeerClient {
	return &GRPCPeerClient{cli: raftpb.NewRaftClient(conn)}
}

func (g *GRPCPeerClient) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error) {
	return g.cli.Forward(ctx, msg)
}

func (g *GRPCPeerClient) ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
	return g.cli.ForwardGet(ctx, req)
}

func (g *GRPCPeerClient) LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
	return g.cli.LogRequest(ctx, in)
}
func (g *GRPCPeerClient) VoteRequest(ctx context.Context, in *raftpb.VoteRequestArgs) (*raftpb.VoteResponse, error) {
	return g.cli.VoteRequest(ctx, in)
}
func (g *GRPCPeerClient) Heartbeat(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	return g.cli.Heartbeat(ctx, in)
}
func (g *GRPCPeerClient) InstallSnapshot(ctx context.Context, in *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return g.cli.InstallSnapshot(ctx, in)
}
