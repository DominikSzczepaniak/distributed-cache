// Definitions of RequestVote, AppendEntries, InstallSnapshot RPC structs.
// Handlers for these RPCs (handleRequestVote, handleAppendEntries, handleInstallSnapshot).
// Helper functions for sending RPCs.
package raft

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/grpc"
)

func (r *Raft) initGRPC(cfg *Config) {
	r.peers = make([]PeerClient, r.totalNodes)
	r.conns = make([]*grpc.ClientConn, r.totalNodes)

	for i, addr := range cfg.raftAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithTimeout(500*time.Millisecond))
		if err != nil {
			panic(fmt.Sprintf("failed to dial %s: %v", addr, err))
		}

		r.conns[i] = conn
		r.peers[i] = NewGRPCPeerClient(conn)
	}

	go r.serveGRPC(cfg.raftAddrs[r.id])
}

func (r *Raft) serveGRPC(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	srv := grpc.NewServer()
	raftpb.RegisterRaftServer(srv, r)
	if err := srv.Serve(lis); err != nil {
		panic(err)
	}
}

func (r *Raft) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.Null, error) {
	var val *int
	if msg.Value != nil {
		tmp := int(msg.Value.Value)
		val = &tmp
	}
	internal := Message{
		msgType: MessageType(msg.Type.String()),
		key:     int(msg.Key),
		value:   val,
	}

	if r.currentRole != Leader {
		if r.currentLeaderId < 0 {
			return nil, fmt.Errorf("no leader known")
		}
		return r.peers[r.currentLeaderId].Forward(ctx, msg)
	}
	r.Broadcast(internal)
	return &raftpb.Null{}, nil
}

func convertLogRequestArgs(args *raftpb.LogRequestArgs) (int, int, int, int, int, []LogEntry) {
	leaderId := int(args.LeaderId)
	term := int(args.Term)
	prefixLen := int(args.PrefixLen)
	prefixTerm := int(args.PrefixTerm)
	commitLength := int(args.CommitLength)
	suffix := make([]LogEntry, len(args.Suffix))
	for i, e := range args.Suffix {
		var val *int
		if e.Message.Value != nil {
			tmp := int(e.Message.Value.Value)
			val = &tmp
		}
		suffix[i] = LogEntry{
			term: int(e.Term),
			message: Message{
				msgType: MessageType(e.Message.Type.String()),
				key:     int(e.Message.Key),
				value:   val,
			},
		}
	}
	return leaderId, term, prefixLen, prefixTerm, commitLength, suffix

}

func (r *Raft) LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) { //receiving LogRequest - this machine is trying to append log entries to it's log entries
	r.raftElector.ResetTimer()
	leaderId, term, prefixLen, prefixTerm, commitLength, suffix := convertLogRequestArgs(in)
	slog.Info(fmt.Sprintf("Received LogRequest on node %d from node %d, node on role %s", r.id, int(in.LeaderId), r.currentRole))

	if r.currentTerm > term {
		r.currentTerm = term
		r.votedFor = -1
		r.raftElector.ResetTimer()
	}
	if r.currentTerm == term {
		r.currentRole = "follower"
		r.currentLeaderId = leaderId
		slog.Info(fmt.Sprintf("Updated role of %d to %s and set leader id to %d", r.id, r.currentRole, r.currentLeaderId))
	}
	logOk := (len(r.log) >= prefixLen) && (prefixLen == 0 || r.log[prefixLen-1].term == prefixTerm)
	if r.currentTerm == term && logOk {
		r.AppendEntries(prefixLen, commitLength, suffix)
		ack := prefixLen + len(suffix)
		r.logSaver.SaveValues(int32(r.currentTerm), int32(r.votedFor), int32(r.commitedLength), r.log)
		return &raftpb.LogResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(r.currentTerm),
			Ack:         int32(ack),
			Success:     true,
		}, nil
	} else {
		r.logSaver.SaveValues(int32(r.currentTerm), int32(r.votedFor), int32(r.commitedLength), r.log)
		return &raftpb.LogResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(r.currentTerm),
			Ack:         0,
			Success:     false,
		}, nil
	}
}

func (r *Raft) VoteRequest(ctx context.Context, in *raftpb.VoteRequestArgs) (*raftpb.VoteResponse, error) {
	slog.Info(
		"Received request for vote\n",
		slog.Int("nodeOn", r.id),
		slog.Int("nodeFrom", int(in.CandidateId)),
		slog.Int("nodeOnTerm", r.currentTerm),
		slog.Int("nodeFromTerm", int(in.CandidateTerm)),
		slog.String("nodeOnRole", string(r.currentRole)),
	)
	candidateId := int(in.CandidateId)
	candidateTerm := int(in.CandidateTerm)
	candidateLogLength := int(in.CandidateLogLength)
	candidateLogTerm := int(in.CandidateLogTerm)
	if candidateTerm > r.currentTerm {
		slog.Info(
			"Updating current term\n",
			slog.Int("nodeOn", r.id),
			slog.Int("nodeFrom", int(in.CandidateId)),
			slog.Int("nodeOnTerm", r.currentTerm),
			slog.Int("nodeFromTerm", int(in.CandidateTerm)),
		)
		r.currentTerm = candidateTerm
		r.currentRole = "follower"
		r.votedFor = -1
	}
	lastTerm := 0
	if len(r.log) > 0 {
		lastTerm = r.log[len(r.log)-1].term
	}
	logOk := (candidateLogTerm > lastTerm) || (candidateLogTerm == lastTerm && candidateLogLength >= len(r.log))
	slog.Info(
		"LogOk\n",
		slog.Int("nodeOn", r.id),
		slog.Int("nodeFrom", int(in.CandidateId)),
		slog.Bool("LogOK Value", logOk),
		slog.Bool("Granting vote", candidateTerm == r.currentTerm && logOk && (r.votedFor == -1 || r.votedFor == candidateId)),
	)
	if candidateTerm == r.currentTerm && logOk && (r.votedFor == -1 || r.votedFor == candidateId) {
		r.votedFor = candidateId
		r.currentRole = Follower
		r.logSaver.SaveValues(int32(r.currentTerm), int32(r.votedFor), int32(r.commitedLength), r.log)
		slog.Info(
			"VoteRequest successful\n",
			slog.Int("nodeOn", r.id),
			slog.Int("nodeFrom", int(in.CandidateId)))
		return &raftpb.VoteResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(r.currentTerm),
			Granted:     true,
		}, nil
	} else {
		slog.Info(
			"VoteRequest unsuccessful\n",
			slog.Int("nodeOn", r.id),
			slog.Int("nodeFrom", int(in.CandidateId)))
		return &raftpb.VoteResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(r.currentTerm),
			Granted:     false,
		}, nil
	}
}
