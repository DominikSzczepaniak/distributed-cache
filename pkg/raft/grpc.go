package raft

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"net"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/grpc"
)

func (r *Raft) initGRPC(cfg *Config) {
	peers := make([]PeerClient, r.totalNodes)
	r.conns = make([]*grpc.ClientConn, r.totalNodes)

	for i, addr := range cfg.raftAddrs {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			panic(fmt.Sprintf("failed to dial %s: %v", addr, err))
		}

		r.conns[i] = conn
		peers[i] = NewGRPCPeerClient(conn)
	}
	r.setPeers(peers)
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
		MsgType: MessageType(msg.Type.String()),
		Key:     int(msg.Key),
		Value:   val,
	}

	isLeader, leaderID := r.getLeaderData()

	if !isLeader {
		if leaderID < 0 {
			return nil, fmt.Errorf("no leader known")
		}
		peer := r.getPeer(leaderID)
		return peer.Forward(ctx, msg)
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
			Term: int(e.Term),
			Message: Message{
				MsgType: MessageType(e.Message.Type.String()),
				Key:     int(e.Message.Key),
				Value:   val,
			},
		}
	}
	return leaderId, term, prefixLen, prefixTerm, commitLength, suffix

}

func (r *Raft) LogRequest(ctx context.Context, in *raftpb.LogRequestArgs) (*raftpb.LogResponse, error) {
	leaderId, term, prefixLen, prefixTerm, commitLength, suffix := convertLogRequestArgs(in)
	//we have to decrement prefixLen by amount of indexes we have in a snapshot:
	prefixLen -= r.snapshotter.lastIndex

	r.mu.Lock()
	if r.currentTerm < term {
		r.currentTerm = term
		r.votedFor = -1

		go r.raftElector.ResetTimer()
	}
	if r.currentTerm == term {
		r.currentRole = Follower
		r.currentLeaderId = leaderId
	}
	logLength := len(r.log) + r.snapshotter.lastIndex
	var logOk bool
	//TODO might be wrong
	if logLength < prefixLen {
		logOk = false
	} else if prefixLen == r.snapshotter.lastIndex {
		logOk = r.snapshotter.lastTerm == prefixTerm
	} else {
		sliceIndex := prefixLen - r.snapshotter.lastIndex - 1
		if sliceIndex >= 0 && sliceIndex < len(r.log) {
			logOk = r.log[sliceIndex].Term == prefixTerm
		} else {
			logOk = false
		}
	}

	//logOk := (len(r.log) >= prefixLen) && (prefixLen == 0 || r.log[prefixLen-1].Term == prefixTerm) //old version

	if r.currentTerm == term && logOk {
		r.raftElector.ResetTimer()
		r.mu.Unlock()

		r.appendEntries(prefixLen, commitLength, suffix)

		ack := prefixLen + len(suffix)
		go r.logSaver.SaveValues()

		return &raftpb.LogResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(r.currentTerm),
			Ack:         int32(ack),
			Success:     true,
		}, nil
	} else {
		currentTerm := r.currentTerm
		r.mu.Unlock()
		r.logSaver.SaveValues()
		return &raftpb.LogResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(currentTerm),
			Ack:         0,
			Success:     false,
		}, nil
	}
}

func (r *Raft) VoteRequest(ctx context.Context, in *raftpb.VoteRequestArgs) (*raftpb.VoteResponse, error) {
	candidateId := int(in.CandidateId)
	candidateTerm := int(in.CandidateTerm)
	candidateLogLength := int(in.CandidateLogLength)
	candidateLogTerm := int(in.CandidateLogTerm)

	r.mu.Lock()
	toPersist := false

	if candidateTerm > r.currentTerm {
		r.currentTerm = candidateTerm
		r.currentRole = Follower
		r.votedFor = -1

		go r.raftElector.ResetTimer()
		toPersist = true
	}
	lastTerm := r.getLastLogTerm()
	logOk := (candidateLogTerm > lastTerm) || (candidateLogTerm == lastTerm && candidateLogLength >= len(r.log)+r.snapshotter.lastIndex)
	if candidateTerm == r.currentTerm && logOk && (r.votedFor == -1 || r.votedFor == candidateId) {
		r.votedFor = candidateId
		r.currentRole = Follower

		go r.raftElector.ResetTimer()
		r.mu.Unlock()
		go r.logSaver.SaveValues()
		return &raftpb.VoteResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(r.currentTerm),
			Granted:     true,
		}, nil
	} else {
		r.mu.Unlock()
		if toPersist {
			go r.logSaver.SaveValues()
		}
		return &raftpb.VoteResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(r.currentTerm),
			Granted:     false,
		}, nil
	}
}

func (r *Raft) Heartbeat(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	r.heartbeat.receiveHeartbeat()
	return &emptypb.Empty{}, nil
}

func (r *Raft) InstallSnapshot(ctx context.Context, in *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	defaultResponse := &raftpb.InstallSnapshotResponse{Term: int32(r.currentTerm)}
	if int(in.LeaderTerm) < r.currentTerm {
		return defaultResponse, nil
	}
	if int(in.LeaderTerm) > r.currentTerm {
		r.currentTerm = int(in.LeaderTerm)
		r.currentRole = Follower
		r.currentLeaderId = int(in.LeaderId)
		go r.raftElector.ResetTimer()
	}
	r.currentLeaderId = int(in.LeaderId) //TODO check if this is correct

	_, err := r.logSaver.WriteSnapshotData(in.Data, int(in.Offset))
	if err != nil {
		return defaultResponse, err
	}

	if !in.Done {
		return defaultResponse, nil
	}

	lastIncludedIndex := int(in.LastIncludedIndex)
	lastIncludedTerm := int(in.LastIncludedTerm)
	currentLastIndex := r.snapshotter.lastIndex + len(r.log)
	if currentLastIndex > lastIncludedIndex {
		relativeIndex := lastIncludedIndex - r.snapshotter.lastIndex
		if relativeIndex >= 0 && relativeIndex < len(r.log) &&
			r.log[relativeIndex].Term == lastIncludedTerm {
			r.log = r.log[relativeIndex+1:]
		} else {
			r.log = []LogEntry{}
		}
	} else {
		r.log = []LogEntry{}
	}

	snapshot, err := os.ReadFile(r.logSaver.snapshotFilename)
	if err != nil {
		return defaultResponse, err
	}
	err = r.application.RestoreFromSnapshot(snapshot)
	if err != nil {
		return defaultResponse, err
	}
	r.snapshotter.lastIndex = lastIncludedIndex
	r.snapshotter.lastTerm = lastIncludedTerm

	if r.commitedLength < lastIncludedIndex {
		r.commitedLength = lastIncludedIndex
	}
	go r.raftElector.ResetTimer()
	go r.logSaver.SaveValues()
	return defaultResponse, nil
}
