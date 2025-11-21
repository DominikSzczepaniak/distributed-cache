package raft

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/dominikszczepaniak/distributed-cache/pkg/raft/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

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

type forwardHopKey struct{}

func (r *Raft) Forward(ctx context.Context, msg *raftpb.Message) (*raftpb.ForwardResponse, error) {
	slog.Info(fmt.Sprintf("Received Forward on node %d", r.id))
	var val *int
	if msg.Value != nil {
		tmp := int(msg.Value.Value)
		val = &tmp
	}
	internal := Message{
		MsgType:          MessageType(msg.Type.String()),
		Key:              int(msg.Key),
		Value:            val,
		IdempotencyToken: msg.IdempotencyToken,
		ClientID:         msg.ClientId,
	}

	isLeader, leaderID := r.getLeaderData()

	if !isLeader {
		if leaderID < 0 {
			return nil, fmt.Errorf("no leader known")
		}
		if leaderID == r.id {
			r.mu.Lock()
			if r.currentRole != Leader && r.currentLeaderId == r.id {
				r.currentLeaderId = -1
			}
			r.mu.Unlock()
			return nil, fmt.Errorf("no leader known")
		}
		if ctx.Value(forwardHopKey{}) != nil {
			return nil, fmt.Errorf("redirect loop detected at node=%d to leader=%d", r.id, leaderID)
		}
		ctx = context.WithValue(ctx, forwardHopKey{}, true)
		slog.Info(fmt.Sprintf("Node %d forwards Forward call to %d", r.id, leaderID))
		peer := r.getPeer(leaderID)
		if peer == nil {
			return nil, fmt.Errorf("no peer for node %d", r.id)
		}
		return peer.Forward(ctx, msg)
	}

	success, value, err := r.BroadcastSync(internal, 5*time.Second)
	if err != nil {
		return nil, err
	}

	return &raftpb.ForwardResponse{
		Success: success,
		Value:   int32(value),
	}, nil
}

func (r *Raft) ForwardGet(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
	slog.Info(fmt.Sprintf("Received ForwardGet on node %d for key %d", r.id, req.Key))

	isLeader, leaderID := r.getLeaderData()

	if !isLeader {
		if leaderID < 0 {
			return nil, fmt.Errorf("no leader known")
		}
		if ctx.Value(forwardHopKey{}) != nil {
			return nil, fmt.Errorf("redirect loop detected at node=%d to leader=%d", r.id, leaderID)
		}
		ctx = context.WithValue(ctx, forwardHopKey{}, true)
		slog.Info(fmt.Sprintf("Node %d forwards ForwardGet to leader %d", r.id, leaderID))
		peer := r.getPeer(leaderID)
		if peer == nil {
			return nil, fmt.Errorf("no peer for leader %d", leaderID)
		}
		return peer.ForwardGet(ctx, req)
	}

	value := r.application.GetValue(int(req.Key))
	return &raftpb.GetResponse{
		Key:   req.Key,
		Value: int32(value),
		Found: true, // TODO: Add existence check to Application interface if needed (https://github.com/DominikSzczepaniak/distributed-cache/issues/16)
	}, nil
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
	leaderId, term, prevIndex, prevTerm, commitLength, suffix := convertLogRequestArgs(in)

	r.mu.Lock()

	oldRole := r.currentRole
	oldLeader := r.currentLeaderId

	slog.Info(fmt.Sprintf("Node %d received LogRequest from leader=%d term=%d (myTerm=%d myRole=%s myLeader=%d) suffix=%d entries",
		r.id, leaderId, term, r.currentTerm, r.currentRole, r.currentLeaderId, len(suffix)))

	if r.currentTerm < term {
		slog.Info(fmt.Sprintf("Node %d: Updating term from %d to %d (LogRequest from leader %d)",
			r.id, r.currentTerm, term, leaderId))
		r.currentTerm = term
		r.votedFor = -1
		r.raftElector.ResetTimer()
	}

	if r.currentTerm == term {
		if oldRole != Follower {
			slog.Info(fmt.Sprintf("Node %d: Stepping down from %s to Follower (term=%d leader=%d)",
				r.id, oldRole, term, leaderId))
		}
		r.currentRole = Follower
		if r.id != leaderId {
			if oldLeader != leaderId {
				slog.Info(fmt.Sprintf("Node %d: Updating leader from %d to %d",
					r.id, oldLeader, leaderId))
			}
			r.currentLeaderId = leaderId
		} else {
			r.currentLeaderId = -1
		}
	}
	absLastIndex := r.snapshotter.lastIndex + len(r.log)
	logOk := false
	switch {
	case prevIndex == 0:
		logOk = (prevTerm == 0)
	case prevIndex > absLastIndex:
		logOk = false
	case prevIndex == r.snapshotter.lastIndex:
		logOk = (r.snapshotter.lastTerm == prevTerm)
	default:
		rel := prevIndex - r.snapshotter.lastIndex - 1
		if rel >= 0 && rel < len(r.log) {
			logOk = (r.log[rel].Term == prevTerm)
		} else {
			logOk = false
		}
	}
	//logOk := (len(r.log) >= prefixLen) && (prefixLen == 0 || r.log[prefixLen-1].Term == prefixTerm) //old version

	if r.currentTerm == term && logOk {
		slog.Info(fmt.Sprintf("Node %d: LogRequest SUCCESS - appending %d entries (prevIndex=%d commitLen=%d)",
			r.id, len(suffix), prevIndex, commitLength))
		r.raftElector.ResetTimer()
		r.mu.Unlock()

		r.appendEntries(prevIndex, commitLength, suffix)

		ack := prevIndex + len(suffix)
		go r.logSaver.SaveValues()

		slog.Info(fmt.Sprintf("Node %d: LogRequest completed, ack=%d", r.id, ack))
		return &raftpb.LogResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(r.currentTerm),
			Ack:         int32(ack),
			Success:     true,
		}, nil
	} else {
		currentTerm := r.currentTerm
		slog.Warn(fmt.Sprintf("Node %d: LogRequest REJECTED - term=%d logOk=%t (prevIndex=%d absLastIndex=%d prevTerm=%d)",
			r.id, currentTerm, logOk, prevIndex, absLastIndex, prevTerm))
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
	slog.Info(fmt.Sprintf("Received VoteRequest from node %d on node %d, node thinks the leader is %d, node consider itself a leader: %t", in.CandidateId, r.id, r.currentLeaderId, r.currentRole == Leader)) // %+v", in))
	toPersist := false
	if r.currentLeaderId == int(in.CandidateId) {
		r.currentRole = Follower
		r.votedFor = -1
		r.currentLeaderId = -1
		toPersist = true
	}

	if candidateTerm > r.currentTerm {
		r.currentTerm = candidateTerm
		r.currentRole = Follower
		r.votedFor = -1

		r.raftElector.ResetTimer()
		toPersist = true
	}
	lastTerm := r.getLastLogTerm()
	logOk := (candidateLogTerm > lastTerm) || (candidateLogTerm == lastTerm && candidateLogLength >= len(r.log)+r.snapshotter.lastIndex)
	if candidateTerm == r.currentTerm && logOk && (r.votedFor == -1 || r.votedFor == candidateId) {
		r.votedFor = candidateId
		r.currentRole = Follower
		r.currentLeaderId = -1

		r.raftElector.ResetTimer()
		currentTerm := r.currentTerm
		r.mu.Unlock()
		go r.logSaver.SaveValues()
		return &raftpb.VoteResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(currentTerm),
			Granted:     true,
		}, nil
	} else {
		currentTerm := r.currentTerm
		r.mu.Unlock()
		if toPersist {
			go r.logSaver.SaveValues()
		}
		return &raftpb.VoteResponse{
			NodeId:      int32(r.id),
			CurrentTerm: int32(currentTerm),
			Granted:     false,
		}, nil
	}
}

func (r *Raft) Heartbeat(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	r.heartbeat.receiveHeartbeat()
	return &emptypb.Empty{}, nil
}

func (r *Raft) InstallSnapshot(ctx context.Context, in *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	r.raftElector.ResetTimer()
	r.mu.Lock()
	defer r.mu.Unlock()
	slog.Info(fmt.Sprintf("Installing snapshot for node %d, setting installingSnapshot to true", r.id))

	defaultResponse := &raftpb.InstallSnapshotResponse{Term: int32(r.currentTerm)}
	if int(in.LeaderTerm) < r.currentTerm {
		return defaultResponse, nil
	}
	if int(in.LeaderTerm) > r.currentTerm {
		r.currentTerm = int(in.LeaderTerm)
		r.currentRole = Follower
		r.currentLeaderId = int(in.LeaderId)
		r.raftElector.ResetTimer()
	}
	r.currentLeaderId = int(in.LeaderId)

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
	if err, key := r.application.RestoreFromSnapshot(snapshot); err != nil {
		return defaultResponse, err
	} else {
		slog.Info(fmt.Sprintf("When exited restore from snapshot, key value is %d", r.application.GetValue(key)))
	}
	r.snapshotter.lastIndex = lastIncludedIndex
	r.snapshotter.lastTerm = lastIncludedTerm

	if r.commitedLength < lastIncludedIndex {
		r.commitedLength = lastIncludedIndex
	}
	r.raftElector.ResetTimer()
	go r.logSaver.SaveValues()
	return defaultResponse, nil
}

// Replicate handles synchronous replication requests from primary to backup
// This is called by the primary node to replicate writes to the backup
func (r *Raft) Replicate(ctx context.Context, req *raftpb.ReplicateRequest) (*raftpb.ReplicateResponse, error) {
	key := int(req.Key)

	slog.Info(fmt.Sprintf("Node %d: Received REPLICATE %s for key=%d", r.id, req.Operation, key))

	// TODO: Add security validation - verify this node is the designated backup
	// for this partition (requires ShardManager integration in Stage 3)
	// For now, we trust the primary node is sending to the correct backup

	// Get application as the concrete type to access data directly
	// Replication writes bypass Raft consensus - we trust the primary
	switch req.Operation {
	case "PUT":
		value := int(req.Value)

		// Create a message to write directly to application
		msg := Message{
			MsgType: "PUT",
			Key:     key,
			Value:   &value,
		}

		success, _ := r.application.AppendMessage(msg)
		if !success {
			return &raftpb.ReplicateResponse{
				Success: false,
				Error:   "failed to write to local storage",
			}, nil
		}

		slog.Info(fmt.Sprintf("Node %d: REPLICATE PUT key=%d value=%d SUCCESS", r.id, key, value))
		return &raftpb.ReplicateResponse{Success: true}, nil

	case "DELETE":
		// Create a message to delete from application
		msg := Message{
			MsgType: "DELETE",
			Key:     key,
		}

		success, _ := r.application.AppendMessage(msg)
		if !success {
			return &raftpb.ReplicateResponse{
				Success: false,
				Error:   "failed to delete from local storage",
			}, nil
		}

		slog.Info(fmt.Sprintf("Node %d: REPLICATE DELETE key=%d SUCCESS", r.id, key))
		return &raftpb.ReplicateResponse{Success: true}, nil

	default:
		slog.Warn(fmt.Sprintf("Node %d: REPLICATE unknown operation: %s", r.id, req.Operation))
		return &raftpb.ReplicateResponse{
			Success: false,
			Error:   "unknown operation: " + req.Operation,
		}, nil
	}
}
