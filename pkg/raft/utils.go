package raft

import (
	"fmt"
	"log/slog"
	"math"
)

func (r *Raft) getPeers() []PeerClient {
	if r.connMgr != nil {
		return r.connMgr.GetPeers()
	}
	if r.testPeers != nil {
		return r.testPeers
	}
	return nil
}

func (r *Raft) getPeer(id int) PeerClient {
	if id == -1 {
		return nil
	}
	if r.connMgr != nil {
		return r.connMgr.GetPeer(id)
	}
	if r.testPeers != nil && id >= 0 && id < len(r.testPeers) {
		return r.testPeers[id]
	}
	return nil
}

func (r *Raft) isPeerAvailable(nodeId int) bool {
	if r.connMgr != nil {
		return r.connMgr.IsPeerAvailable(nodeId)
	}
	return true
}

func (r *Raft) getAvailablePeerCount() int {
	if r.connMgr != nil {
		return r.connMgr.GetAvailablePeerCount()
	}
	if r.testPeers != nil {
		return len(r.testPeers) - 1
	}
	return r.totalNodes - 1
}

func (r *Raft) setPeers(peers []PeerClient) {
	r.testPeers = peers
}

func (r *Raft) getCurrentRole() Role {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentRole
}

func (r *Raft) becomeFollowerUnlocked(term int) {
	r.currentRole = Follower
	r.currentTerm = term
	r.votedFor = -1
	r.currentLeaderId = -1

	if r.replicators != nil {
		for _, rep := range r.replicators {
			if rep != nil {
				rep.stop()
			}
		}
		r.replicators = nil
	}

	r.raftElector.ResetTimer()
}

func (r *Raft) becomeFollower(term int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.becomeFollowerUnlocked(term)
}

func (r *Raft) becomeLeader() {
	r.currentRole = Leader
	r.currentLeaderId = r.id

	majority := int(math.Ceil(float64(r.totalNodes+1) / 2))
	availableCount := r.getAvailablePeerCount()

	if availableCount < majority-1 {
		slog.Warn(fmt.Sprintf(
			"Node %d became leader with only %d/%d peers available (need %d for quorum)",
			r.id, availableCount, r.totalNodes-1, majority-1))
	} else {
		slog.Info(fmt.Sprintf(
			"Node %d became leader with %d/%d peers available",
			r.id, availableCount, r.totalNodes-1))
	}

	r.replicators = make([]*Replicator, r.totalNodes)
	for followerId := 0; followerId < r.totalNodes; followerId++ {
		if followerId == r.id {
			continue
		}
		lastLogIndex := r.snapshotter.lastIndex + len(r.log)
		r.sentLengths[followerId] = lastLogIndex + 1
		r.ackedLengths[followerId] = 0

		r.replicators[followerId] = NewReplicator(r, followerId)
		r.replicators[followerId].start()
	}
}

func (r *Raft) getLastLogTerm() int {
	if len(r.log) > 0 {
		return r.log[len(r.log)-1].Term
	} else if len(r.log) == 0 && r.snapshotter.lastIndex > 0 {
		return r.snapshotter.lastTerm
	}
	return 0
}

func (r *Raft) getLeaderData() (bool, int) {
	r.mu.RLock()
	isLeader := r.currentRole == Leader
	leaderID := r.currentLeaderId
	r.mu.RUnlock()
	return isLeader, leaderID
}

func (r *Raft) checkQuorumHealth() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentRole != Leader {
		return
	}

	failedFollowers := 0
	healthyFollowers := 0

	for i, rep := range r.replicators {
		if i == r.id || rep == nil {
			continue
		}

		if rep.consecutiveFailures >= rep.maxConsecutiveFailures {
			failedFollowers++
			slog.Debug(fmt.Sprintf("Node %d: Follower %d has %d consecutive failures",
				r.id, i, rep.consecutiveFailures))
		} else {
			healthyFollowers++
		}
	}

	majority := int(math.Ceil(float64(r.totalNodes+1) / 2))
	requiredHealthyFollowers := majority - 1

	slog.Info(fmt.Sprintf("Node %d quorum check: %d healthy, %d failed, need %d healthy followers (role=%s term=%d)",
		r.id, healthyFollowers, failedFollowers, requiredHealthyFollowers, r.currentRole, r.currentTerm))

	if healthyFollowers < requiredHealthyFollowers {
		slog.Warn(fmt.Sprintf(
			"Node %d STEPPING DOWN: lost quorum (only %d/%d healthy followers, need %d) - becoming Follower at term %d",
			r.id, healthyFollowers, r.totalNodes-1, requiredHealthyFollowers, r.currentTerm))

		r.becomeFollowerUnlocked(r.currentTerm)

		slog.Info(fmt.Sprintf("Node %d: Stepped down to Follower (term=%d, leader=%d)",
			r.id, r.currentTerm, r.currentLeaderId))
	}
}

// GetApplication returns the state machine instance associated with this Raft node.
func (r *Raft) GetApplication() Application {
	return r.application
}

// IsLeader returns true if the current node believes it is the cluster leader.
func (r *Raft) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentRole == Leader
}

// IsLeaderUnsafe returns whether this node is the leader WITHOUT acquiring locks.
// Use this ONLY in callbacks from the Raft state machine (like Application.AppendMessage)
// where the Raft mutex is already held by the calling code.
// WARNING: This is NOT safe to use in general code - use IsLeader() instead.
// IsLeaderUnsafe provides the leader status without acquiring the Raft mutex.
// Use with extreme caution.
func (r *Raft) IsLeaderUnsafe() bool {
	return r.currentRole == Leader
}

// ClusterStatus provides a high-level overview of the Raft node's current state.
type ClusterStatus struct {
	NodeID     int
	Role       string
	Term       int
	LeaderID   int
	TotalNodes int
}

// GetStatus returns the current ClusterStatus for monitoring and debugging.
func (r *Raft) GetStatus() ClusterStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return ClusterStatus{
		NodeID:     r.id,
		Role:       string(r.currentRole),
		Term:       r.currentTerm,
		LeaderID:   r.currentLeaderId,
		TotalNodes: r.totalNodes,
	}
}
