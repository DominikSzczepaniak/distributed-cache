package raft

func (r *Raft) setPeers(in []PeerClient) {
	buf := make([]PeerClient, len(in))
	copy(buf, in)
	r.peers.Store(buf)
}

func (r *Raft) getPeers() []PeerClient {
	return r.peers.Load().([]PeerClient)
}

func (r *Raft) getPeer(id int) PeerClient {
	return r.peers.Load().([]PeerClient)[id]
}

func (r *Raft) getCurrentRole() Role {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentRole
}

func (r *Raft) becomeFollower(term int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentRole = Follower
	r.currentTerm = term
	r.votedFor = -1

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

func (r *Raft) becomeLeader() {
	r.currentRole = Leader
	r.currentLeaderId = r.id

	r.replicators = make([]*Replicator, r.totalNodes)
	for followerId := 0; followerId < r.totalNodes; followerId++ {
		if followerId == r.id {
			continue
		}
		r.sentLengths[followerId] = len(r.log) + r.snapshotter.lastIndex
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
