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
