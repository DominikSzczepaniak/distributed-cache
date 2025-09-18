package raft

type Snapshot struct {
	lastIndex int
	lastTerm  int
	state     map[int]int
}

//TODO
//on new snapshot do
//1. delete all previous logs until lastIndex
//2. delete all previous snapshots
