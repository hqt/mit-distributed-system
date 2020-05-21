package raft

import (
	"sync/atomic"
)

// PeerState represents the raftState of Raft
type PeerState int32

// all constants for PeerState
const (
	Follower PeerState = iota
	Candidate
	Leader
)

// String string interface
func (p PeerState) String() string {
	if p == Follower {
		return "Follower"
	}
	if p == Candidate {
		return "Candidate"
	}
	return "Leader"
}

// raftState internal raftState of raft
type raftState struct {
	state PeerState

	// persistent state
	currentTerm int64 // latest term server has seen
	votedFor    int64 // candidateID that received vote in the current term

	// volatile state
	commitIdx   int64 // idx of highest log entry known to be committed
	lastApplied int64 // idx of highest log entry applied to state machine
}

// newRaftState creates a raftState object
func newRaftState() raftState {
	return raftState{
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
	}
}

func (s *raftState) updateState(state PeerState) {
	atomic.StoreInt32((*int32)(&s.state), int32(state))
}

func (s *raftState) getState() PeerState {
	return PeerState(atomic.LoadInt32((*int32)(&s.state)))
}

func (s *raftState) getCurrentTerm() int64 {
	return atomic.LoadInt64(&s.currentTerm)
}

func (s *raftState) increaseTerm() int64 {
	return atomic.AddInt64(&s.currentTerm, 1)
}

func (s *raftState) setTerm(term int64) {
	atomic.StoreInt64(&s.currentTerm, term)
}

// TODO we don't need to persist voteForID in the lab2A
func (s *raftState) persistVoteFor(serverID int64) {
	atomic.StoreInt64(&s.votedFor, serverID)
}

func (s *raftState) getVoteFor() int64 {
	return atomic.LoadInt64(&s.votedFor)
}

// TODO lab 2A doesn't require log
func (s *raftState) getLastLogIndex() int64 {
	return -1
}

// TODO lab 2A doesn't require log
func (s *raftState) getLastLogTerm() int64 {
	return -1
}

