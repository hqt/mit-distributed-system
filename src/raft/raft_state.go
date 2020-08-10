package raft

import (
	"sync"
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
	mu sync.RWMutex // Lock to protect shared resources that don't have atomic api
	state       PeerState

	// figure 2: persistent state
	currentTerm int64 // latest term server has seen
	votedFor    int64 // candidateID that received vote in the current term

	// figure 2: volatile state
	commitIdx   int64 // idx of highest log entry known to be committed --> check newer possible commit index
	lastApplied int64 // idx of highest log entry applied to state machine

	logManager *LogManager
}

// newRaftState creates a raftState object
func newRaftState() raftState {
	return raftState{
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		logManager:  newLogManager(),
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

func (s *raftState) persistVoteFor(serverID int64) {
	atomic.StoreInt64(&s.votedFor, serverID)
}

func (s *raftState) getVoteFor() int64 {
	return atomic.LoadInt64(&s.votedFor)
}

func (s *raftState) getCommitIndex() int64 {
	return atomic.LoadInt64(&s.commitIdx)
}
func (s *raftState) setCommitIndex(idx int64) {
	atomic.StoreInt64(&s.commitIdx, idx)
}
