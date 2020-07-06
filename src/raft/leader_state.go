package raft

import (
	"fmt"
	"sync"
)

// leaderState data for the peer in the leader state
type leaderState struct {
	mu       sync.RWMutex
	leaderID int64

	// figure 2
	nextIdx  []int64 // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIdx []int64 // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	commitCh          chan bool       // notify to main app when detect one/some messages have been replicated to quorum size of node and can be marked as committed
	stepDownCh        chan bool       // use to receive result from concurrent jobs when see newer term
	processingCommand *CommandRequest // command in processing. wait until it is committed

	replicationJobs map[int64]*replicationJob // store all replication jobs for each follower
}

// newLeaderState returns leader state. initialize all values per spec
func newLeaderState(rf *Raft) *leaderState {
	state := &leaderState{
		leaderID:          rf.me,
		commitCh:          make(chan bool),
		stepDownCh:        make(chan bool, 1),
		processingCommand: nil,
	}

	replicationJobs := map[int64]*replicationJob{}
	for idx := range rf.peers {
		if int64(idx) == rf.me {
			continue
		}
		replicationJobs[int64(idx)] = newReplicationJob(rf, rf.getCurrentTerm(), int64(idx))
	}
	state.replicationJobs = replicationJobs

	lastLogIndex := rf.logManager.getLastLogIndex()
	for _ = range rf.peers {
		state.nextIdx = append(state.nextIdx, lastLogIndex+1) // figure 2: state
		state.matchIdx = append(state.matchIdx, 0)            // figure 2: state
	}

	return state
}

// startReplicationJobs start replication jobs for each follower
func (s *leaderState) startReplicationJobs() {
	for serverID := range s.replicationJobs {
		go func(serverID int64) {
			s.replicationJobs[serverID].run()
		}(serverID)
	}
}

// stopReplicationJobs stop all replication jobs. Sync call
func (s *leaderState) stopReplicationJobs() {
	for serverID := range s.replicationJobs {
		fmt.Println("try to stop replication job from ", serverID)
		s.replicationJobs[serverID].stop()
		fmt.Println("replication stop done ", serverID)
	}
}

// getNextIdx returns next expected server's index for a log entry
func (s *leaderState) getNextIdx(serverID int64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextIdx[serverID]
}

// updateIdx called by replication job when replicate successfully
// rule-for-server: leader section point 3.1
func (s *leaderState) updateIdx(serverID int64, nextIdx int64, matchIdx int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextIdx[serverID] = nextIdx
	s.matchIdx[serverID] = matchIdx
}

// decreaseNextIdx called by replication job when replicate fail. Need to decrease and wait for the next replication
// rule-for-server: leader section point 3.2
func (s *leaderState) decreaseNextIdx(serverID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nextID := maxInt64(s.nextIdx[serverID]-1, 1)
	s.nextIdx[serverID] = nextID
}

// getLatestReplicateIdx returns latest replicate id on each follower node.
// is called when calculating largest possible commit index
func (s *leaderState) getLatestReplicateIdx() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var res []int64
	for i, e := range s.matchIdx {
		if int64(i) == s.leaderID {
			continue
		}
		res = append(res, e)
	}
	return res
}
