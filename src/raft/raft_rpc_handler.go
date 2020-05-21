package raft

import (
	"fmt"
	"time"
)

// section 5.2: when a follower in the state of candidate, and they want to vote for itself as a leader
func (rf *Raft) rpcRequestVote(req RequestVoteRequest) {
	args := req.args
	reply := RequestVoteReply{}
	defer func() {
		req.replyCh <- reply
	}()

	reply.Term = rf.getCurrentTerm()
	reply.VotedGranted = false

	// condition 1 at figure 2(section 5.1) - RequestVote RPC
	if args.Term < rf.raftState.getCurrentTerm() {
		PrintDebug(rf,
			fmt.Sprintf("[rpcRequestVote] server=%d denied vote: term is old. [server=%d term=%d]",
				rf.me, args.CandidateID, args.Term))
		return
	}

	// update the state by following "rule for server" at figure 2 (section 5.1)
	// we also have granted vote YET, because it depends on the "election restriction" rule below
	if args.Term > rf.raftState.getCurrentTerm() {
		PrintDebug(rf,
			fmt.Sprintf("[rpcRequestVote] down to follower because larger term: old_term=%d -> (server=%d)new_term=%d",
				rf.getCurrentTerm(), args.CandidateID, args.Term))
		rf.raftState.updateState(Follower)
		rf.raftState.setTerm(args.Term)
		rf.persistVoteFor(-1)
		reply.Term = args.Term
	}

	// condition 2 at the figure 2
	if rf.getVoteFor() == -1 || rf.getVoteFor() == args.CandidateID {
		if rf.moreUpToDate(args) {
			PrintDebug(rf, fmt.Sprintf("[rpcRequestVote] accepted vote for server=%d with term=%d", args.CandidateID, args.Term))
			reply.VotedGranted = true
			rf.persistVoteFor(args.CandidateID)
			return
		} else {
			PrintDebug(rf, fmt.Sprintf("[rpcRequestVote] denied vote: violate election restriction rule"))
			return
		}
	} else {
		PrintDebug(rf, fmt.Sprintf("[rpcRequestVote] [server=%d] denied vote: candidate=%d != vote_for=%d",
			rf.me, args.CandidateID, rf.getVoteFor()))
		return
	}
}

// isUpToDate check if the candidate is more up-to-date than the current server
// section 5.4.1: Election restriction
// Implementation: check on the last log's term and index to choose which server is more up-to-date when storing log
// Reason: simplify the logic to transmit missing logs after the leader is voted
func (rf *Raft) moreUpToDate(candidate *RequestVoteArgs) bool {
	logIdx := rf.getLastLogIndex()
	logTerm := rf.getLastLogTerm()

	if (candidate.Term > logTerm) || (logTerm == candidate.Term && candidate.LastLogIdx >= logIdx) {
		return true
	}
	return false
}

func (rf *Raft) rpcAppendEntries(req AppendEntriesRequest) {
	args := req.args
	reply := AppendEntriesReply{}
	defer func() {
		req.replyCh <- reply
	}()

	reply.Success = false
	reply.Term = rf.getCurrentTerm()

	// condition 1
	if args.Term < rf.getCurrentTerm() {
		return
	}

	// "rule-for-server"
	if args.Term > rf.currentTerm {
		PrintDebug(rf,
			fmt.Sprintf("[rpcAppendEntries] server=%d down to follower because term outdate. current_term=%d (server=%d)peer_term=%d",
				rf.me, rf.getCurrentTerm(), args.LeaderID, args.Term))
		rf.updateState(Follower)
		rf.setTerm(args.Term)
		reply.Term = args.Term
	}

	// "rule-for-candidate"
	if rf.getState() == Candidate {
		PrintDebug(rf,
			fmt.Sprintf("[rpcAppendEntries] server=%d from candidate down to follower. AppendEntries info:server=%d term=%d",
				rf.me, args.LeaderID, args.Term))
		rf.updateState(Follower)
	}

	rf.persistVoteFor(args.LeaderID)
	// TODO check replication log condition
	reply.Success = true
	rf.lastHeard[args.LeaderID] = time.Now()
	return
}
