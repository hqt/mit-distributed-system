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
	logIdx := rf.logManager.getLastLogIndex()
	logTerm := rf.logManager.getLastLogTerm()

	if (candidate.Term > logTerm) || (logTerm == candidate.Term && candidate.LastLogIdx >= logIdx) {
		return true
	}
	return false
}

// rpcAppendEntries is called under follower either:
// - heartbeat from leader
// - command from client -> leader replication job -> follower
func (rf *Raft) rpcAppendEntries(req AppendEntriesRequest) {
	args := req.args
	reply := AppendEntriesReply{}
	defer func() {
		req.replyCh <- reply
	}()

	reply.Success = false
	reply.Term = rf.getCurrentTerm()

	// figure 2: condition 1
	if args.Term < rf.getCurrentTerm() {
		return
	}

	// update state after checking term to avoid receive requests from the very old leader
	rf.persistVoteFor(args.LeaderID)
	rf.lastHeard[args.LeaderID] = time.Now()

	// "rule-for-server": section 5.1
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

	// figure 2: condition 2
	// This condition maintains Log Matching property (5.3 - part 1):
	// if 2 entries in different logs have the same index and term:
	// - they store the same command
	// - the logs are identical in all preceding entries
	// algorithm:
	//	- leader sends last log index and term
	//  - follower refuses the new entry if found no entry that match index and log
	// len == 0 when heartbeat protocol
	if len(args.Entries) > 0 {
		log, ok := rf.logManager.getLogAtIndex(args.PrevLogIdx)
		if !ok {
			PrintDebug(rf, fmt.Sprintf("[rpcAppendEntries] violate condition 2. argIdx=%d, log_length=%d",
				args.PrevLogIdx, rf.logManager.length()))
			return
		}
		// log.Term == -1 --> null --> this index has not replicated to this node. no problem
		if log.Term != -1 && log.Term != args.PrevLogTerm {
			PrintDebug(rf, fmt.Sprintf("[rpcAppendEntries] violate condition 2. [leader](LogIdx=%d, term=%d) [peer](LogIdx=%d, term=%d)",
				args.PrevLogIdx, args.PrevLogTerm, log.Index, log.Term))
			return
		}
	}

	// figure 2: condition 3
	// This condition maintains log consistency between leader and follower (5.3 - part 2)
	// If an existing entry conflicts with a new one (same index but different terms)
	// delete the existing entry and all that follow it
	// TODO

	// figure 2: condition 4
	// Append any new entries not already in the log
	for _, entry := range args.Entries {
		rf.logManager.addLog(&entry)
		PrintDebug(rf, fmt.Sprintf("[rpcAppendEntries] replicated log with index=%d term=%d. log-length:%d",
			entry.Index, entry.Term, rf.logManager.length()))
	}

	// figure 2: condition 5
	// we update the last commit index
	lastCommitIdx := rf.getCommitIndex()
	if args.LeaderCommit > lastCommitIdx {
		min := minInt64(args.LeaderCommit, rf.logManager.getLastLogIndex())
		rf.setCommitIndex(min)

		// notify new commits to FSM. necessary for testing
		for i := lastCommitIdx + 1; i <= min; i++ {
			log, _ := rf.logManager.getLogAtIndex(i)
			PrintDebug(rf, fmt.Sprintf("[rpcAppendEntries] accepted commit from leader. leader_id=%d index=%d term=%d",
				args.LeaderID, i, args.Term))
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Data,
				CommandIndex: int(log.Index),
			}
		}
	}

	reply.Success = true
	return
}
