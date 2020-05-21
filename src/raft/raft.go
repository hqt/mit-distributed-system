package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hqt/mit-distributed-system/src/labrpc"
)

// all constants
const (
	// min and max timeout is optimized based on section 5.2 and 9.3
	minElectionTimeoutMs = 150
	maxElectionTimeoutMs = 300
	// heartBeat timout <= min election timeout
	// random timeout to avoid many followers become timeout and become candidates at the same time
	minHeartBeatTimeoutMs    = 100
	maxHeartBeatTimeoutMs    = 120
	sendHeartBeatDuration = time.Duration(80) * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int64               // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	raftState

	// custom
	lastHeard       map[int64]time.Time       // last heard from other peers
	stopCh          chan bool                 // use to stop the concurrency job try to run between peer's raftState
	finishCh        chan bool                 // use to wait result from the stopCh to make sure the concurrent job finish
	requestVoteCh   chan RequestVoteRequest   // all rpc RequestVote requests will move here to process
	appendEntriesCh chan AppendEntriesRequest // all rpc AppendEntries requests will move here to process
}

func (rf *Raft) getTotalPeers() int {
	return len(rf.peers)
}

func (rf *Raft) quorum() int {
	return len(rf.peers)/2 + 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.raftState.getCurrentTerm()), rf.raftState.getState() == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// RequestVote handler this method is triggered by the candidate by the RPC call `raft.RequestVote`
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	replyCh := make(chan RequestVoteReply)
	rf.requestVoteCh <- RequestVoteRequest{
		args:    args,
		replyCh: replyCh,
	}
	e := <-replyCh
	// reply = &e // wrong: because behind reply object is the byte array which is tracked by the labrpc package
	// manually copy fields
	reply.Term = e.Term
	reply.VotedGranted = e.VotedGranted
}

// AppendEntries handler this method is triggered by the leader in the RPC call `raft.AppendEntries` with:
// - heartbeat: (section 5.2)
// - replicate log entries: (section 5.3)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	replyCh := make(chan AppendEntriesReply)
	rf.appendEntriesCh <- AppendEntriesRequest{
		args:    args,
		replyCh: replyCh,
	}
	e := <-replyCh
	reply.Success = e.Success
	reply.Term = e.Term
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	rf.stopCh <- true
	// wait for other coroutine finish
	<-rf.finishCh

	close(rf.stopCh)
	//close(rf.requestVoteCh)
	//close(rf.appendEntriesCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent raftState, and also initially holds the most
// recent saved raftState, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        int64(me),

		// Your initialization code here (2A, 2B, 2C).
		raftState: newRaftState(),

		stopCh:          make(chan bool, 0),
		finishCh:        make(chan bool, 0),
		requestVoteCh:   make(chan RequestVoteRequest, 100),
		appendEntriesCh: make(chan AppendEntriesRequest, 100),
		lastHeard:       map[int64]time.Time{},
	}

	go rf.run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// run switch between Raft's finite raftState machine Follower / Candidate / Leader
func (rf *Raft) run() {
	for {
		select {
		case <-rf.stopCh:
			rf.finishCh <- true
			return
		default:
		}

		switch rf.raftState.getState() {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}
	}
}

func (rf *Raft) runFollower() {
	PrintDebug(rf, fmt.Sprintf("enter follower loop. Leader=%d", rf.getVoteFor()))

	heartBeatTimeoutMs := int64(randomInt(minHeartBeatTimeoutMs, maxHeartBeatTimeoutMs))
	heartBeatTimeoutCh := time.After(time.Duration(heartBeatTimeoutMs) * time.Millisecond)
	for rf.getState() == Follower {
		select {
		case e := <-rf.appendEntriesCh:
			rf.rpcAppendEntries(e)
		case e := <-rf.requestVoteCh:
			rf.rpcRequestVote(e)
		case <-heartBeatTimeoutCh:
			heartBeatTimeoutCh = time.After(time.Duration(heartBeatTimeoutMs) * time.Millisecond)
			if rf.votedFor == -1 {
				rf.updateState(Candidate)
				return
			}
			lastTime := rf.lastHeard[rf.votedFor]
			delta := time.Now().Sub(lastTime).Milliseconds()
			if delta > int64(heartBeatTimeoutMs) {
				PrintDebug(rf, fmt.Sprintf("timeout %d ms for waiting leader %d", delta, rf.votedFor))
				rf.updateState(Candidate)
				rf.votedFor = -1
				return
			}
		case <-rf.stopCh:
			rf.finishCh <- true
			return
		default:
		}
	}
}

func (rf *Raft) runCandidate() {
	PrintDebug(rf, fmt.Sprintf("enter candidate loop. Leader=%d", rf.getVoteFor()))

	// random the elect timeout: section 5.2. avoid the elect phrase runs indefinitely
	electionTimeoutCh := randomTimeout(minElectionTimeoutMs, maxElectionTimeoutMs)
	resultCh := rf.electSelf()

	totalVotes := 0
	for rf.getState() == Candidate {
		select {
		case e := <-rf.requestVoteCh:
			rf.rpcRequestVote(e)
		case e := <-rf.appendEntriesCh:
			rf.rpcAppendEntries(e)
		case e := <-resultCh:
			// handle the event
			if !e.networkStatus {
				PrintDebug(rf, fmt.Sprintf("network error when request vote to %d", e.receiverID))
			} else {
				if e.voteReply.Term > rf.getCurrentTerm() {
					// "rule-for-server"
					PrintDebug(rf,
						fmt.Sprintf("[voteResult] down to follower because older term: my_term=%d - peer_term=%d",
							rf.getCurrentTerm(), e.voteReply.Term))
					rf.setTerm(e.voteReply.Term)
					rf.updateState(Follower)
					return
				}
				if e.voteReply.VotedGranted {
					totalVotes++
					PrintDebug(rf,
						fmt.Sprintf("[voteResult] server=%d received vote from server=%d with term=%d. Total: %d",
							rf.me, e.receiverID, e.voteReply.Term, totalVotes))
				}
			}

			// check if elect phrase can be finished
			if totalVotes >= rf.quorum() {
				PrintDebug(rf, fmt.Sprintf("[voteResult] server=%d become leader at term %d", rf.me, rf.getCurrentTerm()))
				rf.updateState(Leader)
				return
			}
		case <-electionTimeoutCh:
			// restart again the election process
			PrintDebug(rf, fmt.Sprintf("[voteResult] server=%d election timeout", rf.me))
			return
		case <-rf.stopCh:
			rf.finishCh <- true
			return
		}
	}
}

type voteResult struct {
	networkStatus bool
	receiverID    int64
	voteReply     *RequestVoteReply
}

// electSelf sends all requests to all peers
func (rf *Raft) electSelf() <-chan voteResult {
	result := make(chan voteResult, 100)

	term := rf.increaseTerm()
	// send itself a vote
	rf.raftState.persistVoteFor(rf.me)
	result <- voteResult{
		networkStatus: true,
		receiverID:    rf.me,
		voteReply: &RequestVoteReply{
			Term:         term,
			VotedGranted: true,
		},
	}

	for idx := range rf.peers {
		if int64(idx) == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			Term:        term,
			CandidateID: rf.me,
			LastLogIdx:  rf.getLastLogIndex(),
			LastLogTerm: rf.getLastLogTerm(),
		}

		go func(serverID int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(serverID, args, reply)
			result <- voteResult{
				networkStatus: ok,
				receiverID:    int64(serverID),
				voteReply:     reply,
			}
		}(idx)
	}

	return result
}

func (rf *Raft) runLeader() {
	PrintDebug(rf, fmt.Sprintf("enter leader loop. Leader=%d", rf.getVoteFor()))

	replyCh := make(chan AppendEntriesReply, rf.getTotalPeers()*2)
	timeOutCh := time.After(sendHeartBeatDuration)
	rf.sendHeartBeat(replyCh)
	for rf.getState() == Leader {
		select {
		case e := <-rf.requestVoteCh:
			rf.rpcRequestVote(e)
		case e := <-rf.appendEntriesCh:
			rf.rpcAppendEntries(e)
		case e := <-replyCh:
			if e.Term > rf.getCurrentTerm() {
				rf.updateState(Follower)
				rf.persistVoteFor(-1)
				return
			}
		case <-timeOutCh:
			// resend heartbeat
			timeOutCh = time.After(sendHeartBeatDuration)
			rf.sendHeartBeat(replyCh)
		case <-rf.stopCh:
			rf.finishCh <- true
			return
		}
	}
}

func (rf *Raft) sendHeartBeat(replyCh chan<- AppendEntriesReply) {
	args := &AppendEntriesArgs{
		Term:         rf.getCurrentTerm(),
		LeaderID:     rf.me,
		PrevLogIdx:   -1, // TODO later assignment
		PrevLogTerm:  -1, // TODO later assignment
		Entries:      nil,
		LeaderCommit: 0,
	}
	for idx := range rf.peers {
		if idx == int(rf.me) {
			continue
		}
		go func(serverID int) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(serverID, args, reply)
			if ok {
				replyCh <- *reply
			}
		}(idx)
	}
}
