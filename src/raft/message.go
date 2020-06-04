package raft

// RequestVoteRequest contains request and response for RequestVote RPC call
type RequestVoteRequest struct {
	args    *RequestVoteArgs
	replyCh chan<- RequestVoteReply
}

// RequestVoteArgs represents argument from peer's client
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int64 // candidate's term
	CandidateID int64 // candidate requesting vote
	LastLogIdx  int64 // index of candidate's last log entry (section 5.4)
	LastLogTerm int64 // term of candidate's last log entry (section 5.4)
}

// RequestVoteReply represents a reply returned to peer
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int64 // currentTerm, for candidate update itself
	VotedGranted bool  // true means candidate receive vote
}

// AppendEntriesRequest contains request and response for AppendEntries RPC call
type AppendEntriesRequest struct {
	args    *AppendEntriesArgs
	replyCh chan<- AppendEntriesReply
}

// AppendEntriesArgs
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int64      // leader term
	LeaderID     int64      // so follower can know and redirect to client
	PrevLogIdx   int64      // idx of log entry immediately preceding new one
	PrevLogTerm  int64      // term of prevLogIdx
	Entries      []LogEntry // empty for heartbeat
	LeaderCommit int64      // leader's commitIdx
}

// AppendEntriesReply
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int64 // current term, for leader update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// CommandRequest contains request and response for Command RPC call
type CommandRequest struct {
	command  interface{}
	logIndex int64
	replyCh  chan<- CommandReply
}

// CommandReply reply from Command RPC call
type CommandReply struct {
	Success     bool
	Index       int64
	CurrentTerm int64
}
