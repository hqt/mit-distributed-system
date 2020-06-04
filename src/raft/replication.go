package raft

import "fmt"

// replicationJob represents a replication job for one follower
type replicationJob struct {
	*Raft
	termID     int64
	followerID int64
	signalCh   chan bool // notify when there are new logs append to system that need to replicate to all followers
	stopCh     chan bool // stop the background job
}

// newReplicationJob returns replicationJob object
func newReplicationJob(raft *Raft, termID int64, followerID int64) *replicationJob {
	return &replicationJob{
		Raft:       raft,
		termID:     termID,
		followerID: followerID,
		stopCh:     make(chan bool, 0),
		signalCh:   make(chan bool, 1), // avoid lost signal
	}
}

// stop stops the job
func (rp *replicationJob) stop() {
	rp.stopCh <- true
}

// run runs the job.
func (rp *replicationJob) run() {
	for {
		select {
		case <-rp.stopCh:
			return
		case <-rp.signalCh:
			lastLogIdx := rp.logManager.getLastLogIndex()
			nextIdx := rp.leaderState.getNextIdx(rp.followerID)

			args := &AppendEntriesArgs{
				Term:         rp.termID,
				LeaderID:     rp.nodeID(),
				PrevLogIdx:   nextIdx - 1,
				PrevLogTerm:  rp.logManager.getLastLogTerm(),
				LeaderCommit: rp.getCommitIndex(),
			}

			// rule-for-server: Leaders point 3
			if lastLogIdx >= nextIdx {
				args.Entries = rp.logManager.getLogsFromRange(int(nextIdx), int(lastLogIdx))
			}
			if len(args.Entries) == 0 {
				PrintDebug(rp.Raft,
					fmt.Sprintf("[replication] warning !!! trigger but no new log found"))
				continue
			}

			PrintDebug(rp.Raft,
				fmt.Sprintf("[replication] replicating to node %d from idx[%d-%d]. Total %d items",
					rp.followerID, nextIdx, lastLogIdx, len(args.Entries)))
			reply := &AppendEntriesReply{}
			ok := rp.sendAppendEntries(int(rp.followerID), args, reply)
			if !ok {
				continue
			}

			// rule-for-server
			if reply.Term > rp.termID {
				PrintDebug(rp.Raft,
					fmt.Sprintf("[replication] step down because old term. current-term: %d newer-term: %d node: %d",
						rp.termID, reply.Term, rp.followerID))
				asyncNotify(rp.stepDownCh)
			}

			if reply.Success {
				// rule-for-server leader 3.1: update nextIndex and matchIndex for follower
				PrintDebug(rp.Raft,
					fmt.Sprintf("[replication] replicated logs to node %d idx[%d-%d] success",
						rp.followerID, nextIdx, lastLogIdx))
				rp.Raft.leaderState.updateIdx(rp.followerID, lastLogIdx+1, lastLogIdx)

				idx, ok := rp.checkPossibleCommitIdx()
				if ok {
					PrintDebug(rp.Raft,
						fmt.Sprintf("[replication] job of node %d detects possible commit til index %d",
							rp.followerID, idx))
					asyncNotify(rp.commitCh)
				}
			} else {
				// rule-for-server leader 3.2:
				// fails because of log inconsistency. decrement nextIndex and retry
				rp.Raft.leaderState.decreaseNextIdx(rp.followerID)
				asyncNotify(rp.signalCh)
			}
		}
	}
}
