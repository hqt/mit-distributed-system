package raft

import (
	"fmt"
	"time"
)

// replicationJob represents a replication job for one follower
type replicationJob struct {
	*Raft
	termID      int64
	followerID  int64
	signalCh    chan bool // notify when there are new logs append to system that need to replicate to all followers
	stopCh      chan bool // stop the background job
	waitCh      chan bool
}

// newReplicationJob returns replicationJob object
func newReplicationJob(raft *Raft, termID int64, followerID int64) *replicationJob {
	return &replicationJob{
		Raft:        raft,
		termID:      termID,
		followerID:  followerID,
		stopCh:      make(chan bool, 1),
		waitCh:      make(chan bool, 0),
		signalCh:    make(chan bool, 1), // avoid lost signal
	}
}

// stop stops the job
func (rp *replicationJob) stop() {
	rp.stopCh <- true
	<-rp.waitCh
}

// run runs the job.
func (rp *replicationJob) run() {
	defer func() {
		PrintDebug(rp.Raft, fmt.Sprintf("QUITTTTTTTT for server %d\n", rp.followerID))
	}()

	for {
		PrintDebug(rp.Raft, fmt.Sprintf("replication run for %d", rp.followerID))
		select {
		case <-rp.stopCh:
			rp.waitCh <- true
			PrintDebug(rp.Raft, fmt.Sprintf("[replication] stop for node %d", rp.followerID))
			return
		case <-rp.signalCh:
			nextIdx := rp.leaderState.getNextIdx(rp.followerID)
			log := rp.logManager.getLogAtIndex(nextIdx - 1)

			args := &AppendEntriesArgs{
				Term:         rp.termID,
				LeaderID:     rp.nodeID(),
				PrevLogIdx:   log.Index,
				PrevLogTerm:  log.Term,
				LeaderCommit: rp.getCommitIndex(),
			}

			// rule-for-server / Leaders : point 3
			lastLogIdx := rp.logManager.getLastLogIndex()
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

			reply, ok := rp.asyncSend(args)
			if !ok {
				// asyncNotify(rp.signalCh)
				continue
			}

			//reply := &AppendEntriesReply{}
			//ok := rp.sendAppendEntries(int(rp.followerID), args, reply)
			//if !ok {
			//	continue
			//}

			// rule-for-server
			if reply.Term > rp.termID {
				PrintDebug(rp.Raft,
					fmt.Sprintf("[replication] step down because old term. current-term: %d newer-term: %d node: %d",
						rp.termID, reply.Term, rp.followerID))
				asyncNotify(rp.stepDownCh)
				return
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
				oldNextIdx := rp.Raft.leaderState.getNextIdx(rp.followerID)
				rp.Raft.leaderState.decreaseNextIdx(rp.followerID)
				newNextIdx := rp.Raft.leaderState.getNextIdx(rp.followerID)
				PrintDebug(rp.Raft,
					fmt.Sprintf("[replication] job of node %d detects log inconsistent. Decrease from %d to %d",
						rp.followerID, oldNextIdx, newNextIdx))
				asyncNotify(rp.signalCh)
			}
		}
	}
}

func (rp *replicationJob) asyncSend(args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{}
	res := make(chan bool)
	go func() {
		ok := rp.sendAppendEntries(int(rp.followerID), args, reply)
		res <- ok
	}()

	timeout := time.After(100 * time.Millisecond)
	select {
	case e := <-res:
		if !e {
			fmt.Println("FUCK")
			return nil, false
		}
		return reply, true
	case <-timeout:
		fmt.Println("SHIT")
		return nil, false
	}

}
