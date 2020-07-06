package raft

import (
	"fmt"
	"log"
)

// RaftInfo interface to get info from raft for logging
type RaftInfo interface {
	nodeID() int64
	getState() PeerState
	getCurrentTerm() int64
}

// PrintDebug prints log at debug level
func PrintDebug(raft RaftInfo, msg string) {
	if Debug > 0 {
		log.Println(
			fmt.Sprintf("id=%d", raft.nodeID()),
			fmt.Sprintf("[%s]", raft.getState()),
			fmt.Sprintf("[term=%d]", raft.getCurrentTerm()),
			msg,
		)
	}
}

// PrintInfo print log at info level
func PrintInfo(raft *Raft, msg string) {
	log.Println(
		fmt.Sprintf("id=%d", raft.me),
		fmt.Sprintf("[%s]", raft.raftState.getState()),
		fmt.Sprintf("[term=%d]", raft.raftState.getCurrentTerm()),
		msg,
	)
}
