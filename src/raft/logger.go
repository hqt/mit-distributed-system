package raft

import (
	"fmt"
	"log"
)

// PrintDebug prints log at debug level
func PrintDebug(raft *Raft, msg string) {
	if Debug > 0 {
		log.Println(
			fmt.Sprintf("id=%d", raft.me),
			fmt.Sprintf("[%s]", raft.raftState.getState()),
			fmt.Sprintf("[term=%d]", raft.raftState.getCurrentTerm()),
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
