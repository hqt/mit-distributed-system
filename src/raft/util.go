package raft

import (
	"log"
	"math/rand"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func randomInt(min int, max int) int {
	return rand.Intn(max-min) + min
}

// random a timeout and return channel for asynchronous processing
func randomTimeout(minMs int, maxMs int) <-chan time.Time {
	v := randomInt(minMs, maxMs)
	return time.After(time.Duration(v) * time.Millisecond)
}
