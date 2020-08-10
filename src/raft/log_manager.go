package raft

import (
	"sync"
)

// LogEntry represents log object
type LogEntry struct {
	Term  int64
	Index int64
	Data  interface{}
}

// LogManager manages log
type LogManager struct {
	mu   sync.RWMutex
	logs []LogEntry
}

// newLogManager returns log manager instance
func newLogManager() *LogManager {
	dump := LogEntry{
		Term:  -1, // returned term when no logs in database. useful for getLastLogIndex
		Index: 0,  // returned index when no index in database. useful for getLastLogTerm
		Data:  nil,
	}
	return &LogManager{
		logs: []LogEntry{dump}, // create dump entry. because raft's log index starts from 1
	}
}

func (m *LogManager) getLastLogTerm() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.logs[len(m.logs)-1].Term
}

func (m *LogManager) getLastLogIndex() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.logs[len(m.logs)-1].Index
}

func (m *LogManager) getLogAtIndex(idx int64) *LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if idx < 0 {
		panic("invalid id")
	}
	if int64(len(m.logs)) < idx+1 {
		return nil
	}
	return &m.logs[int(idx)]
}

func (m *LogManager) deleteLogsFrom(idx int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = m.logs[:idx+1]
}

// getLogsFromRange uses for replicating logs
func (m *LogManager) getLogsFromRange(from int, to int) []LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var res []LogEntry
	for i := from; i <= to; i++ {
		res = append(res, m.logs[i])
	}
	return res
}

func (m *LogManager) addLog(log LogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, log)
}

func (m *LogManager) length() int {
	return len(m.logs)
}
