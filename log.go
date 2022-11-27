package raft

import "fmt"

type Log struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// LogStore is log storage interface used for retrieving and
// managing logs
type LogStore interface {
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(idx uint64, log *Log) error
	// GetRangeLog return log starting from min index until max index
	GetRangeLog(min, max uint64) ([]Log, error)
	StoreLog(log Log) error
	StoreLogs(logs []Log) error
	DeleteRange(minIdx, maxIdx uint64) error
}

var (
	ErrLogNotFound = fmt.Errorf("log not found")
)
