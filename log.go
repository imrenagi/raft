package raft

import "fmt"

type Log struct {
	Index   int32
	Term    int32
	Command []byte
}

// LogStore is log storage interface used for retrieving and
// managing logs
type LogStore interface {
	FirstIndex() (int32, error)
	LastIndex() (int32, error)
	GetLog(idx int32, log *Log) error
	// GetRangeLog return log starting from min index until max index
	GetRangeLog(min, max int32) ([]Log, error)
	StoreLog(log Log) error
	StoreLogs(logs []Log) error
	DeleteRange(minIdx, maxIdx int32) error
}

var (
	ErrLogNotFound = fmt.Errorf("log not found")
)
