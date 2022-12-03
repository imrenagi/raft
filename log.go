package raft

import "fmt"

type LogType int

func (lt LogType) String() string {
	switch lt {
	case LogNoOp:
		return "LogNoOp"
	default:
		return "LogCommand"
	}
}

func LogTypeFrom(s string) LogType {
	switch s {
	case "LogCommand":
		return LogCommand
	default:
		return LogNoOp
	}
}

const (
	LogCommand LogType = iota
	LogNoOp
)

type Log struct {
	Type    LogType
	Index   uint64
	Term    uint64
	Command []byte
}

func (l Log) String() string {
	return fmt.Sprintf("type: %d, index: %d, term: %d, command: %s", l.Type, l.Index, l.Term, string(l.Command))
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
