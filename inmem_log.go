package raft

import "sync"

func NewInMemoryLogStore() *InMemoryStore {
	return &InMemoryStore{
		lowIndex:  0,
		highIndex: 0,
		logs:      make(map[uint64]Log),
	}
}

type InMemoryStore struct {
	sync.Mutex

	lowIndex  uint64
	highIndex uint64
	logs      map[uint64]Log
}

func (i *InMemoryStore) FirstIndex() (uint64, error) {
	i.Lock()
	defer i.Unlock()
	return i.lowIndex, nil
}

func (i *InMemoryStore) LastIndex() (uint64, error) {
	i.Lock()
	defer i.Unlock()
	return i.highIndex, nil
}

func (i *InMemoryStore) GetLog(idx uint64, log *Log) error {
	i.Lock()
	defer i.Unlock()
	l, ok := i.logs[idx]
	if !ok {
		return ErrLogNotFound
	}
	*log = l
	return nil
}

func (i *InMemoryStore) GetRangeLog(min, max uint64) ([]Log, error) {
	i.Lock()
	defer i.Unlock()

	var logs []Log
	for j := min; j <= max; j++ {
		val, ok := i.logs[j]
		if !ok {
			break
		}
		logs = append(logs, val)
	}
	return logs, nil
}

func (i *InMemoryStore) StoreLog(log Log) error {
	return i.StoreLogs([]Log{log})
}

func (i *InMemoryStore) StoreLogs(logs []Log) error {
	i.Lock()
	defer i.Unlock()
	for _, l := range logs {
		i.logs[l.Index] = l
		if i.lowIndex == 0 {
			i.lowIndex = l.Index
		}
		if l.Index > i.highIndex {
			i.highIndex = l.Index
		}
	}
	return nil
}

func (i *InMemoryStore) DeleteRange(minIdx, maxIdx uint64) error {
	i.Lock()
	defer i.Unlock()
	for j := minIdx; j <= maxIdx; j++ {
		delete(i.logs, j)
	}

	if minIdx <= i.lowIndex {
		i.lowIndex = maxIdx + 1
	}
	if maxIdx >= i.highIndex {
		i.highIndex = minIdx - 1
	}
	if i.lowIndex >= i.highIndex {
		i.lowIndex = 0
		i.highIndex = 0
	}
	return nil
}
