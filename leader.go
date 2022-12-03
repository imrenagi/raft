package raft

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
)

func newLeader(r *Raft) *leader {
	r.LeaderId = r.Id
	return &leader{
		Raft: r,
	}
}

func (r *Raft) setupLeaderState() {
	r.leaderState = &leaderState{
		queue:     make([]*logFuture, 0),
		replState: make(map[string]*followerReplication),
	}
}

type commitment struct {
	sync.Mutex
	// to notify commitIndex has increased
	commitChan   chan struct{}
	matchIndexes map[string]uint64
	commitIndex  uint64
}

func (c commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}

// updateMatchIndex might change the replication commitIndex if majority agrees to its new commitIndex
func (c *commitment) updateMatchIndex(server string, matchIndex uint64) {
	c.Lock()
	defer c.Unlock()

	if prev, ok := c.matchIndexes[server]; ok && matchIndex > prev {
		c.matchIndexes[server] = matchIndex

		// setting commitIndex:
		// if there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] >= N, and log.at(N).Term == currentTerm
		// set commitIndex = N
		if len(c.matchIndexes) == 0 {
			return
		}
		matches := make([]uint64, 0, len(c.matchIndexes))
		for _, val := range c.matchIndexes {
			matches = append(matches, val)
		}
		sort.Sort(byUint64(matches))
		quorumIndex := matches[(len(matches)-1)/2]
		if quorumIndex > c.commitIndex {
			c.commitIndex = quorumIndex
			select {
			case c.commitChan <- struct{}{}:
			default:
			}
		}
	}
}

type byUint64 []uint64

func (a byUint64) Len() int           { return len(a) }
func (a byUint64) Less(i, j int) bool { return a[i] < a[j] }
func (a byUint64) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func newCommitment(commitChan chan struct{}, servers []string, startIndex uint64) *commitment {
	c := &commitment{
		matchIndexes: make(map[string]uint64),
		commitIndex:  startIndex,
		commitChan:   commitChan,
	}

	// TODO after restart, this match index becomes zero
	for _, server := range servers {
		c.matchIndexes[server] = 0
	}

	return c
}

type leaderState struct {
	queue      []*logFuture // TODO use linkedlist?
	replState  map[string]*followerReplication
	commitment *commitment
}

type followerReplication struct {
	nextIndex   uint64
	currentTerm uint64

	server      string
	stopChan    chan struct{}
	triggerChan chan struct{}
}

type leader struct {
	*Raft
}

func (l *leader) startReplication() {
	lastIndex := l.getLastIndex()
	for _, server := range l.servers {
		if server == l.Id {
			continue
		}
		if _, ok := l.leaderState.replState[server]; !ok {
			followerReplication := &followerReplication{
				server:      server,
				stopChan:    make(chan struct{}, 1),
				triggerChan: make(chan struct{}, len(l.servers)),
				nextIndex:   lastIndex + 1,
				currentTerm: l.getCurrentTerm(),
			}
			l.leaderState.replState[server] = followerReplication
			go l.replicate(followerReplication)
		}
	}
}

func (l *leader) replicate(replication *followerReplication) {
	var shouldStop bool
	for !shouldStop {
		select {
		case <-replication.stopChan:
			return
		case <-time.After(l.heartbeatTimeout):
			lastIndex := l.getLastIndex()
			shouldStop = l.replicateTo(replication, lastIndex)
		case <-replication.triggerChan:
			lastIndex := l.getLastIndex()
			shouldStop = l.replicateTo(replication, lastIndex)
		}
	}
}

func (l *leader) replicateTo(s *followerReplication, lastLogIdx uint64) (shouldStop bool) {

	req := &api.AppendEntriesRequest{
		Term:            l.getCurrentTerm(),
		LeaderId:        l.Id,
		LeaderCommitIdx: l.getCommitIndex(),
	}

	err := l.prepareAppendEntries(req, atomic.LoadUint64(&s.nextIndex), lastLogIdx)
	if err != nil {
		return
	}

	rpc := RPC{}
	res, err := rpc.AppendEntries(s.server, req)
	if err != nil {
		return
	}

	if res.Term > req.Term {
		// TODO current leader is no longer leader
	}

	if res.Success {
		if logs := req.Entries; len(logs) > 0 {
			last := logs[len(logs)-1]
			atomic.StoreUint64(&s.nextIndex, last.Index+1)
			l.leaderState.commitment.updateMatchIndex(s.server, last.Index)
		}
	} else {
		atomic.StoreUint64(&s.nextIndex, s.nextIndex-1)
	}
	return
}

func (l *leader) prepareAppendEntries(req *api.AppendEntriesRequest, nextIndex, lastLogIdx uint64) error {
	if err := l.setPrevLog(req, nextIndex); err != nil {
		return err
	}

	if err := l.setLogEntries(req, nextIndex, lastLogIdx); err != nil {
		return err
	}
	return nil
}

func (l *leader) setLogEntries(req *api.AppendEntriesRequest, nextIndex, lastLogIdx uint64) error {
	var entries []*api.Log
	logs, err := l.logStore.GetRangeLog(nextIndex, lastLogIdx)
	if err != nil {
		return err
	}
	for _, log := range logs {
		entries = append(entries, &api.Log{
			Term:    log.Term,
			Index:   log.Index,
			Command: log.Command,
			Type:    log.Type.String(),
		})
	}
	req.Entries = entries
	return nil
}

func (l *leader) setPrevLog(req *api.AppendEntriesRequest, nextIndex uint64) error {
	if nextIndex == 1 {
		req.PrevLogIdx = 0
		req.PrevLogTerm = 0
	} else {
		var lastPrevLog Log
		err := l.logStore.GetLog(nextIndex-1, &lastPrevLog)
		if err != nil {
			return err
		}
		req.PrevLogIdx = lastPrevLog.Index
		req.PrevLogTerm = lastPrevLog.Term
	}
	return nil
}

func (l *leader) setLeaderState() {
	lastIdx := l.getLastIndex()

	var lastLog Log
	if lastIdx > 0 {
		if err := l.logStore.GetLog(lastIdx, &lastLog); err != nil {
			log.Fatal().Msgf("last log not found for index %d", lastIdx)
		}
	}

	l.leaderState = &leaderState{
		commitment: newCommitment(l.commitChan, l.servers, lastLog.Index),
		queue:      []*logFuture{},
		replState:  make(map[string]*followerReplication),
	}
}

func (l *leader) Run(ctx context.Context) {
	log.Info().Msg("running as leader")

	l.setLeaderState()

	go l.startReplication()

	noop := &logFuture{log: Log{Type: LogNoOp}}
	l.dispatchLogs([]*logFuture{noop})

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("context is done")
			return
		case s, ok := <-l.validLeaderHeartbeat:
			if ok {
				l.CurrentTerm = s.Term // TODO (imre) change this later
				l.LeaderId = s.LeaderId
				l.changeState(newFollower(l.Raft))
				return
			}
		case newLog := <-l.applyChan:
			ready := []*logFuture{newLog}
		GroupCommitLog:
			for i := 0; i <= maxAppendEntries; i++ {
				select {
				case nl := <-l.applyChan:
					log.Warn().Msg("more request on applychan")
					ready = append(ready, nl)
				default:
					break GroupCommitLog
				}
			}
			l.dispatchLogs(ready)
		case <-l.commitChan:
			commitIndex := l.leaderState.commitment.getCommitIndex()
			l.setCommitIndex(commitIndex)

			var lastIndexToApply uint64
			logsToApply := make(map[uint64]*logFuture)

			for _, item := range l.leaderState.queue {
				idx := item.log.Index
				if idx > commitIndex {
					break
				}
				logsToApply[idx] = item
				lastIndexToApply = idx
			}

			log.Debug().Msgf("len of logs to apply %d", len(logsToApply))
			if len(logsToApply) > 0 {
				if err := l.processLogs(lastIndexToApply, logsToApply); err != nil {
					continue
				}
				// remove applied logs
				// TODO this is potential issue for race condition
				l.leaderState.queue = []*logFuture{}
			}
		}
	}
}

func (l leader) String() string {
	return "leader"
}
