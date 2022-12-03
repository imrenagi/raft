package raft

import (
	"context"
	"fmt"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
)

func (r *Raft) Apply(ctx context.Context, cmd []byte) ApplyFuture {

	logF := &logFuture{
		log: Log{
			Type:    LogCommand,
			Command: cmd,
		},
	}
	logF.init()

	select {
	case <-ctx.Done():
		return errorFuture{err: fmt.Errorf("context is done")}
	case <-r.shutdownChan:
		return errorFuture{err: fmt.Errorf("raft is shutting down")}
	case r.applyChan <- logF:
		return logF
	}
}

func (r *Raft) RequestVote(ctx context.Context, req *api.VoteRequest) (*api.VoteResponse, error) {
	res := &api.VoteResponse{
		Term:        r.getCurrentTerm(),
		VoteGranted: false,
	}

	if r.CurrentTerm > req.Term {
		log.Debug().Msg("candidate is left behind")
		return res, nil
	}

	if r.CurrentTerm == req.Term && r.VotedFor != "" && r.VotedFor != req.CandidateId {
		log.Debug().Msg("vote for current term has been given to other candidate")
		return res, nil
	}

	lastIdx, _ := r.logStore.LastIndex()

	var lastLog Log
	if lastIdx > 0 {
		err := r.logStore.GetLog(lastIdx, &lastLog)
		if err != nil {
			return nil, err
		}
	}

	if lastLog.Term == req.LastLogTerm {
		if req.LastLogIdx >= lastLog.Index {
			log.Debug().Msg("candidate term is same and its log is longer or equal with receiver log")
			r.voteGrantedChan <- req
			res.VoteGranted = true
			return res, nil
		}
	}

	if req.LastLogTerm > lastLog.Term {
		log.Debug().Msg("candidate term is more up to date than the receiver term")
		r.voteGrantedChan <- req
		res.VoteGranted = true
		return res, nil
	}

	log.Debug().Msg("vote is not granted. candidate doesn't satisfy any requirements to become leader")
	return res, nil
}

func (r *Raft) AppendEntries(ctx context.Context, req *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	res := &api.AppendEntriesResponse{
		Term:    r.getCurrentTerm(),
		Success: false,
	}

	// implementation 1
	if req.Term < r.CurrentTerm {
		return res, nil
	}

	r.validLeaderHeartbeat <- req // TODO remove this. I think we dont need this

	lastIdx := r.getLastIndex()

	if lastIdx == 0 {
		if req.PrevLogIdx != 0 {
			log.Debug().Msg("follower is left behind. need leader to send older logs")
			return res, nil
		}
	} else {
		var prevLog Log

		err := r.logStore.GetLog(req.PrevLogIdx, &prevLog)
		if err != nil && err != ErrLogNotFound {
			log.Debug().Err(err).Msg("prev log idx not found")
			return nil, err
		}

		// prevLog not found. return false so that leader can perform consistency check
		if err == ErrLogNotFound {
			log.Debug().Msg("prev log on the given index is not found")
			return res, nil
		}

		// case 4
		if prevLog.Term != req.PrevLogTerm {
			// delete the existing entry and all that follow it
			lastIdx := r.getLastIndex()
			err := r.logStore.DeleteRange(prevLog.Index, lastIdx)
			if err != nil {
				log.Warn().Msg("unable to delete conflicted log")
				return nil, err
			}
			return res, nil
		}
	}

	var newLogs []Log
	logFutures := make(map[uint64]*logFuture)

	for _, entry := range req.Entries {
		newLog := Log{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
			Type:    LogTypeFrom(entry.Type),
		}
		newLogs = append(newLogs, newLog)
		logFutures[entry.Index] = newLogFuture(newLog)
		log.Debug().Msgf("printing log %s", newLog.String())
	}

	err := r.logStore.StoreLogs(newLogs)
	if err != nil {
		return nil, err
	}

	lastIdx = r.getLastIndex()
	if req.LeaderCommitIdx > 0 && req.LeaderCommitIdx > r.getCommitIndex() {
		newCommitIndex := min(req.LeaderCommitIdx, lastIdx)
		r.setCommitIndex(newCommitIndex)
		r.processLogs(newCommitIndex, logFutures)
	}

	res.Success = true
	return res, nil
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}
