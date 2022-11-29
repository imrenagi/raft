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
		log.Debug().Msg("sending log to apply chan")
		return logF
	}
}

func (r *Raft) RequestVote(ctx context.Context, req *api.VoteRequest) (*api.VoteResponse, error) {
	r.Lock()
	defer r.Unlock()

	log.Debug().
		Uint64("cTerm", req.Term).
		Str("cId", req.CandidateId).
		Uint64("clastLastLogIdx", req.LastLogIdx).
		Uint64("cLastLogTerm", req.LastLogTerm).
		Str("srvVotedFor", r.VotedFor).
		Msgf("vote request is received")

	if r.CurrentTerm > req.Term {
		log.Debug().Msg("candidate is left behind")
		return &api.VoteResponse{
			Term:        r.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	if r.CurrentTerm == req.Term && r.VotedFor != "" && r.VotedFor != req.CandidateId {
		log.Debug().Msg("vote for current term has been given to other candidate")
		return &api.VoteResponse{
			Term:        r.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	lastIdx, _ := r.logStore.LastIndex()

	var lastLog Log
	if lastIdx > 0 {
		err := r.logStore.GetLog(lastIdx, &lastLog)
		if err != nil {
			return nil, err
		}
	}

	receiverLastLogIdx := lastLog.Index
	receiverLastLogTerm := lastLog.Term

	if receiverLastLogTerm == req.LastLogTerm {
		if req.LastLogIdx >= receiverLastLogIdx {
			log.Debug().Msg("candidate term is same and its log is longer or equal with receiver log")
			r.voteGrantedChan <- req
			return &api.VoteResponse{
				Term:        r.CurrentTerm,
				VoteGranted: true,
			}, nil
		}
	}

	if req.LastLogTerm > receiverLastLogTerm {
		log.Debug().Msg("candidate term is more up to date than the receiver term")
		r.voteGrantedChan <- req
		return &api.VoteResponse{
			Term:        r.CurrentTerm,
			VoteGranted: true,
		}, nil
	}

	log.Debug().Msg("vote is not granted. candidate doesn't satisfy any requirements to become leader")
	return &api.VoteResponse{
		Term:        r.CurrentTerm,
		VoteGranted: false,
	}, nil
}

func (r *Raft) AppendEntries(ctx context.Context, req *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	r.Lock()
	defer r.Unlock()

	log.Debug().
		Uint64("leaderTerm", req.Term).
		Str("leaderId", req.LeaderId).
		Uint64("leaderPrevLogIdx", req.PrevLogIdx).
		Uint64("leaderPrevLogTerm", req.PrevLogTerm).
		Uint64("leaderCommit", req.LeaderCommitIdx).
		Int("entriesLength", len(req.Entries)).
		Msg("receive append entries")

	// implementation 1
	if req.Term < r.CurrentTerm {
		return &api.AppendEntriesResponse{
			Term:    r.CurrentTerm,
			Success: false,
		}, nil
	}

	r.validLeaderHeartbeat <- req

	lastIdx, _ := r.logStore.LastIndex()

	if lastIdx == 0 { // follower has no log yet

		// check if prev log is 0
		if req.PrevLogIdx != 0 {
			// if not, return false
			// so that leader can retry until prevLogIdx = 0
			log.Debug().Msg("follower is left behind. need leader to send older logs")
			return &api.AppendEntriesResponse{
				Term:    r.CurrentTerm,
				Success: false,
			}, nil
		}
	} else { // when follower already has log

		var l Log
		err := r.logStore.GetLog(req.PrevLogIdx, &l)

		log := log.With().
			Uint64("leaderTerm", req.Term).
			Str("leaderId", req.LeaderId).
			Uint64("leaderPrevLogIdx", req.PrevLogIdx).
			Uint64("leaderPrevLogTerm", req.PrevLogTerm).
			Uint64("leaderCommit", req.LeaderCommitIdx).
			Uint64("serverPrevLogIdx", l.Index).
			Uint64("serverPrevLogTerm", l.Term).
			Logger()

		if err != nil && err != ErrLogNotFound {
			log.Debug().Msg("prev log idx not found")
			return nil, err
		}

		// case 2
		if err == ErrLogNotFound {
			log.Debug().
				Uint64("leaderPrevLogIdx", req.PrevLogIdx).
				Uint64("leaderPrevLogTerm", req.PrevLogTerm).
				Msg("prev log on the given index is not found")
			// should return false so that leader can perform consistency check
			return &api.AppendEntriesResponse{
				Term:    r.CurrentTerm,
				Success: false,
			}, nil
		}

		log.Debug().Msg("checking last log term")

		// case 4
		if l.Term != req.PrevLogTerm {
			log.Debug().
				Uint64("prevLogTerm", l.Term).
				Uint64("leaderPrevLogTerm", req.PrevLogTerm).
				Msg("prev log term doesnt match")
			// should return false so that leader can perform consistency check

			// delete the existing entry and all that follow it
			lastIdx, _ := r.logStore.LastIndex()

			err := r.logStore.DeleteRange(l.Index, lastIdx)
			if err != nil {
				log.Warn().Msg("unable to delete conflicted log")
				return nil, err
			}

			return &api.AppendEntriesResponse{
				Term:    r.CurrentTerm,
				Success: false,
			}, nil
		}
	}

	// append new entry to its log
	log.Debug().
		Int("entriesLength", len(req.Entries)).
		Msg("prepare committing entries to log storage")

	var newLogs []Log
	for _, entry := range req.Entries {

		newLog := Log{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		}
		log.Debug().
			Msgf("printing log %v", newLog)

		newLogs = append(newLogs, newLog)
	}

	err := r.logStore.StoreLogs(newLogs)
	if err != nil {
		return nil, err
	}

	if len(req.Entries) > 0 {
		lastNewEntry := req.Entries[len(req.Entries)-1:][0]
		if req.LeaderCommitIdx > r.commitIndex {
			if req.LeaderCommitIdx > lastNewEntry.Index {
				r.commitIndex = lastNewEntry.Index
			} else {
				r.commitIndex = req.LeaderCommitIdx
			}
		}
	}

	return &api.AppendEntriesResponse{
		Term:    r.CurrentTerm,
		Success: true,
	}, nil
}
