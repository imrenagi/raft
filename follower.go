package raft

import (
	"context"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
)

func newFollower(r *Raft) *follower {
	r.VotedFor = "" // remove votedFor when it becomes follower again
	rand.Seed(time.Now().UnixNano())
	return &follower{
		Raft: r,
	}
}

type follower struct {
	*Raft
}

func (f *follower) Run(ctx context.Context) {
	log.Debug().
		Int32("CurrentTerm", f.CurrentTerm).
		Msg("follower run")
	for {
		select {
		case <-time.After(f.electionTimeout):
			log.Debug().Msg("election timeout")
			f.changeState(newCandidate(f.Raft))
			return
		case voteReq, _ := <-f.voteGrantedChan:
			if err := f.voteGranted(voteReq.CandidateId, voteReq.Term); err != nil {
				log.Error().Err(err).Msg("unable to update state after vote is granted")
			}
		case s, _ := <-f.validLeaderHeartbeat:

			if f.LeaderId != s.LeaderId {
				f.LeaderId = s.LeaderId // TODO(imre) change this later and its safe
				f.CurrentTerm = s.Term
				if err := f.saveState(); err != nil {
					// return
				}
			}

			firstIdx, _ := f.logStore.FirstIndex()
			newLastIdx, _ := f.logStore.LastIndex()

			log.Debug().
				Int32("firstIdx", firstIdx).
				Int32("newLastIdx", newLastIdx).
				Int32("CurrentTerm", f.CurrentTerm).
				Msgf("follower is receiving append entries")
		case <-ctx.Done():
			log.Info().Msg("context is done")
			return
		}
	}
}

func (f follower) String() string {
	return "follower"
}
