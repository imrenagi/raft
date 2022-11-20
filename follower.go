package raft

import (
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
)

func newFollower(r *Raft) *follower {
	r.votedFor = "" // remove votedFor when it becomes follower again
	rand.Seed(time.Now().UnixNano())
	return &follower{
		Raft: r,
	}
}

type follower struct {
	*Raft
}

func (f *follower) Run() {
	log.Debug().
		Int32("currentTerm", f.currentTerm).
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
		case <-f.appendEntriesSuccessChan:
			log.Debug().
				Int32("currentTerm", f.currentTerm).
				Msgf("follower is receiving append entries")
		}
	}
}

func (f follower) String() string {
	return "follower"
}
