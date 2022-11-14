package raft

import (
	"context"
	"math/rand"
	"time"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
)

func newFollower(r *Raft) *follower {
	r.votedFor = ""
	rand.Seed(time.Now().UnixNano())
	return &follower{
		Raft:              r,
		voteRequestChan:   make(chan *api.VoteRequest, len(r.servers)),
		appendEntriesChan: make(chan *api.AppendEntriesRequest),
	}
}

type follower struct {
	*Raft
	voteRequestChan   chan *api.VoteRequest
	appendEntriesChan chan *api.AppendEntriesRequest
}

func (f *follower) Run() {
	log.Debug().
		Int32("currentTerm", f.currentTerm).
		Msg("follower run")
	for {
		select {
		case <-time.After(f.electionTimeout):
			log.Debug().Msg("election timeout")
			f.ChangeState(newCandidate(f.Raft))
			return
		case <-f.appendEntriesChan:
			log.Debug().Msgf("follower is receiving append entries")
		}
	}
}

func (f follower) AppendEntries(ctx context.Context, r *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	f.appendEntriesChan <- r
	return nil, nil
}
