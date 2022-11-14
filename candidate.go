package raft

import (
	"context"
	"time"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
)

func newCandidate(r *Raft) *candidate {
	r.votedFor = r.id

	c := &candidate{
		Raft:       r,
		totalVotes: 1, // voting for itself
	}
	r.state = c
	return c
}

type candidate struct {
	*Raft
	totalVotes int

	// electedLeaderChan is used to notify the candidate to change its state to follower
	// because new leader is elected
	electedLeaderChan chan struct{}
}

func (c *candidate) Run() {
	c.currentTerm++ // increment current term

	log.Debug().
		Int32("currentTerm", c.currentTerm).
		Msg("candidate run")

	// vote itself
	// send request vote RPC to other ServerAddr
	voteResponseChan := make(chan *api.VoteResponse, len(c.servers))
	for _, server := range c.servers {
		go func(server ServerAddr) {
			if c.id != server.ID {
				log.Debug().
					Str("voter", server.Addr()).
					Msg("requesting vote")

				rpc := RPC{}
				res, err := rpc.RequestVote(server, &api.VoteRequest{
					Term:        c.currentTerm,
					CandidateId: c.id,
					LastLogIdx:  0,
					LastLogTerm: 0,
				})
				if err != nil {
					log.Error().Err(err).Msg("fail to request for vote")
				}
				log.Info().
					Str("voter", server.Addr()).
					Bool("vote_granted", res.VoteGranted).
					Msg("received vote response")
				voteResponseChan <- res
			}
		}(server)
	}

	for {
		select {
		case r, ok := <-voteResponseChan:
			log.Debug().Msg("processing vote")
			if ok && r.VoteGranted {
				c.totalVotes++
			}
			if c.totalVotes > len(c.servers)/2 {
				log.Debug().
					Int("total_votes", c.totalVotes).
					Msgf("received majority vote")
				// transition to leader
				c.ChangeState(newLeader(c.Raft))
				return
			}
		case <-c.electedLeaderChan:
			log.Debug().Msg("changing state to follower")
			c.ChangeState(newFollower(c.Raft))
			return
		case <-time.After(c.electionTimeout):
			log.Debug().Msg("voting timeout")
			c.ChangeState(newFollower(c.Raft))
			return
		}
	}
}

func (c *candidate) AppendEntries(context.Context, *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {

	c.ChangeState(newFollower(c.Raft))
	// TODO after changing state append entries need to append log as well.
	return nil, nil
}
