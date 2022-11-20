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
}

func (c *candidate) Run(ctx context.Context) {
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
					return
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
				c.changeState(newLeader(c.Raft))
				return
			}
		case voteReq, _ := <-c.voteGrantedChan:
			if err := c.voteGranted(voteReq.CandidateId, voteReq.Term); err != nil {
				log.Error().Err(err).Msg("unable to update state after vote is granted")
			}
		case s, ok := <-c.appendEntriesSuccessChan:
			if ok {
				c.currentTerm = s.Term // TODO(imre) change this later
				c.changeState(newFollower(c.Raft))
				return
			}
		case <-time.After(c.electionTimeout):
			log.Debug().Msg("voting timeout")
			c.changeState(newFollower(c.Raft))
			return
		case <-ctx.Done():
			log.Info().Msg("context is done")
			return
		}
	}
}

func (c candidate) String() string {
	return "candidate"
}
