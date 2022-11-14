package raft

import (
	"context"
	"time"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
)

func newLeader(r *Raft) *leader {
	return &leader{
		Raft: r,
	}
}

type leader struct {
	*Raft
}

func (l *leader) Run() {

	log.Info().Msg("running as leader")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, server := range l.servers {
				go func(server ServerAddr) {
					if server.ID != l.id {

						rpc := RPC{}
						res, err := rpc.AppendEntries(server, &api.AppendEntriesRequest{
							Term:            l.currentTerm,
							LeaderId:        l.id,
							PrevLogIdx:      0,
							PrevLogTerm:     0,
							LeaderCommitIdx: 0,
							Entries:         nil,
						})
						if err != nil {
							log.Error().Err(err).Msg("unable to call append entries")
						}

						log.Debug().
							Bool("success", res.Success).
							Msg("append entries is completed")
					}
				}(server)
			}
		}
	}

}

func (l *leader) AppendEntries(context.Context, *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	// if entries added by valid leader, push data to electedLeaderChan
	return nil, nil
}
