package raft

import (
	"context"
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

type leader struct {
	*Raft
}

func (l *leader) Run(ctx context.Context) {

	log.Info().Msg("running as leader")

	// TODO(imre) should send AppendEntriesRPC upon being elected.
	// it should not wait for the ticker
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("context is done")
			return
		case s, ok := <-l.appendEntriesSuccessChan:
			if ok {
				l.CurrentTerm = s.Term // TODO(imre) change this later
				l.LeaderId = s.LeaderId
				l.changeState(newFollower(l.Raft))
				return
			}
		case <-ticker.C:
			for _, server := range l.servers {
				go func(server string) {
					if server != l.Id {

						rpc := RPC{}
						res, err := rpc.AppendEntries(server, &api.AppendEntriesRequest{
							Term:            l.CurrentTerm,
							LeaderId:        l.Id,
							PrevLogIdx:      0,
							PrevLogTerm:     0,
							LeaderCommitIdx: 0,
							Entries:         nil,
						})
						if err != nil {
							log.Error().Err(err).Msg("unable to call append entries")
							return
						}

						log.Debug().
							Int32("CurrentTerm", l.CurrentTerm).
							Bool("success", res.Success).
							Msg("append entries is completed")
					}
				}(server)
			}
		}
	}
}

func (l leader) String() string {
	return "leader"
}
