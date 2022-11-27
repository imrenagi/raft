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

func (l *leader) initState() {

	l.nextIndex = make(map[string]int32)
	l.matchIndex = make(map[string]int32)

	lastLogIndex, err := l.logStore.LastIndex()
	if err != nil {
		log.Fatal().Err(err).Msg("cant get last index")
	}

	for _, s := range l.servers {
		l.nextIndex[s] = lastLogIndex + 1
		l.matchIndex[s] = 0 // TODO this should be updated once the appendentries succeed
	}
	log.Debug().Msgf("checking nextIndex %v", l.nextIndex)
	log.Debug().Msgf("checking matchIndex %v", l.matchIndex)
}

func (l *leader) Run(ctx context.Context) {

	log.Info().Msg("running as leader")

	l.initState()

	// TODO(imre) should send AppendEntriesRPC upon being elected.
	// it should not wait for the ticker
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("context is done")
			return
		case s, ok := <-l.validLeaderHeartbeat:
			if ok {
				l.CurrentTerm = s.Term // TODO(imre) change this later
				l.LeaderId = s.LeaderId
				l.changeState(newFollower(l.Raft))
				return
			}
		case <-ticker.C:
			lastIdx, err := l.logStore.LastIndex()
			if err != nil {
				log.Error().Err(err).Msg("unable to get last index")
			}

			var lastPrevLog Log
			if lastIdx != 0 {
				err := l.logStore.GetLog(lastIdx, &lastPrevLog)
				if err != nil {
					log.Error().Err(err).Msg("unable to get last log")
				}
			}

			for _, server := range l.servers {
				go func(server string) {
					if server != l.Id {

						rpc := RPC{}
						res, err := rpc.AppendEntries(server, &api.AppendEntriesRequest{
							Term:            l.CurrentTerm,
							LeaderId:        l.Id,
							PrevLogIdx:      lastPrevLog.Index,
							PrevLogTerm:     lastPrevLog.Term,
							LeaderCommitIdx: l.commitIndex,
							Entries:         nil, // heartbeat is expected to sent empty log
						})
						if err != nil {
							log.Error().Err(err).Msg("unable to call append entries")
							return
						}

						// TODO heartbeat can also do retry if res.Success is false

						log.Debug().
							Str("server", server).
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
