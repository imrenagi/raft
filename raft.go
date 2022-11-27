package raft

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

const (
	minElectionTimeoutMs int = 5000  // 150
	maxElectionTimeoutMs int = 10000 // 300
)

type Options struct {
	port       string
	configPath string
}

type Option func(*Options)

func WithServerPort(port string) Option {
	return func(options *Options) {
		options.port = port
	}
}

func WithServerConfig(filePath string) Option {
	return func(options *Options) {
		options.configPath = filePath
	}
}

func defaultOptions() *Options {
	return &Options{
		configPath: "raft.yaml",
		port:       "8001",
	}
}

func New(opts ...Option) *Raft {
	options := defaultOptions()
	for _, o := range opts {
		o(options)
	}

	f, err := os.OpenFile(options.configPath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to open config file")
	}
	defer f.Close()

	rand.Seed(time.Now().UnixNano())
	tms := rand.Intn(maxElectionTimeoutMs-minElectionTimeoutMs) + minElectionTimeoutMs
	raft := &Raft{
		Id:              fmt.Sprintf("%s:%s", "127.0.0.1", options.port),
		electionTimeout: time.Duration(tms) * time.Millisecond,
		logStore: NewBoltLogStore(
			WithPath(fmt.Sprintf("examples/shell_executor/tmp/%s.db", options.port)),
		),
		servers: []string{
			"127.0.0.1:8001",
			"127.0.0.1:8002",
			"127.0.0.1:8003",
		},
		voteGrantedChan:      make(chan *api.VoteRequest),
		commitEntryChan:      make(chan Log),
		validLeaderHeartbeat: make(chan *api.AppendEntriesRequest),
		options:              options,
	}

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&raft)
	if err != nil && err != io.EOF {
		log.Fatal().Err(err).Msg("unable to decode message")
	}

	var raftRole state
	switch raft.Role {
	case "candidate":
		raftRole = newCandidate(raft)
	case "leader":
		raftRole = newLeader(raft)
	default:
		raftRole = newFollower(raft)
	}
	raft.changeState(raftRole)

	log.Debug().
		Str("votedFor", raft.VotedFor).
		Uint64("CurrentTerm", raft.CurrentTerm).
		Str("role", raft.state.String()).
		Msg("successfully read config file")

	return raft
}

type Raft struct {
	sync.Mutex
	api.UnimplementedRaftServer `yaml:"-"`

	Id       string `yaml:"id"`
	LeaderId string `yaml:"leaderId"`

	// persistent state on all servers
	CurrentTerm uint64 `yaml:"term"`
	VotedFor    string `yaml:"votedFor"`

	logStore LogStore

	// volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// volatile state on leaders
	// nextIndex for each server, index of the next log entry
	// to send to that server. initialized to leader last log index + 1
	nextIndex map[string]uint64
	// matchIndex for each server, index of the highest log entry
	// to be replicated on that server. initialized to 0, increases
	// monotonically
	matchIndex map[string]uint64

	Role  string `yaml:"role"`
	state state

	servers []string

	electionTimeout time.Duration

	voteGrantedChan      chan *api.VoteRequest
	validLeaderHeartbeat chan *api.AppendEntriesRequest
	commitEntryChan      chan Log

	options *Options
}

func (r *Raft) Run(ctx context.Context) {

	lis, err := net.Listen("tcp", r.Id)
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}
	defer lis.Close()
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	api.RegisterRaftServer(grpcServer, r)

	log.Info().Msgf("starting grpc server on %s", r.Id)

	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to start grpc server")
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Warn().Msg("raft loop exited")
				return
			default:
				r.state.Run(ctx)
			}
		}
	}()

	<-ctx.Done()

	// if err = os.Remove(r.options.configPath); err != nil {
	// 	log.Warn().Msg("unable to clean state file")
	// }

	grpcServer.GracefulStop()
	log.Warn().Msg("grpc server gracefully stopped")
}

func (r Raft) GetLeaderAddr() (string, error) {
	fmt.Println("leader id", r.LeaderId)

	if r.LeaderId == "" {
		return "", fmt.Errorf("no elected leader")
	}

	for _, s := range r.servers {
		if s == r.LeaderId {
			return s, nil
		}
	}
	return "", fmt.Errorf("no leader with identified leader id %s", r.LeaderId)
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

/*
leader      idx		1
leader 		term	1

// true ==> follower is still empty
follower    idx
follower	term
*/

/*
// idx 6 is the latest entry that leader want to append
// prevLogIdx is 6

leader      idx		1	2	3	4	5	6	7
leader 		term	1	1	1	1	1	1	2

// case 1
// true ==> follower has log in prevLogIdx with correct term

follower    idx		1	2	3	4	5	6
follower    term	1	1	1	1	1	1

// case 2
// new log [7] prevLogIdx 6 // false ==> follower doesnt has log in prevLogIdx
// new log [6, 7] prevLogIdx 5 // true
follower    idx		1	2	3	4	5
follower    term	1	1	1	1	1

// case 3
// false ==>  follower doesnt has log in prevLogIdx. its log is left behind several indices
// leader should reduce follower next index on its state and send all entries
// new log [5, 6, 7]
follower    idx		1	2	3	4
follower    term	1	1	1	1

// case 4
// false ==> conflict. follower should delete the existing entry and all that follow
// leader should reduce follower next index on its state and send all entries from the next index
follower    idx		1	2	3	4	5	6
follower    term	1	1	1	1	1	2

*/

type ApplyFn func(ctx context.Context, command []byte) error

func (r *Raft) CommitEntry(ctx context.Context, command []byte) error {

	log.Debug().Msg("trying to commit entry")

	lastPrevLogIdx, err := r.logStore.LastIndex()
	if err != nil {
		return err
	}

	newLog := Log{
		Index:   lastPrevLogIdx + 1,
		Term:    r.CurrentTerm,
		Command: command,
	}

	log.Debug().
		Uint64("index", newLog.Index).
		Uint64("term", newLog.Term).
		Str("command", string(newLog.Command)).
		Msg("created new log entry")

	// append to the log
	err = r.logStore.StoreLog(newLog)
	if err != nil {
		return err
	}

	r.nextIndex[r.Id] = newLog.Index + 1
	r.matchIndex[r.Id] = newLog.Index
	r.commitIndex++

	log.Debug().
		Msgf("log is stored %v", newLog)

	type appendEntriesRes struct {
		server string
		data   *api.AppendEntriesResponse
	}
	appendChan := make(chan appendEntriesRes, len(r.servers))

	logCommittedChan := make(chan struct{})
	commitFailedChan := make(chan error)

	for _, server := range r.servers {
		go func(server string) {
			if server != r.Id {
				rpc := RPC{}
				log.Debug().Msgf("append new entries for %s", server)

				counter := 0

				for {
					if counter > 5 {
						commitFailedChan <- fmt.Errorf("cant replicate")
						return
					}
					counter++

					leaderLastLogIndex, _ := r.logStore.LastIndex()
					followerNextIndex, _ := r.nextIndex[server]
					if leaderLastLogIndex >= followerNextIndex {

						// send append entries rpc with log entries
						// starting at next index
						// TODO change the use of range log into a single log sent sequentially
						logs, _ := r.logStore.GetRangeLog(followerNextIndex, leaderLastLogIndex)
						var entries []*api.Log
						for _, log := range logs {
							entries = append(entries, &api.Log{
								Term:    log.Term,
								Index:   log.Index,
								Command: log.Command,
							})
						}

						lastPrevLogIdx := followerNextIndex - 1
						if lastPrevLogIdx < 0 {
							lastPrevLogIdx = 0
						}

						var lastPrevLog Log
						err := r.logStore.GetLog(lastPrevLogIdx, &lastPrevLog)
						if err != nil {
							log.Warn().Msg("unable to get lastPrevLog")
						}

						log.Debug().
							Uint64("followerNextIndex", followerNextIndex).
							Uint64("leaderLastLogIndex", leaderLastLogIndex).
							Uint64("leaderLastPrevLogIndex", lastPrevLogIdx).
							Int("entriesLength", len(entries)).
							Msgf("trying to append entries to server %v", server)

						res, err := rpc.AppendEntries(server, &api.AppendEntriesRequest{
							Term:            r.CurrentTerm,
							LeaderId:        r.Id,
							PrevLogIdx:      lastPrevLog.Index,
							PrevLogTerm:     lastPrevLog.Term,
							LeaderCommitIdx: r.commitIndex,
							Entries:         entries,
						})
						if err != nil {
							// TODO if there is one or more follower error, retry indefinitely until all followers store all log entries
							log.Error().Err(err).Msg("unable to call append entries")
							continue
						}

						log.Info().
							Bool("success", res.Success).
							Uint64("term", res.Term).
							Str("server", server).
							Msg("received append entry response")

						// if successful, update nextIndex and matchIndex for follower
						if res.Success {
							r.nextIndex[server] = leaderLastLogIndex + 1
							r.matchIndex[server] = leaderLastLogIndex

							appendChan <- appendEntriesRes{
								server: server,
								data:   res,
							}
							break
						}

						r.nextIndex[server] -= 1
						log.Warn().
							Uint64("nextIndex", r.nextIndex[server]).
							Msgf("decrement server %v nextIndex", server)
					}
				}
			}
		}(server)
	}

	go func() {
		replicationSuccessCount := 1 // leader has committed the changes
		totalResponseReceived := 1
		for {
			select {
			case res, _ := <-appendChan:

				totalResponseReceived++
				if res.data.Success {
					replicationSuccessCount++
				} else {
					// TODO consistency check?
					// if there is one or more follower error, retry indefinitely untill all followers store all log entries
				}

				log.Debug().
					Str("server", res.server).
					Msgf("checking nextIndex %v", r.nextIndex)
				log.Debug().
					Str("server", res.server).
					Msgf("checking matchIndex %v", r.matchIndex)

				if replicationSuccessCount > len(r.servers)/2 {
					log.Info().Msg("entry is committed to majority of cluster")
					logCommittedChan <- struct{}{}
				}

				if totalResponseReceived > len(r.servers)/2 &&
					replicationSuccessCount < len(r.servers)/2 {
					commitFailedChan <- fmt.Errorf("not committed to majority")
				}
			}
		}
	}()

	select {
	case <-logCommittedChan:
		// TODO this still cant guarantee the ordering if there is concurrent request
		return nil
	case <-commitFailedChan:
		return err
	}

	// err = fn(ctx, l.Command)
	// if err != nil {
	// 	return err
	// }
	//
	// r.lastApplied = l.Index

	// when returning to the api client, we need to ensure that they apply it in correct order
	// that is why we need to get its callback so that we can call it from our controller goroutine and for select?
	return nil
}

type state interface {
	fmt.Stringer

	Run(ctx context.Context)
}

func (r *Raft) changeState(state state) error {
	log.Info().Msgf("role transition into %s", state)
	r.state = state

	if err := r.saveState(); err != nil {
		return err
	}
	return nil
}

func (r *Raft) voteGranted(toCandidate string, forTerm uint64) error {
	r.VotedFor = toCandidate
	r.CurrentTerm = forTerm

	if err := r.saveState(); err != nil {
		return err
	}
	return nil
}

func (r Raft) saveState() error {
	options := r.options

	f, err := os.OpenFile(options.configPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	// TODO change this on custom unmarshall
	r.Role = r.state.String()

	encoder := yaml.NewEncoder(f)
	defer encoder.Close()
	if err = encoder.Encode(&r); err != nil {
		return err
	}

	return nil
}
