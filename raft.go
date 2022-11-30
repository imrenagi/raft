package raft

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

const (
	minElectionTimeoutMs int = 5000  // 150
	maxElectionTimeoutMs int = 10000 // 300
	maxAppendEntries     int = 128
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

func New(fsm FSM, opts ...Option) *Raft {
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
		validLeaderHeartbeat: make(chan *api.AppendEntriesRequest),
		applyChan:            make(chan *logFuture, maxAppendEntries),
		commitChan:           make(chan struct{}, maxAppendEntries),
		fsmMutateChan:        make(chan interface{}, maxAppendEntries),
		options:              options,
		fsm:                  fsm,
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

	fsm           FSM
	fsmMutateChan chan interface{}

	Role  string `yaml:"role"`
	state state

	servers []string

	electionTimeout time.Duration

	voteGrantedChan      chan *api.VoteRequest
	validLeaderHeartbeat chan *api.AppendEntriesRequest

	applyChan    chan *logFuture
	commitChan   chan struct{}
	shutdownChan chan struct{}

	options *Options

	leaderState *leaderState
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

	go r.runStateMachine()

	<-ctx.Done()

	if err = os.Remove(r.options.configPath); err != nil {
		log.Warn().Msg("unable to clean state file")
	}

	if err = os.Remove(fmt.Sprintf("examples/shell_executor/tmp/%s.db", r.options.port)); err != nil {
	}

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

type state interface {
	fmt.Stringer

	Run(ctx context.Context)
}

func (r Raft) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.CurrentTerm)
}

func (r Raft) getLastIndex() uint64 {
	// TODO use cache instead
	lastIdx, _ := r.logStore.LastIndex()
	return lastIdx
}

func (r *Raft) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *Raft) setCommitIndex(idx uint64) {
	atomic.StoreUint64(&r.commitIndex, idx)
}

func (r *Raft) getLastAppliedIndex() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *Raft) setLastAppliedIndex(idx uint64) {
	atomic.StoreUint64(&r.lastApplied, idx)
}

func (r *Raft) runStateMachine() {
	apply := func(future *logFuture) {
		var err error
		var res interface{}
		defer func() {
			if future != nil {
				future.response = res
				future.send(err)
			}
		}()
		res, err = r.fsm.Apply(&future.log)
	}

	for {
		select {
		case data := <-r.fsmMutateChan:

			switch d := data.(type) {
			case []*logFuture:
				for _, lf := range d {
					apply(lf)
				}
			}
		case <-r.shutdownChan:
			return
		}
	}
}

func (r *Raft) processLogs(lastIndexToApply uint64, logs map[uint64]*logFuture) error {
	lastAppliedIdx := r.getLastAppliedIndex()

	log.Debug().
		Uint64("lastAppliedIdx", lastAppliedIdx).
		Uint64("lastIndexToApply", lastIndexToApply).
		Msg("processing logs")

	if lastAppliedIdx > lastIndexToApply {
		return nil
	}

	applyBatch := func(logs []*logFuture) {
		select {
		case r.fsmMutateChan <- logs:
		case <-r.shutdownChan:
		}
	}

	var logsToApply []*logFuture
	for idx := lastAppliedIdx + 1; idx <= lastIndexToApply; idx++ {
		logF, ok := logs[idx]
		if ok {
			logsToApply = append(logsToApply, logF)
		} else {
			var logAtIdx Log
			if err := r.logStore.GetLog(idx, &logAtIdx); err != nil {
				return err
			}
			lf := &logFuture{
				deferError: deferError{},
				log:        logAtIdx,
			}
			lf.init()

			logsToApply = append(logsToApply, lf)
		}
	}

	log.Debug().Msgf("logs: %v", logsToApply)

	applyBatch(logsToApply)

	r.setLastAppliedIndex(lastIndexToApply)
	return nil
}

func (r *Raft) dispatchLogs(applyLogs []*logFuture) error {

	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()

	logs := make([]Log, len(applyLogs))

	for idx, nl := range applyLogs {
		lastIndex++
		nl.log.Term = term
		nl.log.Index = lastIndex
		logs[idx] = nl.log
		log.Debug().
			Uint64("term", nl.log.Term).
			Uint64("index", nl.log.Index).
			Msg("enqueue logs to inflight channel")
		r.leaderState.queue = append(r.leaderState.queue, nl)
	}

	log.Debug().Msgf("storing %d logs to disk", len(logs))
	if err := r.logStore.StoreLogs(logs); err != nil {
		log.Error().Err(err).Msg("error while storing logs to disk")
		for _, lf := range applyLogs {
			lf.send(err)
		}
	}
	r.leaderState.commitment.updateMatchIndex(r.Id, lastIndex)

	for _, repl := range r.leaderState.replState {
		select {
		case repl.triggerChan <- struct{}{}:
		default:
		}
	}

	return nil
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
