package raft

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

type Log struct {
	Command string
	Term    int32
}

type ServerAddr struct {
	ID   string
	Host string
	Port string
}

func (s ServerAddr) Addr() string {
	return fmt.Sprintf("%s:%s", s.Host, s.Port)
}

const (
	minElectionTimeoutMs int = 5000  // 150
	maxElectionTimeoutMs int = 10000 // 300
)

func NewRaft() *Raft {

	id := os.Getenv("RAFT_SERVER_ID")

	rand.Seed(time.Now().UnixNano())
	tms := rand.Intn(maxElectionTimeoutMs-minElectionTimeoutMs) + minElectionTimeoutMs
	raft := &Raft{
		id: id,
		server: ServerAddr{
			ID:   "1",
			Host: "127.0.0.1",
			Port: os.Getenv("RAFT_SERVER_PORT"),
		},
		electionTimeout: time.Duration(tms) * time.Millisecond,
		servers: []ServerAddr{
			{
				ID:   "1",
				Host: "127.0.0.1",
				Port: "8001",
			},
			{
				ID:   "2",
				Host: "127.0.0.1",
				Port: "8002",
			},
			{
				ID:   "3",
				Host: "127.0.0.1",
				Port: "8003",
			},
		},
		voteGrantedChan:          make(chan *api.VoteRequest),
		appendEntriesSuccessChan: make(chan *api.AppendEntriesRequest),
	}

	if err := raft.readState(); err != nil {
		log.Fatal().Err(err).Msg("unable to initialize state")
	}

	return raft
}

type Raft struct {
	api.UnimplementedRaftServer

	id     string
	server ServerAddr

	// persistent state on all servers
	currentTerm int32
	votedFor    string
	logs        []Log

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []interface{}
	matchIndex []interface{}

	state state

	servers []ServerAddr

	electionTimeout time.Duration

	voteGrantedChan          chan *api.VoteRequest
	appendEntriesSuccessChan chan *api.AppendEntriesRequest
}

func (r Raft) Stop() error {
	return nil
}

func (r *Raft) Run(ctx context.Context) {

	lis, err := net.Listen("tcp", r.server.Addr())
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}
	defer lis.Close()
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	api.RegisterRaftServer(grpcServer, r)

	log.Info().Msgf("starting grpc server on %s", r.server.Addr())

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

	grpcServer.GracefulStop()
	log.Warn().Msg("grpc server gracefully stopped")
}

func (r *Raft) RequestVote(ctx context.Context, req *api.VoteRequest) (*api.VoteResponse, error) {
	log.Debug().
		Int32("cTerm", req.Term).
		Str("cId", req.CandidateId).
		Int32("clastLastLogIdx", req.LastLogIdx).
		Int32("cLastLogTerm", req.LastLogTerm).
		Str("srvVotedFor", r.votedFor).
		Msgf("vote request is received")

	if r.currentTerm > req.Term {
		log.Debug().Msg("candidate is left behind")
		return &api.VoteResponse{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, nil
	}

	if r.currentTerm == req.Term && r.votedFor != "" && r.votedFor != req.CandidateId {
		log.Debug().Msg("vote for current term has been given to other candidate")
		return &api.VoteResponse{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, nil
	}

	receiverLastLogIdx := int32(len(r.logs))
	receiverLastLogTerm := int32(0)

	if receiverLastLogIdx != 0 {
		receiverLastLogTerm = r.logs[receiverLastLogIdx-1].Term
	}

	if receiverLastLogTerm == req.LastLogTerm {
		if req.LastLogIdx >= receiverLastLogIdx {
			log.Debug().Msg("candidate term is same and its log is longer or equal with receiver log")
			r.voteGrantedChan <- req
			return &api.VoteResponse{
				Term:        r.currentTerm,
				VoteGranted: true,
			}, nil
		}
	}

	if req.LastLogTerm > receiverLastLogTerm {
		log.Debug().Msg("candidate term is more up to date than the receiver term")
		r.voteGrantedChan <- req
		return &api.VoteResponse{
			Term:        r.currentTerm,
			VoteGranted: true,
		}, nil
	}

	log.Debug().Msg("vote is not granted. candidate doesn't satisfy any requirements to become leader")
	return &api.VoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}, nil
}

func (r *Raft) AppendEntries(ctx context.Context, req *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	// implementation 1
	if req.Term < r.currentTerm {
		return &api.AppendEntriesResponse{
			Term:    r.currentTerm,
			Success: false,
		}, nil
	}

	r.appendEntriesSuccessChan <- req

	return &api.AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: true,
	}, nil
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

func (r *Raft) voteGranted(toCandidate string, forTerm int32) error {
	r.votedFor = toCandidate
	r.currentTerm = forTerm

	if err := r.saveState(); err != nil {
		return err
	}
	return nil
}

type stateConf struct {
	Term     int32  `yaml:"term"`
	VotedFor string `yaml:"votedFor"`
	Role     string `yaml:"role"`
}

type stateOptions struct {
	fileName string
}

type stateOption func(*stateOptions)

const (
	stateFileFmt = "%s.state.yaml"
)

func (r *Raft) readState(opts ...stateOption) error {
	options := &stateOptions{
		fileName: fmt.Sprintf(stateFileFmt, r.id),
	}
	for _, o := range opts {
		o(options)
	}

	f, err := os.OpenFile(options.fileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	stateConf := &stateConf{}
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&stateConf)
	if err != nil && err != io.EOF {
		log.Error().Err(err).Msg("unable to decode message")
		return err
	}

	r.votedFor = stateConf.VotedFor
	r.currentTerm = stateConf.Term
	var raftRole state
	switch stateConf.Role {
	case "candidate":
		raftRole = newCandidate(r)
	case "leader":
		raftRole = newLeader(r)
	default:
		raftRole = newFollower(r)
	}
	r.changeState(raftRole)

	log.Debug().
		Str("votedFor", r.votedFor).
		Int32("currentTerm", r.currentTerm).
		Str("role", r.state.String()).
		Msg("successfully read config file")

	return nil
}

func (r Raft) saveState(opts ...stateOption) error {
	options := &stateOptions{
		fileName: fmt.Sprintf(stateFileFmt, r.id),
	}
	for _, o := range opts {
		o(options)
	}

	f, err := os.OpenFile(options.fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := yaml.NewEncoder(f)
	defer encoder.Close()
	if err = encoder.Encode(&stateConf{
		Term:     r.currentTerm,
		VotedFor: r.votedFor,
		Role:     r.state.String(),
	}); err != nil {
		return err
	}

	return nil
}
