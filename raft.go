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
		servers: []string{
			"127.0.0.1:8001",
			"127.0.0.1:8002",
			"127.0.0.1:8003",
		},
		voteGrantedChan:          make(chan *api.VoteRequest),
		appendEntriesSuccessChan: make(chan *api.AppendEntriesRequest),
		options:                  options,
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
		Int32("CurrentTerm", raft.CurrentTerm).
		Str("role", raft.state.String()).
		Msg("successfully read config file")

	return raft
}

type Raft struct {
	api.UnimplementedRaftServer `yaml:"-"`

	Id       string `yaml:"id"`
	LeaderId string `yaml:"leaderId"`

	// persistent state on all servers
	CurrentTerm int32  `yaml:"term"`
	VotedFor    string `yaml:"votedFor"`
	logs        []Log

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []interface{}
	matchIndex []interface{}

	Role  string `yaml:"role"`
	state state

	servers []string

	electionTimeout time.Duration

	voteGrantedChan          chan *api.VoteRequest
	appendEntriesSuccessChan chan *api.AppendEntriesRequest

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
	log.Debug().
		Int32("cTerm", req.Term).
		Str("cId", req.CandidateId).
		Int32("clastLastLogIdx", req.LastLogIdx).
		Int32("cLastLogTerm", req.LastLogTerm).
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
	// implementation 1
	if req.Term < r.CurrentTerm {
		return &api.AppendEntriesResponse{
			Term:    r.CurrentTerm,
			Success: false,
		}, nil
	}

	r.appendEntriesSuccessChan <- req

	return &api.AppendEntriesResponse{
		Term:    r.CurrentTerm,
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
