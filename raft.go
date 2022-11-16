package raft

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
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
	rand.Seed(time.Now().UnixNano())
	tms := rand.Intn(maxElectionTimeoutMs-minElectionTimeoutMs) + minElectionTimeoutMs
	raft := &Raft{
		id: os.Getenv("RAFT_SERVER_ID"),
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
	state := newFollower(raft)
	raft.ChangeState(state)
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

func (r *Raft) Run() {

	lis, err := net.Listen("tcp", r.server.Addr())
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}
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

	for {
		r.state.Run()
	}
}

func (r *Raft) RequestVote(ctx context.Context, req *api.VoteRequest) (*api.VoteResponse, error) {
	log.Debug().
		Int32("cTerm", req.Term).
		Str("cId", req.CandidateId).
		Int32("clastLastLogIdx", req.LastLogIdx).
		Int32("cLastLogTerm", req.LastLogTerm).
		Str("srvVotedFor", r.votedFor).
		Msgf("vote request is received")

	if r.currentTerm <= req.Term {
		// TODO(imre) possibility of race condition due to votedFor
		// is being used on the select case
		if r.votedFor == "" || r.votedFor == req.CandidateId {
			receiverLastLogIdx := int32(len(r.logs))
			if req.Term > r.currentTerm || // if candidate's term is the later
				(req.Term == r.currentTerm && receiverLastLogIdx <= req.LastLogIdx) { // if logs end up with the same term, then whichever has the longer
				log.Debug().Msg("granting vote")
				r.voteGrantedChan <- req
				return &api.VoteResponse{
					Term:        r.currentTerm,
					VoteGranted: true,
				}, nil
			}

		}
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
	Run()
}

func (r *Raft) ChangeState(state state) {
	r.state = state
}
