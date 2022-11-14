package raft

import (
	"context"

	"github.com/imrenagi/raft/api"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RPC struct {
}

func (r RPC) RequestVote(server ServerAddr, req *api.VoteRequest) (*api.VoteResponse, error) {
	conn, err := grpc.Dial(server.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error().Err(err).Msg("unable to dial the server")
		return nil, err
	}
	defer conn.Close()
	client := api.NewRaftClient(conn)
	return client.RequestVote(context.TODO(), req)
}

func (r RPC) AppendEntries(server ServerAddr, req *api.AppendEntriesRequest) (*api.AppendEntriesResponse, error) {
	conn, err := grpc.Dial(server.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error().Err(err).Msg("unable to dial the server")
		return nil, err
	}
	defer conn.Close()
	client := api.NewRaftClient(conn)
	return client.AppendEntries(context.TODO(), req)
}
