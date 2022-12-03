package raft

type FSM interface {
	Apply(*Log) (interface{}, error)
}
