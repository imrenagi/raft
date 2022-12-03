package raft

import "fmt"

type ConfigStore interface {
	Get(key []byte) ([]byte, error)
	Set(key, val []byte) error
	GetUint64(key []byte) (uint64, error)
	SetUint64(key []byte, val uint64) error
}

var (
	ErrConfigNotFound = fmt.Errorf("config not found")
)
