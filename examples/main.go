package main

import (
	"os"

	"github.com/imrenagi/raft"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	r := raft.NewRaft()
	r.Run()
}
