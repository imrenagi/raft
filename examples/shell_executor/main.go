package main

import (
	"github.com/imrenagi/raft/examples/shell_executor/cmd"
	"github.com/rs/zerolog/log"
)

func main() {
	err := cmd.NewCommand().Execute()
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
}
