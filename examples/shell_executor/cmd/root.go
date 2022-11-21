package cmd

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	var command = &cobra.Command{
		Use:   "raft-shell",
		Short: "shell command executor backed with raft algorithm",
		Run: func(c *cobra.Command, args []string) {
			c.HelpFunc()(c, args)
		},
	}

	command.AddCommand(serverCmd(), clientCmd())
	return command
}
