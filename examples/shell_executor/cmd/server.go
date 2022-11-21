package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/imrenagi/raft/examples/shell_executor/server"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func serverCmd() *cobra.Command {
	var (
		raftServerPort  string
		shellServerPort string
	)

	var command = &cobra.Command{
		Use:   "server",
		Short: "Run the shell executor server",
		Run: func(c *cobra.Command, args []string) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt)
			signal.Notify(ch, syscall.SIGTERM)

			go func() {
				oscall := <-ch
				log.Warn().Msgf("system call:%+v", oscall)
				cancel()
			}()

			srv := server.New(server.Options{
				Port:     shellServerPort,
				RaftPort: raftServerPort,
			})
			err := srv.Run(ctx)
			if err != nil {
				log.Fatal().Msg("unable to run the shell executor server")
			}
		},
	}

	command.Flags().StringVar(&raftServerPort, "raft-port", "8001", "raft listener port")
	command.Flags().StringVar(&shellServerPort, "port", "9001", "shell executor listener port")

	return command
}
