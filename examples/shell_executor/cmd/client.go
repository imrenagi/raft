package cmd

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func clientCmd() *cobra.Command {
	var (
		shellServerURL string
	)

	var command = &cobra.Command{
		Use:   "client",
		Short: "Run the test client for sending command to the server",
		RunE: func(c *cobra.Command, args []string) error {

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

			var count int
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(1 * time.Second):
					count += 1
					body := bytes.NewBufferString(fmt.Sprintf("echo %d >> mantappu.txt", count))
					_, err := http.Post(shellServerURL, "text/plain", body)
					if err != nil {
						return err
					}
				}
			}

			return nil
		},
	}

	command.Flags().StringVar(&shellServerURL, "server", "http://localhost:8080", "shell server url")

	return command
}
