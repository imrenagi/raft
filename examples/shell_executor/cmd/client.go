package cmd

import "github.com/spf13/cobra"

func clientCmd() *cobra.Command {
	var command = &cobra.Command{
		Use:   "client",
		Short: "Run the test client for sending command to the server",
		Run: func(c *cobra.Command, args []string) {

		},
	}
	return command
}
