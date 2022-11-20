package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/imrenagi/raft"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	r := raft.NewRaft()
	go r.Run()

	var counter int
	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		counter++
		fmt.Println("counter", counter)
		w.Write([]byte("ok"))
	})
	err := http.ListenAndServe(fmt.Sprintf("localhost:%s", os.Getenv("RAFT_SERVER_HTTP_PORT")), nil)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to start server")
	}
}
