package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/imrenagi/raft"
	"github.com/rs/zerolog/log"
)

type Server struct {
}

func (s Server) Run(ctx context.Context, port int) error {

	r := raft.NewRaft()
	go r.Run(ctx)

	var counter int

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		counter++
		fmt.Println("counter", counter)
		w.Write([]byte("ok"))
	})

	httpSrv := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	go func() {
		err := httpSrv.ListenAndServe()
		if err != nil {
			log.Warn().Err(err).Msg("http server listen get error")
		}
	}()

	log.Info().Msg("shell server started")
	<-ctx.Done()

	log.Warn().Msg("stopping shell server")
	ctxShutDown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		cancel()
	}()

	httpSrv.Shutdown(ctxShutDown)
	log.Warn().Msg("shell server gracefully stopped")

	<-time.After(1 * time.Second)
	return nil
}
