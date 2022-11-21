package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"time"

	"github.com/imrenagi/raft"
	"github.com/rs/zerolog/log"
)

type Options struct {
	Port string

	RaftPort string
}

func New(o Options) *Server {
	return &Server{
		opts: o,
	}
}

type Server struct {
	opts Options
}

func (s Server) Run(ctx context.Context) error {

	r := raft.New(
		raft.WithServerPort(s.opts.RaftPort),
		raft.WithServerConfig(fmt.Sprintf("examples/shell_executor/tmp/%s.yaml", s.opts.RaftPort)),
	)
	go r.Run(ctx)

	shExec := shellExec{
		workDir: fmt.Sprintf("examples/shell_executor/out/%s", s.opts.Port),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {

		leader, err := r.GetLeaderAddr()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Debug().Msgf("leader addr: %s", leader)

		if r.Id != leader {
			http.Error(w, "im no leader", http.StatusUnprocessableEntity)
			return
		}

		log.Debug().Msgf("processing request because Im the leader")

		b, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		command := string(b)
		out, err := shExec.Apply(req.Context(), command)
		if err != nil {
			http.Error(w, fmt.Sprintf("command execution error: %s", err), http.StatusUnprocessableEntity)
			return
		}

		w.Write(out)
	})

	httpSrv := http.Server{
		Addr:    fmt.Sprintf(":%s", s.opts.Port),
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

type shellExec struct {
	workDir string
}

func (s shellExec) Apply(ctx context.Context, command string) ([]byte, error) {
	cmd := exec.Command("bash", "-c", command)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Dir = s.workDir

	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
