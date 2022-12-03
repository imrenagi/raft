package server

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
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

	replShell := &replicatedShell{
		workDir: fmt.Sprintf("examples/shell_executor/out/%s", s.opts.Port),
	}

	r := raft.New(
		replShell,
		raft.WithServerPort(s.opts.RaftPort),
		raft.WithServerConfig(fmt.Sprintf("examples/shell_executor/tmp/%s.yaml", s.opts.RaftPort)),
	)

	go r.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {

		leader, err := r.GetLeaderAddr()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if r.Id != leader {
			http.Error(w, "im no leader", http.StatusUnprocessableEntity)
			return
		}

		// b, err := io.ReadAll(req.Body)
		// if err != nil {
		// 	http.Error(w, "invalid body", http.StatusBadRequest)
		// 	return
		// }

		rn := rand.Intn(1000)
		buf := bytes.NewBufferString(fmt.Sprintf("echo %d >> test.txt", rn))

		res := r.Apply(ctx, buf.Bytes())
		if res.Error() != nil {
			http.Error(w, fmt.Sprintf("cant commit %v", res.Error()), http.StatusUnprocessableEntity)
			return
		}

		if s, ok := res.Response().([]byte); !ok {
			http.Error(w, "cant cast state machine response", http.StatusInternalServerError)
			return
		} else {
			w.Write(s)
		}
	})
	mux.HandleFunc("/leader", func(w http.ResponseWriter, req *http.Request) {
		leader, err := r.GetLeaderAddr()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Debug().
			Str("serverId", r.Id).
			Str("serverLeaderId", r.LeaderId).
			Str("leaderId", leader). // incorrect
			Msg("checking leader")

		if r.Id != leader {
			http.Error(w, "im no leader", http.StatusServiceUnavailable)
			return
		}

		w.Write([]byte("ok"))
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

type replicatedShell struct {
	workDir string
}

func (s replicatedShell) Apply(l *raft.Log) (interface{}, error) {
	log.Info().Msgf("applying log idx %d to fsm %s", l.Index, string(l.Command))
	cmd := exec.Command("bash", "-c", string(l.Command))
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Dir = s.workDir

	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
