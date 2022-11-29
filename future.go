package raft

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

type Future interface {
	Error() error
}

type IndexFuture interface {
	Future
	Index() uint64
}

type ApplyFuture interface {
	IndexFuture
	Response() interface{}
}

type deferError struct {
	err          error
	errChan      chan error
	ShutdownChan chan struct{}
}

func (d *deferError) init() {
	d.errChan = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		return d.err
	}
	if d.errChan == nil {
		log.Fatal().Msg("error chan is not initialized")
	}

	select {
	case d.err = <-d.errChan:
	case <-d.ShutdownChan:
		d.err = fmt.Errorf("raft is shutdown")
	}
	return d.err
}

func (d *deferError) send(err error) {
	log.Debug().Msgf("deferError send %v", err)
	if d.errChan == nil {
		return
	}
	d.errChan <- err
	close(d.errChan)
}

type logFuture struct {
	deferError
	log      Log
	response interface{}
}

func (l logFuture) Index() uint64 {
	return l.log.Index
}

func (l logFuture) Response() interface{} {
	return l.response
}

type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func (e errorFuture) Index() uint64 {
	return 0
}

func (e errorFuture) Response() interface{} {
	return nil
}
