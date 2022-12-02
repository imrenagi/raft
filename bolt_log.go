package raft

import (
	"bytes"
	"encoding/binary"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/rs/zerolog/log"
)

type BoltOptions struct {
	Path    string
	Options *bolt.Options
}

var (
	logBucket    = []byte("logs")
	configBucket = []byte("config")
)

type BoltOption func(options *BoltOptions)

func WithPath(path string) BoltOption {
	return func(options *BoltOptions) {
		options.Path = path
	}
}

func NewBoltLogStore(opts ...BoltOption) *Bolt {
	options := &BoltOptions{
		Path:    "bolt.db",
		Options: &bolt.Options{},
	}

	for _, o := range opts {
		o(options)
	}

	db, err := bolt.Open(options.Path, 0600, options.Options)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to connect to boltdb")
	}

	b := &Bolt{
		conn: db,
		path: options.Path,
	}
	b.initialize()

	return b
}

type Bolt struct {
	conn *bolt.DB

	path string
}

func (b *Bolt) initialize() error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.CreateBucketIfNotExists(logBucket)
	if err != nil {
		return err
	}
	_, err = tx.CreateBucketIfNotExists(configBucket)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (b *Bolt) FirstIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(logBucket).Cursor()
	if k, _ := curs.First(); k == nil {
		return 0, nil
	} else {
		return bytesToUint64(k), nil
	}
}

func (b *Bolt) LastIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(logBucket).Cursor()
	if k, _ := curs.Last(); k == nil {
		return 0, nil
	} else {
		return bytesToUint64(k), nil
	}
}

func (b *Bolt) GetLog(idx uint64, log *Log) error {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	lb := tx.Bucket(logBucket).Get(uint64ToBytes(idx))
	if lb == nil {
		return ErrLogNotFound
	}

	return decodeMsgPack(lb, log)
}

func (b *Bolt) GetRangeLog(minIdx, maxIdx uint64) ([]Log, error) {
	min := uint64ToBytes(minIdx)

	var logs []Log
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(logBucket).Cursor()
	for k, val := curs.Seek(min); k != nil; k, val = curs.Next() {
		if bytesToUint64(k) > maxIdx {
			break
		}

		var log Log
		if err := decodeMsgPack(val, &log); err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, nil
}

func (b *Bolt) StoreLog(log Log) error {
	return b.StoreLogs([]Log{log})
}

func (b *Bolt) StoreLogs(logs []Log) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, l := range logs {
		bucket := tx.Bucket(logBucket)
		key := uint64ToBytes(l.Index)
		val, err := encodeMsgPack(l)
		if err != nil {
			return err
		}
		if err = bucket.Put(key, val.Bytes()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (b *Bolt) DeleteRange(minIdx, maxIdx uint64) error {
	min := uint64ToBytes(minIdx)

	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(logBucket).Cursor()
	for k, _ := curs.Seek(min); k != nil; k, _ = curs.Next() {
		if bytesToUint64(k) > maxIdx {
			break
		}

		if err := curs.Delete(); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (b *Bolt) Get(key []byte) ([]byte, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	lb := tx.Bucket(configBucket).Get(key)
	if lb == nil {
		return nil, ErrConfigNotFound
	}
	return lb, nil
}

func (b *Bolt) Set(key, val []byte) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = tx.Bucket(configBucket).Put(key, val)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (b *Bolt) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

func (b *Bolt) SetUint64(key []byte, val uint64) error {
	vb := uint64ToBytes(val)
	return b.Set(key, vb)
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}
