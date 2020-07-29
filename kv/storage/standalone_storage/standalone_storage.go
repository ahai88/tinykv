package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"io/ioutil"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	_, err := os.Stat(conf.DBPath)
	if err != nil && os.IsNotExist(err) {
		_ = os.MkdirAll(conf.DBPath, 0700)
	}

	dir, err := ioutil.TempDir(conf.DBPath, "")
	if err != nil {
		panic(err)
	}

	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}

	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &reader{storage: s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, b := range batch {
		key := createIndexKey(b.Cf(), string(b.Key()))
		switch b.Data.(type) {
		case storage.Put:
			{
				err := s.db.Update(func(txn *badger.Txn) error {
					return txn.Set([]byte(key), b.Value())
				})

				if err != nil {
					return err
				}
			}
		case storage.Delete:
			{
				err := s.db.Update(func(txn *badger.Txn) error {
					return txn.Delete([]byte(key))
				})

				if err != nil {
					return err
				}

				return nil
			}
		}
	}

	return nil
}
