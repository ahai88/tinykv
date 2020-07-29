package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type reader struct {
	storage   *StandAloneStorage
	iterCount int
}

func (r *reader) GetCF(cf string, key []byte) ([]byte, error) {
	var ret []byte

	err := r.storage.db.View(func(txn *badger.Txn) error {
		realKey := createIndexKey(cf, string(key))
		item, err := txn.Get([]byte(realKey))
		if err != nil {
			return err
		}

		v, err := item.Value()
		if err != nil {
			return err
		}

		ret = v

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	var items []*item
	count := 0

	_ = r.storage.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		cfKey := createCF(cf)

		for it.Seek([]byte(cfKey)); it.ValidForPrefix([]byte(cfKey)); it.Next() {
			key := it.Item().Key()
			val, _ := it.Item().Value()
			items = append(items, &item{key: key, value: val})
			count++
		}

		return nil
	})

	return &iterator{
		collections: items,
		curIndex:    0,
		iterCount:   count,
	}
}

func (r *reader) Close() {
	return
}
