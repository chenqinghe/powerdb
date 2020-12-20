package db

import (
	"github.com/dgraph-io/badger/v2"
	"go.etcd.io/bbolt"
)

type engine interface {
	// View start a Read-only transaction
	View(fn func(tx Txn) error) error

	// Update start a read-write transaction
	Update(fn func(tx Txn) error) error

	// Close close database and do some cleaning stuff
	Close() error
}

type badgerAdapter struct {
	badgerDB *badger.DB
}

func (ba *badgerAdapter) View(fn func(tx Txn) error) error {
	return ba.badgerDB.View(func(tx *badger.Txn) error {
		return fn(&badgerTxAdapter{tx: tx})
	})
}

func (ba *badgerAdapter) Update(fn func(tx Txn) error) error {
	return ba.badgerDB.Update(func(tx *badger.Txn) error {
		return fn(&badgerTxAdapter{tx: tx})
	})
}

func (ba *badgerAdapter) Close() error {
	return ba.badgerDB.Close()
}

type boltAdapter struct {
	boltDB *bbolt.DB
}

func (a *boltAdapter) View(fn func(tx Txn) error) error {
	return a.boltDB.View(func(tx *bbolt.Tx) error {
		return fn(&boltTxAdapter{tx: tx})
	})
}

func (a *boltAdapter) Update(fn func(tx Txn) error) error {
	return a.boltDB.Update(func(tx *bbolt.Tx) error {
		return fn(&boltTxAdapter{tx: tx})
	})
}

func (a *boltAdapter) Close() error {
	return a.boltDB.Close()
}
