package db

import (
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"go.etcd.io/bbolt"
)

type DB struct {
	engine
	driverName string
}

func (db *DB) View(fn func(tx Txn) error) error {
	return db.engine.View(fn)
}

func (db *DB) Update(fn func(tx Txn) error) error {
	return db.engine.Update(fn)
}

func (db *DB) Close() error {
	return db.engine.Close()
}

type Option struct {
	ReadOnly bool
}

var defaultOption = &Option{
	ReadOnly: false,
}

func Open(driver string, path string, option *Option) (*DB, error) {
	if option == nil {
		option = defaultOption
	}
	var ng engine
	switch driver {
	case "badger":
		badgerDB, err := badger.Open(badger.DefaultOptions(path).WithReadOnly(option.ReadOnly))
		if err != nil {
			return nil, err
		}
		ng = &badgerAdapter{
			badgerDB: badgerDB,
		}
	case "bolt":
		boltDB, err := bbolt.Open(path, 0644, &bbolt.Options{
			ReadOnly: option.ReadOnly,
		})
		if err != nil {
			return nil, err
		}
		ng = &boltAdapter{
			boltDB: boltDB,
		}
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}
	return &DB{
		engine:     ng,
		driverName: driver,
	}, nil
}
