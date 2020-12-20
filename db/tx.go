package db

import (
	"errors"

	"github.com/dgraph-io/badger/v2"
	"go.etcd.io/bbolt"
)

type Txn interface {
	// Set overwrite old value with new value,
	// if the k not exist, create a new one
	Set(k, v []byte) error

	// SetIfNotExist set a value only if the k not exist
	SetIfNotExist(k, v []byte) error

	// SetWithExpire set a value with ttl
	SetWithExpire(k, v []byte) error

	// Replace replace old value with new value, and return old value
	Replace(k, v []byte) ([]byte, error)

	// IncrBy increase k's value by increment,
	// if k's value is not a number, an error will be returned
	IncrBy(k []byte, increment int64) (int64, error)

	// DecrBy decrease k's value by decrement,
	// if k's value is not a number, an error will be returned
	DecrBy(k []byte, decrement int64) (int64, error)

	// Get get a k'v value, nil will be returned when k not exist
	Get(k []byte) ([]byte, error)

	// MultiGet get all keys' values.
	MultiGet(keys [][]byte) ([][]byte, error)

	// Commit commit a transaction
	Commit() error

	// Rollback discard all write operations in a transaction
	Rollback() error
}

type badgerTxAdapter struct {
	tx *badger.Txn
}

func (tx *badgerTxAdapter) Get(k []byte) ([]byte, error) {
	item, err := tx.tx.Get(k)
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (tx *badgerTxAdapter) Set(k, v []byte) error {
	return tx.tx.Set(k, v)
}

// If Commit return's error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
// tree won't be updated, so there's no need for any rollback.
func (tx *badgerTxAdapter) Rollback() error {
	return nil
}

func (tx *badgerTxAdapter) Commit() error {
	return tx.tx.Commit()
}

func (tx *badgerTxAdapter) SetIfNotExist(k, v []byte) error {
	return notImplementError
}

func (tx *badgerTxAdapter) SetWithExpire(k, v []byte) error {
	return notImplementError
}

func (tx *badgerTxAdapter) Replace(k, v []byte) ([]byte, error) {
	return nil, notImplementError
}

func (tx *badgerTxAdapter) IncrBy(k []byte, increment int64) (int64, error) {
	return 0, notImplementError
}

func (tx *badgerTxAdapter) DecrBy(k []byte, decrement int64) (int64, error) {
	return 0, notImplementError
}

func (tx *badgerTxAdapter) MultiGet(keys [][]byte) ([][]byte, error) {
	return nil, notImplementError
}

var notImplementError = errors.New("not implemented")

type boltTxAdapter struct {
	tx *bbolt.Tx
}

var defaultBucket = []byte("defaultBucket")

func (tx *boltTxAdapter) Get(k []byte) ([]byte, error) {
	bucket := tx.tx.Bucket(defaultBucket)
	if bucket == nil {
		return nil, nil
	}
	return bucket.Get(k), nil
}

func (tx *boltTxAdapter) Set(k, v []byte) error {
	bucket, err := tx.tx.CreateBucketIfNotExists(defaultBucket)
	if err != nil {
		return err
	}
	return bucket.Put(k, v)
}

func (tx *boltTxAdapter) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *boltTxAdapter) Commit() error {
	return tx.tx.Commit()
}

func (tx *boltTxAdapter) SetIfNotExist(k, v []byte) error {
	return notImplementError
}

func (tx *boltTxAdapter) SetWithExpire(k, v []byte) error {
	return notImplementError
}

func (tx *boltTxAdapter) Replace(k, v []byte) ([]byte, error) {
	return nil, notImplementError
}

func (tx *boltTxAdapter) IncrBy(k []byte, increment int64) (int64, error) {
	return 0, notImplementError
}

func (tx *boltTxAdapter) DecrBy(k []byte, decrement int64) (int64, error) {
	return 0, notImplementError
}

func (tx *boltTxAdapter) MultiGet(keys [][]byte) ([][]byte, error) {
	return nil, notImplementError
}
