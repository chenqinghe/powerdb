package db

import (
	"fmt"

	"github.com/chenqinghe/powerdb/server"
	"github.com/dgraph-io/badger/v2"
)

type Executor struct {
	db *DB
}

func NewExecutor(db *DB)*Executor {
	return &Executor{db:db}
}

func (e *Executor) Set(w server.ReplyWriter, req *server.Request) {
	if len(req.Args) != 2 {
		w.WriteErrorReply(fmt.Sprintf("wrong number args for command: %s", req.Command))
		return
	}
	err := e.db.Update(func(tx Txn) error {
		return tx.Set(req.Args[0], req.Args[1])
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			w.WriteNullBulkReply()
			return
		}
		fmt.Println("exec command error:" + err.Error())
		w.WriteErrorReply(fmt.Sprintf("exec command error: %s", err.Error()))
	} else {
		fmt.Println("ok")
		w.WriteOKReply()
	}
}

func (e *Executor) SetIfNotExist(w server.ReplyWriter, r *server.Request) {

}
func (e *Executor) SetWithExpire(w server.ReplyWriter, r *server.Request) {

}
func (e *Executor) Replace(w server.ReplyWriter, r *server.Request) {

}
func (e *Executor) IncrBy(w server.ReplyWriter, r *server.Request) {

}
func (e *Executor) DecrBy(w server.ReplyWriter, r *server.Request) {

}
func (e *Executor) Get(w server.ReplyWriter, req *server.Request) {
	if len(req.Args) != 1 {
		w.WriteErrorReply("wrong number args for command:" + req.Command)
		return
	}
	var val []byte
	err := e.db.View(func(tx Txn) error {
		var err error
		val, err = tx.Get(req.Args[0])
		return err
	})
	if err != nil {
		fmt.Println("exec command error:" + err.Error())
		w.WriteErrorReply("exec command error:" + err.Error())
	} else {
		fmt.Println("ok")
		w.WriteBulkReply(val)
	}
}
func (e *Executor) MultiGet(w server.ReplyWriter, r *server.Request) {

}
