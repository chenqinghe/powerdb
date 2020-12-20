package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/chenqinghe/powerdb/config"
	dblib "github.com/chenqinghe/powerdb/db"
	"github.com/chenqinghe/powerdb/server"
)

var configFile string

func init() {
	flag.StringVar(&configFile, "c", "config.toml", "config file path")
	flag.Parse()
}

func main() {
	if configFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := config.Load(configFile); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	conf := config.Get()

	db, err := dblib.Open(conf.Engine, conf.DBPath, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	executor := dblib.NewExecutor(db)

	dbserver := server.NewServer(conf.Server.Addr)

	registerCommands(dbserver, executor)

	if err := dbserver.ListenAndServe(); err != nil {
		panic(err)
	}

}

func registerCommands(s *server.Server, e *dblib.Executor) {
	s.Handle("SET", e.Set)
	s.Handle("GET", e.Get)
}
