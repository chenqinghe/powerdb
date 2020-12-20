package config

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
)

type Config struct {
	Engine string       `toml:"engine"`
	DBPath string       `toml:"dbpath"`
	Server ServerConfig `toml:"server"`
}

type ServerConfig struct {
	Addr string `toml:"addr"`
}

var cfgInstance *Config

func Load(file string) error {
	cfgInstance = new(Config)
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	return toml.Unmarshal(data, cfgInstance)
}

func Get() *Config {
	c := new(Config)
	if cfgInstance != nil {
		*c = *cfgInstance
	}

	return c
}
