package config

import "flag"

type Params struct {
	Env        string
	ConfigPath string
}

func NewParams() *Params {
	var env, configPath string
	flag.StringVar(&env, "env", "local", "environment")
	flag.StringVar(&configPath, "conf", "./config/", "config path")
	flag.Parse()
	return &Params{env, configPath}
}
