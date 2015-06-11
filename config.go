package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type CallbackItemConfig struct {
	Url        string   `json:"url"`
	RetryTimes int      `json:"retry_times"`
	Timeout    string   `json:"timeout"`
	Topics     []string `json:"topics"`
	Zookeepers []string `json:"zookeepers"`
	ZkPath     string   `json:"zk_path"`
}

type ServiceConfig struct {
	LogFile   string               `json:"log_file"`
	Callbacks []CallbackItemConfig `json:"callbacks"`
}

func loadConfig(configFile string) (*ServiceConfig, error) {
	var c *ServiceConfig
	path := configFile
	fi, err := os.Open(path)
	defer fi.Close()
	if nil != err {
		return nil, err
	}

	fd, err := ioutil.ReadAll(fi)
	err = json.Unmarshal([]byte(fd), &c)
	if nil != err {
		return nil, err
	}

	return c, nil
}
