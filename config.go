package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/golang/glog"
	"net/http"
)

const (
	CFG_DEFAULT_TIMEOUT      = time.Second
	CFG_DEFAULT_FAILED_SLEEP = time.Second
	CFG_MIN_FAILED_SLEEP     = time.Second
)

type CallbackItemConfig struct {
	WorkerNum          int           `json:"worker_num"`
	Url                string        `json:"url"`
	RetryTimes         int           `json:"retry_times"`
	TimeoutStr         string        `json:"timeout"`
	Timeout            time.Duration `json:"null,omitempty"`
	BypassFailed       bool          `json:"bypass_failed"`
	FailedSleepStr     string        `json:"failed_sleep"`
	FailedSleep        time.Duration `json:"null,omitempty"`
	Topics             []string      `json:"topics"`
	Zookeepers         []string      `json:"zookeepers"`
	ZkPath             string        `json:"zk_path"`
	Serializer         string        `json:"serializer"`
	ContentType        string        `json:"content_type"`
	ConnectionPoolSize int           `json:"connection_pool_size"`
}

type ServiceConfig struct {
	LogFile   string               `json:"log_file"`
	Callbacks []CallbackItemConfig `json:"consumer_groups"`
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

	for i, _ := range c.Callbacks {
		callback := &c.Callbacks[i]
		callback.Timeout, err = time.ParseDuration(callback.TimeoutStr)
		if nil != err {
			glog.Errorf("callback config timeout error(%s),using default.config value:%s", err.Error(), callback.TimeoutStr)

			callback.Timeout = CFG_DEFAULT_TIMEOUT
			callback.TimeoutStr = fmt.Sprintf("%dms", CFG_DEFAULT_TIMEOUT/time.Millisecond)
		}

		callback.FailedSleep, err = time.ParseDuration(callback.FailedSleepStr)
		if nil != err {
			glog.Errorf("callback config failed_sleep error(%s),using default.config value:%s", err.Error(), callback.FailedSleepStr)

			callback.FailedSleep = CFG_DEFAULT_FAILED_SLEEP
			callback.FailedSleepStr = fmt.Sprintf("%dms", CFG_DEFAULT_FAILED_SLEEP/time.Millisecond)

		}

		if callback.FailedSleep < CFG_MIN_FAILED_SLEEP {
			glog.Errorf("callback config failed_sleep too small,using min.config value:%s,%s", callback.FailedSleep, callback.FailedSleepStr)

			callback.FailedSleep = CFG_MIN_FAILED_SLEEP
			callback.FailedSleepStr = fmt.Sprintf("%dms", CFG_MIN_FAILED_SLEEP/time.Millisecond)

		}

		if callback.ConnectionPoolSize <= 0 {
			callback.ConnectionPoolSize = http.DefaultMaxIdleConnsPerHost
		}

	}

	return c, nil
}
