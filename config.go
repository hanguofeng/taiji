package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"errors"

	"github.com/cihub/seelog"
)

const (
	CFG_DEFAULT_TIMEOUT               = time.Second
	CFG_DEFAULT_FAILED_SLEEP          = time.Second
	CFG_MIN_FAILED_SLEEP              = time.Second
	CFG_DEFAULT_PROCESSING_TIMEOUT    = 10 * time.Second
	CFG_MIN_PROCESSING_TIMEOUT        = 10 * time.Second
	CFG_MIN_STAT_SERVER_PORT          = 8000
	CFG_MAX_STAT_SERVER_PORT          = 10000
	CFG_DEFAULT_MASTER_OFFSET_STORAGE = "zookeeper"
	CFG_DEFAULT_ARBITER               = "sequential"
	CFG_DEFAULT_TRANSPORTER           = "http"
	DEFAULT_LOG_COLLECT_RATIO         = 20
)

type MapConfig map[string]interface{}
type ArbiterConfig MapConfig
type OffsetStorageConfig MapConfig
type TransporterConfig MapConfig

type OffsetMangerConfig struct {
	StorageName   string                         `json:"storage_name"`
	StorageConfig OffsetStorageConfig            `json:"storage_config"`
	SlaveStorage  map[string]OffsetStorageConfig `json:"slave_storage_config"`
}

type CallbackItemConfig struct {
	WorkerNum            int                `json:"worker_num"`
	Url                  string             `json:"url"`
	RetryTimes           int                `json:"retry_times"`
	TimeoutStr           string             `json:"timeout"`
	Timeout              time.Duration      `json:"null,omitempty"`
	BypassFailed         bool               `json:"bypass_failed"`
	FailedSleepStr       string             `json:"failed_sleep"`
	FailedSleep          time.Duration      `json:"null,omitempty"`
	Topics               []string           `json:"topics"`
	Zookeepers           []string           `json:"zookeepers"`
	ZkPath               string             `json:"zk_path"`
	Serializer           string             `json:"serializer"`
	ContentType          string             `json:"content_type"`
	LogCollectRatio      int                `json:"log_collect_ratio"`
	OffsetConfig         OffsetMangerConfig `json:"offset"`
	ArbiterName          string             `json:"arbiter_name"`
	ArbiterConfig        ArbiterConfig      `json:"arbiter_config"`
	TransporterName      string             `json:"transporter_name"`
	TransporterConfig    TransporterConfig  `json:"transporter_config"`
	InitialFromOldest    bool               `json:"initial_from_oldest"`
	ProcessingTimeout    time.Duration      `json:"null,omitempty"`
	ProcessingTimeoutStr string             `json:"processing_timeout"`
}

type ServiceConfig struct {
	LogFile            string               `json:"log_file"`
	Callbacks          []CallbackItemConfig `json:"consumer_groups"`
	ConnectionPoolSize int                  `json:"connection_pool_size"`
	StatServerPort     int                  `json:"stat_server_port"`
}

func LoadConfigFile(configFile string) (*ServiceConfig, error) {
	var c *ServiceConfig
	path := configFile
	fi, err := os.Open(path)
	defer fi.Close()
	if err != nil {
		return nil, err
	}

	fd, err := ioutil.ReadAll(fi)
	err = json.Unmarshal([]byte(fd), &c)
	if err != nil {
		return nil, err
	}

	if c.ConnectionPoolSize <= 0 {
		c.ConnectionPoolSize = http.DefaultMaxIdleConnsPerHost
	}

	if c.StatServerPort != 0 && (c.StatServerPort < CFG_MIN_STAT_SERVER_PORT || c.StatServerPort >= CFG_MAX_STAT_SERVER_PORT) {
		seelog.Warnf("server config stat_server_port should be greater than %d and lower than %d",
			CFG_MIN_STAT_SERVER_PORT, CFG_MAX_STAT_SERVER_PORT)
		c.StatServerPort = 0
	}

	for i, _ := range c.Callbacks {
		callback := &c.Callbacks[i]

		if "" == callback.Url {
			seelog.Errorf("callback config url should not be empty")
			return nil, errors.New("callback url is empty")
		}

		if 0 == len(callback.Topics) {
			seelog.Errorf("callback config topics should not be empty")
			return nil, errors.New("callback config topics is empty")
		}

		if 0 == len(callback.Zookeepers) {
			seelog.Errorf("callback config zookeepers should not be empty")
			return nil, errors.New("callback config zookeepers is empty")
		}

		callback.Timeout, err = time.ParseDuration(callback.TimeoutStr)
		if err != nil {
			seelog.Warnf("callback config timeout error(%s),using default.config value:%s",
				err.Error(), callback.TimeoutStr)

			callback.Timeout = CFG_DEFAULT_TIMEOUT
			callback.TimeoutStr = fmt.Sprintf("%dms", CFG_DEFAULT_TIMEOUT/time.Millisecond)
		}

		callback.FailedSleep, err = time.ParseDuration(callback.FailedSleepStr)
		if err != nil {
			seelog.Warnf("callback config failed_sleep error(%s),using default.config value:%s",
				err.Error(), callback.FailedSleepStr)

			callback.FailedSleep = CFG_DEFAULT_FAILED_SLEEP
			callback.FailedSleepStr = fmt.Sprintf("%dms", CFG_DEFAULT_FAILED_SLEEP/time.Millisecond)
		}

		if callback.FailedSleep < CFG_MIN_FAILED_SLEEP {
			seelog.Warnf("callback config failed_sleep too small,using min.config value:%s,%s",
				callback.FailedSleep, callback.FailedSleepStr)

			callback.FailedSleep = CFG_MIN_FAILED_SLEEP
			callback.FailedSleepStr = fmt.Sprintf("%dms", CFG_MIN_FAILED_SLEEP/time.Millisecond)
		}

		callback.ProcessingTimeout, err = time.ParseDuration(callback.ProcessingTimeoutStr)
		if err != nil {
			seelog.Warnf("callback config processing_timeout error(%s),using default.config value:%s",
				err.Error(), callback.ProcessingTimeoutStr)

			callback.ProcessingTimeout = CFG_DEFAULT_PROCESSING_TIMEOUT
		}

		if callback.ProcessingTimeout < CFG_MIN_PROCESSING_TIMEOUT {
			seelog.Warnf("callback config processing_timeout too small,using min.config value:%s,%s",
				callback.ProcessingTimeout, callback.ProcessingTimeoutStr)

			callback.ProcessingTimeout = CFG_MIN_PROCESSING_TIMEOUT
			callback.ProcessingTimeoutStr = fmt.Sprintf("%dms", CFG_MIN_PROCESSING_TIMEOUT/time.Millisecond)
		}

		if callback.OffsetConfig.StorageName == "" {
			callback.OffsetConfig.StorageName = CFG_DEFAULT_MASTER_OFFSET_STORAGE
		}

		if callback.ArbiterName == "" {
			callback.ArbiterName = CFG_DEFAULT_ARBITER
		}

		if callback.TransporterName == "" {
			callback.TransporterName = CFG_DEFAULT_TRANSPORTER
		}

		if callback.LogCollectRatio <= 0 {
			callback.LogCollectRatio = DEFAULT_LOG_COLLECT_RATIO
		}

	}

	return c, nil
}
