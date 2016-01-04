package main

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	TEST_CONFIG_FILE_CONTENT = `{
    "consumer_groups": [
        {
            "worker_num" : 8,
            "url": "http://localhost:8080/foo",
            "retry_times": 4,
            "timeout": "3s",
            "bypass_failed":true,
            "failed_sleep":"12s",
            "topics": [
                "yanl"
            ],
            "zookeepers": [
                "127.0.0.1:2181"
            ],
            "zk_path": "/kafka",
            "serializer": "raw",
            "content_type": "json",
			"log_collect_ratio": 100,
			"offset": {
				"storage_name": "zookeeper",
				"storage_config": {
					"commit_interval": "10s"
				},
				"slave_storage_config": {
					"file": {
						"file_name": "test_offset_file"
					}
				}
			},
			"arbiter_name": "sequential",
			"arbiter_config": {},
			"transporter_name": "http",
			"transporter_config": {},
			"initial_from_oldest": false,
			"processing_timeout": "50s"
        }
    ],
    "log_file": "test.log",
    "connection_pool_size": 10,
    "stat_server_port": 8080
}`

	TEST_CONFIG_FILE_CONTENT_EX1 = `{
    "consumer_groups": [
        {
            "worker_num" : 8,
            "url": "http://localhost:8080/foo",
            "retry_times": 4,
            "timeout": "abcd",
            "bypass_failed":true,
            "failed_sleep":"1ms",
            "topics": [
                "yanl"
            ],
            "zookeepers": [
                "127.0.0.1:2181"
            ],
            "zk_path": "/kafka",
            "serializer": "raw",
            "content_type": "json",
			"log_collect_ratio": -1,
			"offset": {
				"storage_name": "",
				"storage_config": {
					"commit_interval": "10s"
				},
				"slave_storage_config": {
					"file": {
						"file_name": "test_offset_file"
					}
				}
			},
			"arbiter_name": "sequential",
			"arbiter_config": {},
			"transporter_name": "http",
			"transporter_config": {},
			"initial_from_oldest": false,
			"processing_timeout": "5s"
        }
    ],
    "log_file": "test.log",
    "connection_pool_size": -5,
    "stat_server_port": 500
}`

	TEST_CONFIG_FILE_CONTENT_EX2 = `{
    "consumer_groups": [
        {
            "worker_num" : 8,
            "url": "http://localhost:8080/foo",
            "retry_times": 4,
            "timeout": "abcd",
            "bypass_failed":true,
            "failed_sleep":"abcd",
            "topics": [
                "yanl"
            ],
            "zookeepers": [
                "127.0.0.1:2181"
            ],
            "zk_path": "/kafka",
            "serializer": "raw",
            "content_type": "json",
			"log_collect_ratio": -1,
			"offset": {
				"storage_name": "",
				"storage_config": {
					"commit_interval": "10s"
				},
				"slave_storage_config": {
					"file": {
						"file_name": "test_offset_file"
					}
				}
			},
			"arbiter_name": "sequential",
			"arbiter_config": {},
			"transporter_name": "http",
			"transporter_config": {},
			"initial_from_oldest": false,
			"processing_timeout": "abcd"
        }
    ],
    "log_file": "test.log",
    "connection_pool_size": -5,
    "stat_server_port": 500
}`
)

func TestLoadConfigFile(t *testing.T) {
	file, err := ioutil.TempFile("", "config_file_")
	assert.Nil(t, err, "Create tempfile failed, failed to test")

	fileName := file.Name()
	file.WriteString(TEST_CONFIG_FILE_CONTENT)
	file.Close()

	defer os.Remove(fileName)

	config, err := LoadConfigFile(fileName)
	assert.Nil(t, err, "Parse config file failed error[%v]", err)
	t.Logf("Config file parse result: %#v", config)

	// check config parse result
	assert.Equal(t, 10, config.ConnectionPoolSize, "ConnectionPoolSize not correct")
	assert.Equal(t, 8080, config.StatServerPort, "StatServerPort not correct")
	assert.Len(t, config.Callbacks, 1, "CallbackItemConfig count not correct")
	assert.Equal(t, 8, config.Callbacks[0].WorkerNum, "WorkerNum not correct")
	assert.Equal(t, "http://localhost:8080/foo", config.Callbacks[0].Url, "Url not correct")
	assert.Equal(t, 4, config.Callbacks[0].RetryTimes, "RetryTimes not correct")
	assert.Equal(t, 3*time.Second, config.Callbacks[0].Timeout, "Timeout not correct")
	assert.Equal(t, true, config.Callbacks[0].BypassFailed, "BypassFailed not correct")
	assert.Equal(t, 12*time.Second, config.Callbacks[0].FailedSleep, "FailedSleep not correct")
	assert.Equal(t, []string{"yanl"}, config.Callbacks[0].Topics, "Topics not correct")
	assert.Equal(t, []string{"127.0.0.1:2181"}, config.Callbacks[0].Zookeepers, "Zookeeper not correct")
	assert.Equal(t, "/kafka", config.Callbacks[0].ZkPath, "ZkPath not correct")
	assert.Equal(t, "raw", config.Callbacks[0].Serializer, "Serializer not correct")
	assert.Equal(t, "json", config.Callbacks[0].ContentType, "ContentType not correct")
	assert.Equal(t, "zookeeper", config.Callbacks[0].OffsetConfig.StorageName, "OffsetConfig.StorageName not correct")
	assert.NotEmpty(t, config.Callbacks[0].OffsetConfig.StorageConfig, "OffsetStorage.StorageConfig is empty")
	assert.Equal(t, "10s", config.Callbacks[0].OffsetConfig.StorageConfig["commit_interval"],
		"OffsetStorage.StorageConfig.commit_interval is not correct")
	assert.NotEmpty(t, config.Callbacks[0].OffsetConfig.SlaveStorage, "OffsetStorage.StorageConfig.SlaveStorage is empty")
	assert.NotEmpty(t, config.Callbacks[0].OffsetConfig.SlaveStorage["file"],
		"OffsetStorage.StorageConfig.SlaveStorage.file is empty")
	assert.Equal(t, "test_offset_file", config.Callbacks[0].OffsetConfig.SlaveStorage["file"]["file_name"],
		"OffsetStorage.StorageConfig.SlaveStorage.file.file_name is not correct")
	assert.Equal(t, "sequential", config.Callbacks[0].ArbiterName, "ArbiterName is not correct")
	assert.Empty(t, config.Callbacks[0].ArbiterConfig, "ArbiterConfig is not empty")
	assert.Equal(t, "http", config.Callbacks[0].TransporterName, "TransporterName is not correct")
	assert.Empty(t, config.Callbacks[0].TransporterConfig, "TransporterConfig is not empty")
	assert.False(t, config.Callbacks[0].InitialFromOldest, "InitialFromOldest is not false")
	assert.Equal(t, 50*time.Second, config.Callbacks[0].ProcessingTimeout, "ProcessingTimeout is not correct")
}

func TestLoadConfigFileEx(t *testing.T) {
	file, err := ioutil.TempFile("", "config_file_")
	assert.Nil(t, err, "Create tempfile failed, failed to test")

	fileName := file.Name()
	file.WriteString(TEST_CONFIG_FILE_CONTENT_EX1)
	file.Close()

	defer os.Remove(fileName)

	config, err := LoadConfigFile(fileName)
	assert.Nil(t, err, "Parse config file failed error[%v]", err)
	t.Logf("Config file parse result: %#v", config)

	// check config parse result
	assert.Equal(t, CFG_DEFAULT_HTTP_CONNECTION_POOL_SIZE, config.ConnectionPoolSize, "ConnectionPoolSize not fallback to default")
	assert.Equal(t, 0, config.StatServerPort, "StatServerPort not fallback to zero")
	assert.Equal(t, CFG_DEFAULT_TIMEOUT, config.Callbacks[0].Timeout, "Timeout not fallback to default")
	assert.Equal(t, CFG_DEFAULT_FAILED_SLEEP, config.Callbacks[0].FailedSleep, "FailedSleep not fallback to default")
	assert.Equal(t, CFG_DEFAULT_PROCESSING_TIMEOUT, config.Callbacks[0].ProcessingTimeout,
		"ProcessingTimeout not fallback to default")
	assert.Equal(t, CFG_DEFAULT_MASTER_OFFSET_STORAGE, config.Callbacks[0].OffsetConfig.StorageName,
		"OffsetConfig.StorageName not fallback to default")

	file, err = ioutil.TempFile("", "config_file_")
	assert.Nil(t, err, "Create tempfile failed, failed to test")

	fileName2 := file.Name()
	file.WriteString(TEST_CONFIG_FILE_CONTENT_EX2)
	file.Close()

	defer os.Remove(fileName2)

	config, err = LoadConfigFile(fileName2)
	assert.Nil(t, err, "Parse config file failed error[%v]", err)
	t.Logf("Config file parse result: %#v", config)

	assert.Equal(t, CFG_DEFAULT_FAILED_SLEEP, config.Callbacks[0].FailedSleep, "FailedSleep not fallback to default")
	assert.Equal(t, CFG_DEFAULT_PROCESSING_TIMEOUT, config.Callbacks[0].ProcessingTimeout,
		"ProcessingTimeout not fallback to default")
}

func TestLoadConfigFileFailed(t *testing.T) {
	// open file failed
	var err error
	_, err = LoadConfigFile("/proc/a_file_could_never_exists")
	assert.NotNil(t, err, "This config file is impossible to be loaded")
	t.Logf("Parse config file failed [%v]", err)

	// json could not parsed
	file, err := ioutil.TempFile("", "config_file_")
	assert.Nil(t, err, "Create tempfile failed, failed to test")

	fileName := file.Name()
	file.WriteString("{") // incorrect json
	file.Close()

	defer os.Remove(fileName)

	_, err = LoadConfigFile(fileName)
	assert.NotNil(t, err, "This config file is impossible to be parsed")
	t.Logf("Parse config file failed [%v]", err)

	// url is empty
	file, err = ioutil.TempFile("", "config_file_")
	assert.Nil(t, err, "Create tempfile failed, failed to test")
	fileName2 := file.Name()
	file.WriteString(`{"consumer_groups": [{"url": ""}]}`)
	file.Close()

	defer os.Remove(fileName2)

	_, err = LoadConfigFile(fileName2)
	assert.NotNil(t, err, "This config file contains empty url, impossible to load")
	t.Logf("Parse config file failed [%v]", err)

	// topics
	file, err = ioutil.TempFile("", "config_file_")
	assert.Nil(t, err, "Create tempfile failed, failed to test")
	fileName3 := file.Name()
	file.WriteString(`{"consumer_groups": [{"url": "a", "topics": []}]}`)
	file.Close()

	defer os.Remove(fileName3)

	_, err = LoadConfigFile(fileName3)
	assert.NotNil(t, err, "This config file contains no topics, impossible to load")
	t.Logf("Parse config file failed [%v]", err)

	// zookeepers
	file, err = ioutil.TempFile("", "config_file_")
	assert.Nil(t, err, "Create tempfile failed, failed to test")
	fileName4 := file.Name()
	file.WriteString(`{"consumer_groups": [{"url": "a", "topics": ["a"], "zookeepers": []}]}`)
	file.Close()

	defer os.Remove(fileName4)

	_, err = LoadConfigFile(fileName4)
	assert.NotNil(t, err, "This config file contains no zookeeprs, impossible to load")
	t.Logf("Parse config file failed [%v]", err)
}
