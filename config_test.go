package main

import (
	"io/ioutil"
	"os"
	"testing"
)

const (
	TEST_CONFIG_FILE_CONTENT = `{
    "consumer_groups": [
        {
            "worker_num" : 8,
            "url": "http://localhost:8080/foo",
            "retry_times": 4,
            "bypass_failed":true,
            "failed_sleep":"12s",
            "timeout": "3s",
            "topics": [
                "yanl"
            ],
            "zookeepers": [
                "127.0.0.1:2181"
            ],
            "zk_path": "/kafka",
            "serializer": "raw",
            "content_type": "json",
            "connection_pool_size": 1,
			"log_collect_ratio": 100,
			"transporter_config": {
				"batch_size": 10
			}
        }
    ]
}`
)

func TestReadConfigFile(t *testing.T) {
	file, err := ioutil.TempFile("", "config_file_")

	if err != nil {
		t.Error("Create tempfile failed, failed to test")
	}

	fileName := file.Name()
	file.WriteString(TEST_CONFIG_FILE_CONTENT)
	file.Close()

	defer os.Remove(fileName)

	config, err := LoadConfigFile(fileName)

	if nil == config || nil != err {
		t.Errorf("Parse config file failed with config[%v] error[%s]", config, err.Error())
	}

	t.Logf("Config file parse result: %#v", config)
}
