package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
)

var (
	configFile string
	callback   string
	testMode   bool
)

func getGroupName(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}

func init() {
	flag.StringVar(&configFile, "c", "config.json", "config.json file to upgrade")
	flag.StringVar(&callback, "l", "", "callback url to merge")
	flag.BoolVar(&testMode, "t", false, "test callback merge result, do not really merge")
	zk.DefaultLogger = log.New(ioutil.Discard, "[Zookeeper] ", log.LstdFlags)
}

func main() {
	flag.Parse()

	if callback == "" {
		panic("to merge callback url is required")
	}

	configJsonObject, err := os.Open(configFile)
	if err != nil {
		panic(fmt.Sprintf("could not open config.json file: %v", err))
	}

	configJsonBytes, err := ioutil.ReadAll(configJsonObject)
	if err != nil {
		panic(fmt.Sprintf("could not read config.json file: %v", err))
	}
	configJsonObject.Close()

	configJson := make(map[string]interface{})
	err = json.Unmarshal(configJsonBytes, &configJson)
	if err != nil {
		panic(fmt.Sprintf("could not parse config.json file: %v", err))
	}

	// get to merge url in callback config
	if configJson["consumer_groups"] == nil {
		panic("consumer_groups is not provided in config.json")
	}
	callbackList := configJson["consumer_groups"].([]interface{})
	if len(callbackList) == 0 {
		panic("consumer_groups list is empty, no callback is avaiable for merge")
	}

	// filter to merge callback in consumerGroupsList
	newConsumerGroups := make([]map[string]interface{}, 0)
	newCallback := make(map[string]interface{})
	oldCallbackTopics := make(map[string][]string)
	newTopicsSet := make(map[string]bool)
	newTopics := make([]string, 0)
	needUpdate := false

	for _, rawCallbackItem := range callbackList {
		callbackItem := rawCallbackItem.(map[string]interface{})
		oldCallback := callbackItem["url"].(string)

		if strings.HasPrefix(oldCallback, callback) {
			// should merge
			if !needUpdate {
				for key, value := range callbackItem {
					newCallback[key] = value
				}

				newCallback["url"] = callback
			}

			needUpdate = true

			topics := callbackItem["topics"].([]interface{})

			callbackTopics := make([]string, 0)
			for _, topic := range topics {
				topicStr := topic.(string)
				newTopicsSet[topicStr] = true
				callbackTopics = append(callbackTopics, topicStr)
			}

			oldCallbackTopics[oldCallback] = callbackTopics
		} else {
			// directly copy
			newConsumerGroups = append(newConsumerGroups, callbackItem)
		}
	}

	if !needUpdate {
		panic("find no matched callbacks in consumer_group")
	}

	for topic, _ := range newTopicsSet {
		newTopics = append(newTopics, topic)
	}
	newCallback["topics"] = newTopics
	newConsumerGroups = append(newConsumerGroups, newCallback)
	configJson["consumer_groups"] = newConsumerGroups

	// serialize
	configJsonBytes, err = json.MarshalIndent(configJson, "", "    ")
	if err != nil {
		panic(fmt.Sprintf("serialize config.json file content failed: %v", err))
	}

	// print
	if testMode {
		fmt.Printf("%s\n\n", configJsonBytes)
	} else {
		// update data in zookeeper
		rawZookeepersList := newCallback["zookeepers"].([]interface{})
		zkPath := newCallback["zk_path"].(string)

		zookeepersList := make([]string, 0)
		for _, zk := range rawZookeepersList {
			zookeepersList = append(zookeepersList, zk.(string))
		}

		// connect zookeeper
		kazooConfig := kazoo.NewConfig()
		kazooConfig.Chroot = zkPath
		kazooInstance, err := kazoo.NewKazoo(zookeepersList, kazooConfig)
		if err != nil {
			panic(fmt.Sprintf("connect to zookeeper failed: %v", err))
		}
		defer kazooInstance.Close()

		// create new directory
		newCallbackConsumerGroup := kazooInstance.Consumergroup(getGroupName(callback))
		if err := newCallbackConsumerGroup.Create(); err != nil {
			panic(fmt.Sprintf("create new callback node in zookeeper failed: %v", err))
		}

		// get all old offsets
		for oldCallback, topics := range oldCallbackTopics {
			oldCallbackConsumerGroup := kazooInstance.Consumergroup(getGroupName(oldCallback))
			offsets, err := oldCallbackConsumerGroup.FetchAllOffsets()
			if err != nil {
				// get old offset failed
				panic(fmt.Sprintf("get old callback offsets failed: %s, %v", oldCallback, err))
			}

			// commit to new group
			for _, topic := range topics {
				if offsets[topic] != nil {
					for partition, offset := range offsets[topic] {
						if err := newCallbackConsumerGroup.CommitOffset(topic, partition, offset); err != nil {
							panic(fmt.Sprintf("write offset to zookeeper failed: %s, %d, %d, %v", topic, partition, offset, err))
						}
					}
				}
			}
		}

		err = ioutil.WriteFile(configFile, configJsonBytes, 0600)
		if err != nil {
			panic(fmt.Sprintf("update config.json file failed: %v", err))
		}
	}
}
