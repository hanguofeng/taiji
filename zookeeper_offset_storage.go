package main

import (
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/golang/glog"
)

const CONFIG_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL = "commit_interval"
const MIN_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL = 10 * time.Second

var (
	ERROR_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL_CONFIG_NOT_VALID = errors.New(
		"OffsetStorageConfig.commit_interval is not valid (duration format and >10s)")
)

type ZookeeperOffsetStorage struct {
	*StartStopControl

	// config
	config         OffsetStorageConfig
	commitInterval time.Duration

	// offsets, current, committed
	offsets              OffsetMap
	lastCommittedOffsets OffsetMap
	lastCommittedTime    time.Time
	l                    sync.RWMutex

	// parent
	manager *CallbackManager
}

func NewZookeeperOffsetStorage() OffsetStorage {
	return &ZookeeperOffsetStorage{
		StartStopControl: NewStartStopControl(),
	}
}

func (zom *ZookeeperOffsetStorage) Init(config OffsetStorageConfig, manager *CallbackManager) error {
	zom.config = config
	zom.manager = manager
	zom.commitInterval = MIN_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL

	// read commitInterval from config
	if config != nil && config[CONFIG_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL] != nil {
		reflectValue := reflect.ValueOf(config[CONFIG_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL])
		if reflectValue.Kind() == reflect.String {
			commitIntervalStr := reflectValue.String()
			var err error
			if zom.commitInterval, err = time.ParseDuration(commitIntervalStr); err == nil {
				if zom.commitInterval < MIN_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL {
					return ERROR_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL_CONFIG_NOT_VALID
				}
			} else {
				return ERROR_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL_CONFIG_NOT_VALID
			}
		} else {
			return ERROR_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL_CONFIG_NOT_VALID
		}
	}

	zom.offsets = make(OffsetMap)
	zom.lastCommittedOffsets = make(OffsetMap)

	return nil
}

func (zom *ZookeeperOffsetStorage) InitializePartition(topic string, partition int32) (int64, error) {
	zom.l.Lock()
	defer zom.l.Unlock()

	if zom.offsets[topic] == nil {
		zom.offsets[topic] = make(map[int32]int64)
	}

	nextOffset, err := zom.manager.GetKazooGroup().FetchOffset(topic, partition)
	if err != nil {
		return 0, err
	}
	zom.offsets[topic][partition] = nextOffset

	return nextOffset, nil
}

func (zom *ZookeeperOffsetStorage) FinalizePartition(topic string, partition int32) error {
	zom.l.Lock()
	defer zom.l.Unlock()
	delete(zom.offsets[topic], partition)
	return nil
}

func (zom *ZookeeperOffsetStorage) ReadOffset(topic string, partition int32) (int64, error) {
	zom.l.RLock()
	defer zom.l.RUnlock()

	if zom.offsets[topic] == nil {
		return 0, errors.New("Invalid topic")
	}

	return zom.offsets[topic][partition], nil
}

func (zom *ZookeeperOffsetStorage) WriteOffset(topic string, partition int32, offset int64) error {
	zom.l.Lock()
	defer zom.l.Unlock()
	zom.offsets[topic][partition] = offset

	return nil
}

func (zom *ZookeeperOffsetStorage) Run() error {
	if err := zom.ensureStart(); err != nil {
		return err
	}
	defer zom.markStop()

	// run ticker
	commitTicker := time.NewTicker(zom.commitInterval)
	defer commitTicker.Stop()

tickerLoop:
	for {
		select {
		case <-zom.WaitForCloseChannel():
			break tickerLoop
		case <-commitTicker.C:
			// ticker update
			zom.l.Lock()
			for topic, topicOffsets := range zom.offsets {
				if zom.lastCommittedOffsets[topic] == nil {
					zom.lastCommittedOffsets[topic] = make(map[int32]int64)
				}

				for partition, offset := range topicOffsets {
					if err := zom.manager.GetKazooGroup().CommitOffset(topic, partition, offset); err != nil {
						glog.Warningf("Failed to commit offset [topic:%s][partition:%d][offset:%d]", topic, partition, offset)
					} else {
						zom.lastCommittedOffsets[topic][partition] = offset
					}
				}
			}
			zom.lastCommittedTime = time.Now()
			zom.l.Unlock()
		}
	}

	zom.l.Lock()
	for topic, _ := range zom.offsets {
		if zom.offsets[topic] != nil && len(zom.offsets[topic]) > 0 {
			glog.Warningf("Not all offsets were committed before shutdown was completed")
			delete(zom.offsets, topic)
		}
	}
	zom.l.Unlock()

	return nil
}

func (zom *ZookeeperOffsetStorage) GetStat() interface{} {
	zom.l.RLock()
	defer zom.l.RUnlock()

	result := make(map[string]interface{})
	result["offsets"] = stringKeyOffsetMap(zom.offsets)
	result["committed_offsets"] = stringKeyOffsetMap(zom.lastCommittedOffsets)
	result["committed_time"] = zom.lastCommittedTime.Local()

	return result
}

func init() {
	RegisterOffsetStorage("Zookeeper", NewZookeeperOffsetStorage)
}
