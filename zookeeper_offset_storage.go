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

func (zos *ZookeeperOffsetStorage) Init(config OffsetStorageConfig, manager *CallbackManager) error {
	zos.config = config
	zos.manager = manager
	zos.commitInterval = MIN_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL

	// read commitInterval from config
	if config != nil && config[CONFIG_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL] != nil {
		reflectValue := reflect.ValueOf(config[CONFIG_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL])
		if reflectValue.Kind() == reflect.String {
			commitIntervalStr := reflectValue.String()
			var err error
			if zos.commitInterval, err = time.ParseDuration(commitIntervalStr); err == nil {
				if zos.commitInterval < MIN_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL {
					return ERROR_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL_CONFIG_NOT_VALID
				}
			} else {
				return ERROR_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL_CONFIG_NOT_VALID
			}
		} else {
			return ERROR_ZOOKEEPER_OFFSET_STORAGE_COMMIT_INTERVAL_CONFIG_NOT_VALID
		}
	}

	zos.offsets = make(OffsetMap)
	zos.lastCommittedOffsets = make(OffsetMap)

	return nil
}

func (zos *ZookeeperOffsetStorage) InitializePartition(topic string, partition int32) (int64, error) {
	zos.l.Lock()
	defer zos.l.Unlock()

	if zos.offsets[topic] == nil {
		zos.offsets[topic] = make(map[int32]int64)
	}

	nextOffset, err := zos.manager.GetKazooGroup().FetchOffset(topic, partition)
	if err != nil {
		return 0, err
	}
	zos.offsets[topic][partition] = nextOffset

	return nextOffset, nil
}

func (zos *ZookeeperOffsetStorage) FinalizePartition(topic string, partition int32) error {
	zos.l.Lock()
	defer zos.l.Unlock()
	delete(zos.offsets[topic], partition)
	return nil
}

func (zos *ZookeeperOffsetStorage) ReadOffset(topic string, partition int32) (int64, error) {
	zos.l.RLock()
	defer zos.l.RUnlock()

	if zos.offsets[topic] == nil {
		return 0, errors.New("Invalid topic")
	}

	return zos.offsets[topic][partition], nil
}

func (zos *ZookeeperOffsetStorage) WriteOffset(topic string, partition int32, offset int64) error {
	zos.l.Lock()
	defer zos.l.Unlock()
	zos.offsets[topic][partition] = offset

	return nil
}

func (zos *ZookeeperOffsetStorage) Run() error {
	if err := zos.ensureStart(); err != nil {
		return err
	}
	defer zos.markStop()

	// run ticker
	commitTicker := time.NewTicker(zos.commitInterval)
	defer commitTicker.Stop()

tickerLoop:
	for {
		select {
		case <-zos.WaitForCloseChannel():
			break tickerLoop
		case <-commitTicker.C:
			// ticker update
			zos.l.Lock()
			for topic, topicOffsets := range zos.offsets {
				if zos.lastCommittedOffsets[topic] == nil {
					zos.lastCommittedOffsets[topic] = make(map[int32]int64)
				}

				for partition, offset := range topicOffsets {
					if err := zos.manager.GetKazooGroup().CommitOffset(topic, partition, offset); err != nil {
						glog.Warningf("Failed to commit offset [topic:%s][partition:%d][offset:%d]", topic, partition, offset)
					} else {
						zos.lastCommittedOffsets[topic][partition] = offset
					}
				}
			}
			zos.lastCommittedTime = time.Now()
			zos.l.Unlock()
		}
	}

	zos.l.Lock()
	for topic, _ := range zos.offsets {
		if zos.offsets[topic] != nil && len(zos.offsets[topic]) > 0 {
			glog.Warningf("Not all offsets were committed before shutdown was completed")
			delete(zos.offsets, topic)
		}
	}
	zos.l.Unlock()

	return nil
}

func (zos *ZookeeperOffsetStorage) GetStat() interface{} {
	zos.l.RLock()
	defer zos.l.RUnlock()

	result := make(map[string]interface{})
	result["offsets"] = stringKeyOffsetMap(zos.offsets)
	result["committed_offsets"] = stringKeyOffsetMap(zos.lastCommittedOffsets)
	result["committed_time"] = zos.lastCommittedTime.Local()

	return result
}

func init() {
	RegisterOffsetStorage("Zookeeper", NewZookeeperOffsetStorage)
}
