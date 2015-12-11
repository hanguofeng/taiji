package main

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type OffsetStorage interface {
	Init(config OffsetStorageConfig, manager *CallbackManager)
	InitializePartition(topic string, partition int32) (int64, error)
	ReadOffset(topic string, partition int32) (int64, error)
	WriteOffset(topic string, partition int32, offset int64) error
	FinalizePartition(topic string, partition int32) error
	Close() error
}

type ZookeeperOffsetStorage struct {
	config               OffsetStorageConfig
	manageConfig         *ZookeeperOffsetManagerConfig
	commitInterval       time.Duration
	offsets              OffsetMap
	lastCommittedOffsets OffsetMap
	lastCommittedTime    time.Time
	// dirty map[string]map[int32]bool
	dirty   bool
	l       sync.RWMutex
	manager *CallbackManager
	// kazooGroup *kazoo.Consumergroup
}

// OffsetManagerConfig holds configuration setting son how the offset manager should behave.
type ZookeeperOffsetManagerConfig struct {
	CommitInterval time.Duration // Interval between offset flushes to the backend store.
	VerboseLogging bool          // Whether to enable verbose logging.
}

// NewOffsetManagerConfig returns a new OffsetManagerConfig with sane defaults.
func NewOffsetManagerConfig() *ZookeeperOffsetManagerConfig {
	return &ZookeeperOffsetManagerConfig{
		CommitInterval: 10 * time.Second,
	}
}

// ZookeeperOffsetStorage returns an offset manager that uses Zookeeper
// to store offsets.
func NewZookeeperOffsetStorage(config *ZookeeperOffsetManagerConfig) OffsetStorage {
	if config == nil {
		config = NewOffsetManagerConfig()
	}

	zom := &ZookeeperOffsetStorage{
		manageConfig:         config,
		offsets:              make(OffsetMap),
		lastCommittedOffsets: make(OffsetMap),
	}

	return zom
}

func (zom *ZookeeperOffsetStorage) Init(config OffsetStorageConfig, manager *CallbackManager) {
	zom.config = config
	zom.manager = manager
}

func (zom *ZookeeperOffsetStorage) InitializePartition(topic string, partition int32) (int64, error) {
	zom.l.Lock()
	defer zom.l.Unlock()

	if zom.offsets[topic] == nil {
		zom.offsets[topic] = make(map[int32]int64)
	}

	nextOffset, err := zom.manager.kazooGroup.FetchOffset(topic, partition)
	if err != nil {
		return 0, err
	}

	return nextOffset, nil
}

func (zom *ZookeeperOffsetStorage) FinalizePartition(topic string, partition int32) error {
	zom.l.Lock()
	delete(zom.offsets[topic], partition)
	zom.l.Unlock()

	return nil
}

func (zom *ZookeeperOffsetStorage) ReadOffset(topic string, partition int32) (int64, error) {
	zom.l.RLock()
	defer zom.l.RUnlock()

	if zom.offsets[topic] == nil {
		return 0, errors.New("invalid topic")
	}

	return zom.offsets[topic][partition], nil
}

func (zom *ZookeeperOffsetStorage) WriteOffset(topic string, partition int32, offset int64) error {
	zom.l.RLock()
	defer zom.l.RUnlock()

	if offset >= 0 {
		if offset < zom.lastCommittedOffsets[topic][partition] {
			zom.dirty = true
		}
		if err := zom.manager.kazooGroup.CommitOffset(topic, partition, offset+1); err != nil {
			seelog.Infof("Failed to commit offset [topic:%s][partition:%d][offset:%d]", topic, partition, offset)
			return err
		}
	} else {
		seelog.Infof("Committed offset [topic:%s][partition:%d][offset:%d]", topic, partition, offset)
		return nil
	}
	return nil
}

func (zom *ZookeeperOffsetStorage) Close() error {
	zom.l.Lock()
	defer zom.l.Unlock()

	var closeError error
	for _, partitionOffsets := range zom.offsets {
		if len(partitionOffsets) > 0 {
			closeError = errors.New("Not all offsets were committed before shutdown was completed")
		}
	}

	return closeError
}

// 以下为本次开发的可选内容
type FileOffsetStorage struct {
	config   *OffsetStorageConfig
	filePath string
	offsets  OffsetMap
	l        sync.RWMutex
	manager  *CallbackManager
}

type SaramaOffsetManagerMap map[string]map[int32]*sarama.PartitionOffsetManager

type KafkaOffsetStorage struct {
	config                 *OffsetStorageConfig
	commitInterval         time.Duration
	manager                *CallbackManager
	offsetManager          *sarama.OffsetManager
	partitionOffsetManager SaramaOffsetManagerMap
}
