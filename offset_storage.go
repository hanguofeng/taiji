package main

import (
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
	"sync"
	"time"
)

type OffsetStorage interface {
	Init(config *OffsetStorageConfig, manager *CallbackManager)
	InitializePartition(topic string, partition int32) (int64, error)
	ReadOffset(topic string, partition int32) error
	WriteOffset(topic string, partition int32, offset int64) error
	FinalizePartition(topic string, partition int32) error
	Close()
}

type ZookeeperOffsetStorage struct {
	config               *OffsetStorageConfig
	commitInterval       time.Duration
	offsets              OffsetMap
	lastCommittedOffsets OffsetMap
	lastCommittedTime    time.Time
	// dirty map[string]map[int32]bool
	dirty      bool
	l          sync.RWMutex
	manager    *CallbackManager
	kazooGroup *kazoo.Consumergroup
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
