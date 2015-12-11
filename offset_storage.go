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
	Init(config OffsetStorageConfig, manager *CallbackManager) error
	InitializePartition(topic string, partition int32) (int64, error)
	ReadOffset(topic string, partition int32) (int64, error)
	WriteOffset(topic string, partition int32, offset int64) error
	FinalizePartition(topic string, partition int32) error
	Run() error
	Close() error
}

type OffsetStorageCreator func() OffsetStorage

var registeredOffsetStorageMap = make(map[string]OffsetStorageCreator)

func RegisterOffsetStorage(name string, factory OffsetStorageCreator) {
	registeredOffsetStorageMap[strings.ToLower(name)] = factory
}

func NewOffsetStorage(name string) (OffsetStorage, error) {
	name = strings.ToLower(name)
	if registeredOffsetStorageMap[name] == nil {
		return nil, errors.New(fmt.Sprintf("OffsetStorage %s not exists", name))
	}

	return registeredOffsetStorageMap[name](), nil
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
