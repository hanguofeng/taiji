package main

import (
	"sync"

	"errors"
	"github.com/cihub/seelog"
)

type OffsetMap map[string]map[int32]int64

type OffsetManager struct {
	config             *OffsetMangerConfig
	offsetStorage      OffsetStorage
	slaveOffsetStorage []OffsetStorage
	l                  sync.RWMutex
	offsets            OffsetMap
	manager            *CallbackManager
}

func NewOffsetManager() *OffsetManager {
	return &OffsetManager{}
}

func (om *OffsetManager) Init(config *OffsetMangerConfig, manager *CallbackManager) {
	// init storage/slaveStorage
	om.config = config
	om.manager = manager
	if om.config.StorageName == "zookeeper" {
		om.offsetStorage = NewZookeeperOffsetStorage(nil)
		for i := 0; i < len(om.config.SlaveStorage); i++ {
			om.slaveOffsetStorage[i] = NewZookeeperOffsetStorage(nil)
		}
	}
	om.offsetStorage.Init(om.config.StorageConfig, om.manager)
	i := 0
	for key, slave := range om.config.SlaveStorage {
		om.slaveOffsetStorage[i].Init(slave, om.manager)
		i++
	}
}

func (om *OffsetManager) InitializePartition(topic string, partition int32) (int64, error) {
	// call storage/slaveStorage InitializePartition
	// call storage ReadOffset
	for i := 0; i < len(om.slaveOffsetStorage); i++ {
		_, _ = om.slaveOffsetStorage[i].InitializePartition(topic, partition)
	}
	return om.offsetStorage.InitializePartition(topic, partition)
}

func (om *OffsetManager) FinalizePartition(topic string, partition int32) error {
	for i := 0; i < len(om.slaveOffsetStorage); i++ {
		_ = om.slaveOffsetStorage[i].FinalizePartition(topic, partition)
	}
	// call storage/slaveStorage FinalizePartition
	return om.offsetStorage.FinalizePartition(topic, partition)
}

func (om *OffsetManager) CommitOffset(topic string, partition int32, offset int64) error {
	// call storage/slaveStorage WriteOffset
	for i := 0; i < len(om.slaveOffsetStorage); i++ {
		_ = om.slaveOffsetStorage[i].WriteOffset(topic, partition, offset)
	}
	return om.offsetStorage.WriteOffset(topic, partition, offset)
}

func (om *OffsetManager) Close() error {
	// call storage close
	for i := 0; i < len(om.slaveOffsetStorage); i++ {
		om.slaveOffsetStorage[i].Close()
	}
	om.offsetStorage.Close()
	return nil
}
