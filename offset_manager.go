package main

import (
	"errors"
	"sync"

	"github.com/golang/glog"
)

type OffsetMap map[string]map[int32]int64

type OffsetManager struct {
	*StartStopControl

	// config
	config OffsetMangerConfig

	// parent
	manager *CallbackManager

	// master storage
	offsetStorage OffsetStorage

	// slave storage
	slaveOffsetStorage map[string]OffsetStorage

	// offsetStorage runner
	offsetStorageRunner *ServiceRunner

	// lastCommitted offset
	lastCommitted OffsetMap
	l             sync.RWMutex
}

func NewOffsetManager() *OffsetManager {
	return &OffsetManager{
		StartStopControl: &StartStopControl{},
	}
}

func (om *OffsetManager) Init(config OffsetMangerConfig, manager *CallbackManager) error {
	// init storage/slaveStorage
	om.config = config
	om.manager = manager
	om.lastCommitted = make(OffsetMap)

	var err error

	if om.offsetStorage, err = NewOffsetStorage(om.config.StorageName); err != nil {
		return err
	}

	if err = om.offsetStorage.Init(om.config.StorageConfig, om.manager); err != nil {
		return err
	}

	om.slaveOffsetStorage = make(map[string]OffsetStorage)

	for storageName, storageConfig := range om.config.SlaveStorage {
		var storage OffsetStorage
		if storage, err = NewOffsetStorage(storageName); err != nil {
			return err
		}
		if err = storage.Init(storageConfig, om.manager); err != nil {
			return err
		}
		om.slaveOffsetStorage[storageName] = storage
	}

	om.offsetStorageRunner = NewServiceRunner()

	return nil
}

func (om *OffsetManager) InitializePartition(topic string, partition int32) (int64, error) {
	om.l.Lock()
	defer om.l.Unlock()

	for name, storage := range om.slaveOffsetStorage {
		go func(name string, storage OffsetStorage) {
			if _, err := storage.InitializePartition(topic, partition); err != nil {
				glog.Warningf("Initialize slaveOffsetStorage failed [topic:%s][partition:%d][storageName:%s]",
					topic, partition, name)
			}
		}(name, storage)
	}

	offset, err := om.offsetStorage.InitializePartition(topic, partition)

	if err == nil {
		if om.lastCommitted[topic] == nil {
			om.lastCommitted[topic] = make(map[int32]int64)
		}

		om.lastCommitted[topic][partition] = -1
	}

	return offset, err
}

func (om *OffsetManager) FinalizePartition(topic string, partition int32) error {
	om.l.Lock()
	defer om.l.Unlock()
	defer func() {
		if om.lastCommitted[topic] != nil {
			if _, exists := om.lastCommitted[topic][partition]; !exists {
				delete(om.lastCommitted[topic], partition)
			}
		}
	}()

	for name, storage := range om.slaveOffsetStorage {
		go func(name string, storage OffsetStorage) {
			if err := storage.FinalizePartition(topic, partition); err != nil {
				glog.Warningf("Finalize slaveOffsetStorage failed [topic:%s][partition:%d][storageName:%s]",
					topic, partition, name)
			}
		}(name, storage)
	}

	return om.offsetStorage.FinalizePartition(topic, partition)
}

func (om *OffsetManager) GetOffsets() OffsetMap {
	om.l.RLock()
	defer om.l.RUnlock()

	result := make(OffsetMap)

	for topic, partitionOffsets := range om.lastCommitted {
		resultOffsets := make(map[int32]int64)

		for partition, offset := range partitionOffsets {
			resultOffsets[partition] = offset
		}

		result[topic] = resultOffsets
	}

	return result
}

func (om *OffsetManager) CommitOffset(topic string, partition int32, offset int64) error {
	om.l.Lock()
	defer om.l.Unlock()

	if om.lastCommitted[topic] == nil {
		om.lastCommitted[topic] = make(map[int32]int64)
	}

	if om.lastCommitted[topic][partition] > offset {
		glog.Errorf("Invalid offset committed [topic:%s][partition:%d][lastCommitted:%d][current:%d]",
			topic, partition, om.lastCommitted[topic][partition], offset)
		return errors.New("Invalid offset to commit")
	}

	for name, storage := range om.slaveOffsetStorage {
		go func(name string, storage OffsetStorage) {
			if err := storage.WriteOffset(topic, partition, offset+1); err != nil {
				glog.Warningf("CommitOffset to slaveOffsetStorage failed [topic:%s][partition:%d][offset:%d][storageName:%s]",
					topic, partition, offset, name)
			}
		}(name, storage)
	}

	if err := om.offsetStorage.WriteOffset(topic, partition, offset+1); err == nil {
		// update offset to lastCommitted
		om.lastCommitted[topic][partition] = offset
	} else {
		return err
	}

	return nil
}

func (om *OffsetManager) Run() error {
	if err := om.ensureStart(); err != nil {
		return err
	}
	defer om.markStop()

	// start all offsetStorage run
	offsetStorage := make([]Runnable, 0, len(om.slaveOffsetStorage)+1)

	offsetStorage = append(offsetStorage, om.offsetStorage)

	for _, storage := range om.slaveOffsetStorage {
		offsetStorage = append(offsetStorage, storage)
	}

	om.offsetStorageRunner.Prepare()
	if _, err := om.offsetStorageRunner.RunAsync(offsetStorage); err != nil {
		return err
	}
	defer om.offsetStorageRunner.Close()

	select {
	case <-om.offsetStorageRunner.WaitForExitChannel():
	case <-om.WaitForCloseChannel():
	}

	return nil
}
