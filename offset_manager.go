package main

import (
	"errors"
	"sync"
)

type OffsetMap map[string]map[int32]int64

type OffsetManager struct {
	config             *OffsetManagerConfig
	offsetStorage      OffsetStorage
	slaveOffsetStorage []OffsetStorage
	l                  sync.RWMutex
	offsets            OffsetMap
	manager            *CallbackManager
}

func NewOffsetManager() *OffsetManager {
	return nil
}

func (*OffsetManager) Init(config *OffsetManagerConfig, manager *CallbackManager) {
	// init storage/slaveStorage
}

func (*OffsetManager) InitializePartition(topic string, partition int32) (int64, error) {
	// call storage/slaveStorage InitializePartition
	// call storage ReadOffset
	return -1, errors.New("Not Implemented")
}

func (*OffsetManager) FinalizePartition(topic string, partition int32) error {
	// call storage/slaveStorage FinalizePartition
	return errors.New("Not Implemented")
}

func (*OffsetManager) CommitOffset(topic string, partition int32, offset int64) {
	// call storage/slaveStorage WriteOffset
}

func (*OffsetManager) Close() {
	// call storage close
}
