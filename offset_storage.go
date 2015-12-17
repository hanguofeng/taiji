package main

import (
	"errors"
	"fmt"
	"strings"
)

type OffsetStorage interface {
	// start stop control
	Init(config OffsetStorageConfig, manager *CallbackManager) error
	Run() error
	Close() error

	// offset management
	InitializePartition(topic string, partition int32) (int64, error)
	ReadOffset(topic string, partition int32) (int64, error)
	WriteOffset(topic string, partition int32, offset int64) error
	FinalizePartition(topic string, partition int32) error

	// stat
	GetStat() interface{}
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
