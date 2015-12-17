package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Transporter interface {
	// start stop control
	Init(config *CallbackItemConfig, transporterConfig TransporterConfig, manager *PartitionManager) error
	Run() error
	Close() error

	// stat
	GetStat() interface{}
}

type WorkerCallback struct {
	Url          string
	RetryTimes   int
	Timeout      time.Duration
	BypassFailed bool
	FailedSleep  time.Duration
}

type TransporterCreator func() Transporter

var registeredTransporterMap = make(map[string]TransporterCreator)

func RegisterTransporter(name string, factory TransporterCreator) {
	registeredTransporterMap[strings.ToLower(name)] = factory
}

func NewTransporter(name string) (Transporter, error) {
	name = strings.ToLower(name)
	if registeredTransporterMap[name] == nil {
		return nil, errors.New(fmt.Sprintf("Transporter %s not exists", name))
	}

	return registeredTransporterMap[name](), nil
}
