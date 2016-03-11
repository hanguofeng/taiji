package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

type Arbiter interface {
	// input/output
	OffsetChannel() chan<- int64
	MessageChannel() <-chan *sarama.ConsumerMessage

	// stat
	GetStat() interface{}

	// start stop control
	Init(config *CallbackItemConfig, arbiterConfig ArbiterConfig, manager *PartitionManager) error
	Prepare()
	Run() error
	Ready() error
	WaitForExitChannel() <-chan struct{}
	Close() error

	// transporter worker num judgement
	PreferredTransporterWorkerNum(workerNum int) int
}

type ArbiterCreator func() Arbiter

var registeredArbiterMap = make(map[string]ArbiterCreator)

func RegisterArbiter(name string, factory ArbiterCreator) {
	registeredArbiterMap[strings.ToLower(name)] = factory
}

func NewArbiter(name string) (Arbiter, error) {
	name = strings.ToLower(name)
	if registeredArbiterMap[name] == nil {
		return nil, errors.New(fmt.Sprintf("Arbiter %s not exists", name))
	}

	return registeredArbiterMap[name](), nil
}
