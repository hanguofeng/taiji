package main

import (
	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
)

type PartitionManager struct {
	*StartStopControl
	Topic     string
	Partition int32

	// parent
	manager *CallbackManager

	// config
	config *CallbackItemConfig

	// partition consumer
	partitionConsumer *PartitionConsumer

	// arbiter
	arbiter Arbiter

	// transporter
	transporter       []Transporter
	transporterRunner *ServiceRunner
}

func NewPartitionManager() *PartitionManager {
	return &PartitionManager{
		StartStopControl: &StartStopControl{},
	}
}

func (this *PartitionManager) Init(config *CallbackItemConfig, topic string, partition int32, manager *CallbackManager) error {
	this.Topic = topic
	this.Partition = partition

	// parent
	this.manager = manager

	// config
	this.config = config

	// init partitionConsumer
	this.partitionConsumer = NewPartitionConsumer()
	this.partitionConsumer.Init(config, topic, partition, this)

	// init arbiter
	var err error
	if this.arbiter, err = NewArbiter(config.ArbiterName); err != nil {
		return err
	}
	if err = this.arbiter.Init(config, config.ArbiterConfig, this); err != nil {
		return err
	}

	// init transporter
	this.transporterRunner = NewServiceRunner()

	return nil
}

func (this *PartitionManager) Run() error {
	if err := this.ensureStart(); err != nil {
		return err
	}

	defer this.markStop()

	// start partition consumer
	go this.partitionConsumer.Run()
	defer this.partitionConsumer.Close()
	if err := this.partitionConsumer.Ready(); err != nil {
		seelog.Errorf("Partition consumer start failed [err:%s]", err)
		return err
	}

	// start arbiter
	go this.arbiter.Run()
	defer this.arbiter.Close()
	if err := this.arbiter.Ready(); err != nil {
		seelog.Errorf("Arbiter start failed [err:%s]", err)
		return err
	}

	// start transporter group
	this.transporter = make([]Transporter, 0)
	for i := 0; i != this.config.WorkerNum; i++ {
		if transporter, err := NewTransporter(this.config.TransporterName); err == nil {
			if err := transporter.Init(this.config, this.config.TransporterConfig, this); err != nil {
				return err
			}
			this.transporter = append(this.transporter, transporter)
		} else {
			return err
		}
	}
	this.transporterRunner.RetryTimes = len(this.transporter) * 3
	this.transporterRunner.RunAsync(this.transporter)
	defer this.transporterRunner.Close()

	select {
	case <-this.partitionConsumer.WaitForExitChannel():
	case <-this.arbiter.WaitForExitChannel():
	case <-this.transporterRunner.WaitForExitChannel():
	case <-this.WaitForCloseChannel():
	}

	return nil
}

func (this *PartitionManager) GetConsumer() sarama.PartitionConsumer {
	return this.partitionConsumer.GetConsumer()
}

func (this *PartitionManager) GetArbiter() Arbiter {
	return this.arbiter
}

func (this *PartitionManager) GetCallbackManager() *CallbackManager {
	return this.manager
}
