package main

import (
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
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
	transporter       Transporter
	transporterRunner *ConcurrencyRunner
}

func NewPartitionManager() *PartitionManager {
	return &PartitionManager{
		StartStopControl: NewStartStopControl(),
	}
}

func (pm *PartitionManager) Init(config *CallbackItemConfig, topic string, partition int32, manager *CallbackManager) error {
	pm.Topic = topic
	pm.Partition = partition

	// parent
	pm.manager = manager

	// config
	pm.config = config

	// init partitionConsumer
	pm.partitionConsumer = NewPartitionConsumer()
	pm.partitionConsumer.Init(config, topic, partition, pm)

	// init arbiter
	var err error
	if pm.arbiter, err = NewArbiter(config.ArbiterName); err != nil {
		return err
	}
	if err = pm.arbiter.Init(config, config.ArbiterConfig, pm); err != nil {
		return err
	}

	// init transporter
	pm.transporterRunner = NewConcurrencyRunner()
	if pm.transporter, err = NewTransporter(pm.config.TransporterName); err != nil {
		return err
	}
	if err = pm.transporter.Init(config, config.TransporterConfig, pm); err != nil {
		return err
	}

	return nil
}

func (pm *PartitionManager) Run() error {
	if err := pm.ensureStart(); err != nil {
		return err
	}

	defer pm.markStop()

	// start partition consumer
	pm.partitionConsumer.Prepare()
	go pm.partitionConsumer.Run()
	defer pm.partitionConsumer.Close()
	if err := pm.partitionConsumer.Ready(); err != nil {
		glog.Errorf("Partition consumer start failed [err:%s]", err)
		return err
	}

	// start arbiter
	pm.arbiter.Prepare()
	go pm.arbiter.Run()
	defer pm.arbiter.Close()
	if err := pm.arbiter.Ready(); err != nil {
		glog.Errorf("Arbiter start failed [err:%s]", err)
		return err
	}

	// start transporter group
	// TODO, dynamic create worker using this.config.WorkerNum and arbiter judgement
	workerNum := pm.arbiter.PreferredTransporterWorkerNum(pm.config.WorkerNum)
	pm.transporterRunner.RetryTimes = workerNum * 3
	pm.transporterRunner.Concurrency = workerNum
	pm.transporterRunner.Prepare()
	pm.transporter.ResetStat()
	if _, err := pm.transporterRunner.RunAsync(pm.transporter); err != nil {
		glog.Errorf("Transporter start failed [err:%v]", err)
		return err
	}
	defer pm.transporterRunner.Close()

	select {
	case <-pm.partitionConsumer.WaitForExitChannel():
	case <-pm.arbiter.WaitForExitChannel():
	case <-pm.transporterRunner.WaitForExitChannel():
	case <-pm.WaitForCloseChannel():
	}

	return nil
}

func (pm *PartitionManager) GetKafkaPartitionConsumer() sarama.PartitionConsumer {
	return pm.partitionConsumer.GetKafkaPartitionConsumer()
}

func (pm *PartitionManager) GetPartitionConsumer() *PartitionConsumer {
	return pm.partitionConsumer
}

func (pm *PartitionManager) GetArbiter() Arbiter {
	return pm.arbiter
}

func (pm *PartitionManager) GetTransporter() Transporter {
	return pm.transporter
}

func (pm *PartitionManager) GetCallbackManager() *CallbackManager {
	return pm.manager
}
