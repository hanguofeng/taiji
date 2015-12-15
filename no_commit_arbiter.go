package main

import (
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

type NoCommitArbiter struct {
	*StartStopControl

	// input/output
	offsets  chan int64
	messages chan *sarama.ConsumerMessage

	// parent
	manager *PartitionManager

	// config
	config        *CallbackItemConfig
	arbiterConfig ArbiterConfig
}

func NewNoCommitArbiter() Arbiter {
	return &NoCommitArbiter{
		StartStopControl: &StartStopControl{},
	}
}

func (*NoCommitArbiter) PreferredWorkerNum(workerNum int) int {
	return workerNum
}

func (sa *NoCommitArbiter) OffsetChannel() chan<- int64 {
	return sa.offsets
}

func (sa *NoCommitArbiter) MessageChannel() <-chan *sarama.ConsumerMessage {
	return sa.manager.GetConsumer().Messages()
}

func (sa *NoCommitArbiter) Init(config *CallbackItemConfig, arbiterConfig ArbiterConfig, manager *PartitionManager) error {
	sa.manager = manager
	sa.config = config
	sa.arbiterConfig = arbiterConfig

	return nil
}

func (sa *NoCommitArbiter) Run() error {
	if err := sa.ensureStart(); err != nil {
		return err
	}
	defer sa.markStop()

	sa.offsets = make(chan int64, 256)
	sa.markReady()

arbiterLoop:
	for {
		select {
		case <-sa.WaitForCloseChannel():
			glog.V(1).Infof("Stop event triggered [url:%s]", sa.config.Url)
			break arbiterLoop
		case offset := <-sa.offsets:
			glog.V(1).Infof("Read offset from Transporter [topic:%s][partition:%d][url:%s][offset:%d]",
				sa.manager.Topic, sa.manager.Partition, sa.config.Url, offset)
		}
	}

	return nil
}

func init() {
	RegisterArbiter("NoCommit", NewNoCommitArbiter)
}
