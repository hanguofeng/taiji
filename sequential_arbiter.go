package main

import (
	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
)

type SequentialArbiter struct {
	offsets       chan int64
	messages      chan *sarama.ConsumerMessage
	manager       *PartitionManager
	config        *CallbackItemConfig
	arbiterConfig ArbiterConfig
	stopper       chan struct{}
}

func NewSequentialArbiter() Arbiter {
	return &SequentialArbiter{}
}

func (sa *SequentialArbiter) OffsetChannel() chan<- int64 {
	return sa.offsets
}

func (sa *SequentialArbiter) MessageChannel() <-chan *sarama.ConsumerMessage {
	return sa.messages
}

func (sa *SequentialArbiter) Init(config *CallbackItemConfig, arbiterConfig ArbiterConfig, manager *PartitionManager) error {
	sa.manager = manager
	sa.config = config
	sa.arbiterConfig = arbiterConfig
	sa.stopper = make(chan struct{})

	// buffer only one message
	sa.offsets = make(chan int64, 1)
	sa.messages = make(chan *sarama.ConsumerMessage, 1)

	return nil
}

func (sa *SequentialArbiter) Run() {
	consumer := sa.manager.GetConsumer()

	// cold start trigger
	sa.offsets <- int64(-1)

arbiterLoop:
	for {
		select {
		case <-sa.stopper:
			seelog.Debugf("Stop event triggered [url:%s]", sa.config.Url)
			break arbiterLoop
		case err := <-consumer.Errors():
			seelog.Errorf("Read error from PartitionConsumer [topic:%s][partition:%d][url:%s][err:%s]",
				err.Topic, err.Partition, sa.config.Url, err.Err.Error())
		case offset := <-sa.offsets:
			seelog.Debugf("Read offset from Transporter [topic:%s][partition:%d][url:%s][offset:%d]",
				sa.manager.Topic, sa.manager.Partition, sa.config.Url, offset)
			if offset >= 0 {
				// trigger offset commit
				sa.manager.GetCallbackManager().GetOffsetManager().CommitOffset(
					sa.manager.Topic, sa.manager.Partition, offset)
			}
			select {
			case <-sa.stopper:
				seelog.Debugf("Stop event triggered [url:%s]", sa.config.Url)
				break arbiterLoop
			case message := <-consumer.Messages():
				seelog.Debugf("Read message from PartitionConsumer [topic:%s][partition:%d][url:%s][offset:%d]",
					message.Topic, message.Partition, sa.config.Url, message.Offset)
				// feed message to transporter
				sa.messages <- message
			}
		}
	}
}

func (sa *SequentialArbiter) Close() {
	close(sa.stopper)
}
