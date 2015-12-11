package main

import (
	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
)

type UnboundArbiter struct {
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

func NewUnboundArbiter() Arbiter {
	return &UnboundArbiter{
		StartStopControl: &StartStopControl{},
	}
}

func (ua *UnboundArbiter) OffsetChannel() chan<- int64 {
	return ua.offsets
}

func (ua *UnboundArbiter) MessageChannel() <-chan *sarama.ConsumerMessage {
	return ua.messages
}

func (ua *UnboundArbiter) Init(config *CallbackItemConfig, arbiterConfig ArbiterConfig, manager *PartitionManager) error {
	ua.manager = manager
	ua.config = config
	ua.arbiterConfig = arbiterConfig

	return nil
}

func (ua *UnboundArbiter) Run() error {
	if err := ua.ensureStart(); err != nil {
		return err
	}
	defer ua.markStop()
	ua.initReady()

	consumer := ua.manager.GetConsumer()

	ua.messages = make(chan *sarama.ConsumerMessage)
	ua.offsets = make(chan int64)
	offsetBase := int64(-1)
	offsetWindow := make([]bool, 0, 10)

	ua.markReady()

arbiterLoop:
	for {
		select {
		case <-ua.WaitForCloseChannel():
			seelog.Debugf("Stop event triggered [url:%s]", ua.config.Url)
			break arbiterLoop
		case offset := <-ua.offsets:
			seelog.Debugf("Read offset from Transporter [topic:%s][partition:%d][url:%s][offset:%d]",
				ua.manager.Topic, ua.manager.Partition, ua.config.Url, offset)
			if offset >= 0 {
				if offset-offsetBase >= int64(len(offsetWindow)) {
					// extend offsetWindow
					newOffsetWindow := make([]bool, (offset-offsetBase)*2)
					copy(newOffsetWindow, offsetWindow)
					offsetWindow = newOffsetWindow
				}
				offsetWindow[offset-offsetBase] = true
				if offset == offsetBase {
					// rebase
					advanceCount := 0
					for advanceCount, _ = range offsetWindow {
						if !offsetWindow[advanceCount] {
							break
						}
					}

					// trigger offset commit
					ua.manager.GetCallbackManager().GetOffsetManager().CommitOffset(
						ua.manager.Topic, ua.manager.Partition, offsetBase+int64(advanceCount)-1)

					// rebase to idx + offsetBase
					// TODO, use ring buffer instead of array slicing
					offsetBase += int64(advanceCount)
					offsetWindow = offsetWindow[advanceCount:]
				}
			}
		case message := <-consumer.Messages():
			seelog.Debugf("Read message from PartitionConsumer [topic:%s][partition:%d][url:%s][offset:%d]",
				message.Topic, message.Partition, ua.config.Url, message.Offset)
			// feed message to transporter
			select {
			case <-ua.WaitForCloseChannel():
				seelog.Debugf("Stop event triggered [url:%s]", ua.config.Url)
				break arbiterLoop
			case ua.messages <- message:
			}

			// set base
			if -1 == offsetBase {
				offsetBase = message.Offset
			}
		}
	}

	return nil
}

func init() {
	RegisterArbiter("Unbound", NewUnboundArbiter)
}
