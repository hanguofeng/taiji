package main

import (
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

type SequentialArbiter struct {
	*StartStopControl

	// input/output
	offsets  chan int64
	messages chan *sarama.ConsumerMessage

	// parent
	manager *PartitionManager

	// config
	config        *CallbackItemConfig
	arbiterConfig ArbiterConfig

	// stat variables
	processed uint64
	startTime time.Time
}

func NewSequentialArbiter() Arbiter {
	return &SequentialArbiter{
		StartStopControl: NewStartStopControl(),
	}
}

func (*SequentialArbiter) PreferredTransporterWorkerNum(workerNum int) int {
	return 1
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

	return nil
}

func (sa *SequentialArbiter) Run() error {
	if err := sa.ensureStart(); err != nil {
		return err
	}
	defer sa.markStop()

	// reset stat variables
	atomic.StoreUint64(&sa.processed, 0)
	sa.startTime = time.Now().Local()

	consumer := sa.manager.GetKafkaPartitionConsumer()

	// buffer only one message
	sa.offsets = make(chan int64)
	sa.messages = make(chan *sarama.ConsumerMessage)

	// cold start trigger
	go func() {
		sa.offsets <- int64(-1)
	}()
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
			if offset >= 0 {
				// trigger offset commit
				sa.manager.GetCallbackManager().GetOffsetManager().CommitOffset(
					sa.manager.Topic, sa.manager.Partition, offset)
			}
			select {
			case <-sa.WaitForCloseChannel():
				glog.V(1).Infof("Stop event triggered [url:%s]", sa.config.Url)
				break arbiterLoop
			case message := <-consumer.Messages():
				glog.V(1).Infof("Read message from PartitionConsumer [topic:%s][partition:%d][url:%s][offset:%d]",
					message.Topic, message.Partition, sa.config.Url, message.Offset)
				// feed message to transporter
				select {
				case <-sa.WaitForCloseChannel():
					glog.V(1).Infof("Stop event triggered [url:%s]", sa.config.Url)
					break arbiterLoop
				case sa.messages <- message:
				}

				atomic.AddUint64(&sa.processed, 1)
			}
		}
	}

	return nil
}

func (sa *SequentialArbiter) GetStat() interface{} {
	result := make(map[string]interface{})
	result["processed"] = atomic.LoadUint64(&sa.processed)
	result["start_time"] = sa.startTime
	return result
}

func init() {
	RegisterArbiter("Sequential", NewSequentialArbiter)
	RegisterArbiter("seq", NewSequentialArbiter)
}
