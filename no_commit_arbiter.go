package main

import (
	"sync/atomic"
	"time"

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

	// stat variables
	processed uint64
	startTime time.Time
}

func NewNoCommitArbiter() Arbiter {
	return &NoCommitArbiter{
		StartStopControl: NewStartStopControl(),
	}
}

func (*NoCommitArbiter) PreferredTransporterWorkerNum(workerNum int) int {
	return workerNum
}

func (nca *NoCommitArbiter) OffsetChannel() chan<- int64 {
	return nca.offsets
}

func (nca *NoCommitArbiter) MessageChannel() <-chan *sarama.ConsumerMessage {
	return nca.manager.GetKafkaPartitionConsumer().Messages()
}

func (nca *NoCommitArbiter) Init(config *CallbackItemConfig, arbiterConfig ArbiterConfig, manager *PartitionManager) error {
	nca.manager = manager
	nca.config = config
	nca.arbiterConfig = arbiterConfig

	return nil
}

func (nca *NoCommitArbiter) Run() error {
	if err := nca.ensureStart(); err != nil {
		return err
	}
	defer nca.markStop()

	nca.offsets = make(chan int64, 256)

	// reset stat variables
	atomic.StoreUint64(&nca.processed, 0)
	nca.startTime = time.Now().Local()

	nca.markReady()

arbiterLoop:
	for {
		select {
		case <-nca.WaitForCloseChannel():
			glog.V(1).Infof("Stop event triggered [url:%s]", nca.config.Url)
			break arbiterLoop
		case offset := <-nca.offsets:
			glog.V(1).Infof("Read offset from Transporter [topic:%s][partition:%d][url:%s][offset:%d]",
				nca.manager.Topic, nca.manager.Partition, nca.config.Url, offset)
			atomic.AddUint64(&nca.processed, 1)
		}
	}

	return nil
}

func (nca *NoCommitArbiter) GetStat() interface{} {
	result := make(map[string]interface{})
	result["processed"] = atomic.LoadUint64(&nca.processed)
	result["start_time"] = nca.startTime
	return result
}

func init() {
	RegisterArbiter("NoCommit", NewNoCommitArbiter)
}
