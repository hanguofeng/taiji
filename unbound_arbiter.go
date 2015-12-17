package main

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
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

	// unbound window context
	offsetBase   int64
	offsetWindow []bool

	// stat variables
	inflight  uint64
	uncommit  uint64
	processed uint64
	startTime time.Time
}

func NewUnboundArbiter() Arbiter {
	return &UnboundArbiter{
		StartStopControl: NewStartStopControl(),
	}
}

func (*UnboundArbiter) PreferredTransporterWorkerNum(workerNum int) int {
	return workerNum
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

	consumer := ua.manager.GetKafkaPartitionConsumer()

	ua.messages = make(chan *sarama.ConsumerMessage, 256)
	ua.offsets = make(chan int64, 256)
	ua.offsetBase = int64(-1)
	ua.offsetWindow = make([]bool, 0, 10)

	// reset stat variables
	atomic.StoreUint64(&ua.inflight, 0)
	atomic.StoreUint64(&ua.uncommit, 0)
	atomic.StoreUint64(&ua.processed, 0)
	ua.startTime = time.Now().Local()

	ua.markReady()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
	arbiterOffsetLoop:
		for {
			select {
			case <-ua.WaitForCloseChannel():
				glog.V(1).Infof("Stop event triggered [url:%s]", ua.config.Url)
				break arbiterOffsetLoop
			case offset := <-ua.offsets:
				glog.V(1).Infof("Read offset from Transporter [topic:%s][partition:%d][url:%s][offset:%d]",
					ua.manager.Topic, ua.manager.Partition, ua.config.Url, offset)
				if offset >= 0 {
					// extend uncommit offset window
					if offset-ua.offsetBase >= int64(len(ua.offsetWindow)) {
						glog.V(1).Infof("Extend offsetWindow [topic:%s][partition:%d][url:%s][offset:%d][offsetBase:%d][offsetWindowSize:%d]",
							ua.manager.Topic, ua.manager.Partition, ua.config.Url, offset, ua.offsetBase, len(ua.offsetWindow))
						// extend offsetWindow
						newOffsetWindow := make([]bool, (offset-ua.offsetBase+1)*2)
						copy(newOffsetWindow, ua.offsetWindow)
						ua.offsetWindow = newOffsetWindow
					}

					// decrement inflight, increment uncommit
					atomic.AddUint64(&ua.inflight, ^uint64(0))
					atomic.AddUint64(&ua.uncommit, 1)

					ua.offsetWindow[offset-ua.offsetBase] = true
					if offset == ua.offsetBase {
						// rebase
						advanceCount := 0

						for advanceCount = 0; advanceCount != len(ua.offsetWindow); advanceCount++ {
							if !ua.offsetWindow[advanceCount] {
								break
							}
						}

						// trigger offset commit
						ua.manager.GetCallbackManager().GetOffsetManager().CommitOffset(
							ua.manager.Topic, ua.manager.Partition, ua.offsetBase+int64(advanceCount)-1)

						// rebase to idx + offsetBase
						// TODO, use ring buffer instead of array slicing
						ua.offsetBase += int64(advanceCount)
						ua.offsetWindow = ua.offsetWindow[advanceCount:]

						// decrement uncommit
						atomic.AddUint64(&ua.uncommit, ^uint64(advanceCount-1))

						glog.V(1).Infof("Fast-forward offsetBase [topic:%s][partition:%d][url:%s][originOffsetBase:%d][offsetBase:%d][offsetWindowSize:%d][advance:%d]",
							ua.manager.Topic, ua.manager.Partition, ua.config.Url, ua.offsetBase-int64(advanceCount),
							ua.offsetBase, len(ua.offsetWindow), advanceCount)
					}

					glog.V(1).Infof("Current unbound offset window status [topic:%s][partition:%d][url:%s][offsetBase:%d][inflight:%d][uncommit:%d]",
						ua.manager.Topic, ua.manager.Partition, ua.config.Url, ua.offsetBase,
						atomic.LoadUint64(&ua.inflight),
						atomic.LoadUint64(&ua.uncommit))
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
	arbiterMessageLoop:
		for {
			select {
			case <-ua.WaitForCloseChannel():
				glog.V(1).Infof("Stop event triggered [url:%s]", ua.config.Url)
				break arbiterMessageLoop
			case message := <-consumer.Messages():
				glog.V(1).Infof("Read message from PartitionConsumer [topic:%s][partition:%d][url:%s][offset:%d]",
					message.Topic, message.Partition, ua.config.Url, message.Offset)

				// set base
				if -1 == ua.offsetBase {
					ua.offsetBase = message.Offset
				}

				// feed message to transporter
				select {
				case <-ua.WaitForCloseChannel():
					glog.V(1).Infof("Stop event triggered [url:%s]", ua.config.Url)
					break arbiterMessageLoop
				case ua.messages <- message:
				}

				// increment counter
				atomic.AddUint64(&ua.inflight, 1)
				atomic.AddUint64(&ua.processed, 1)
			}
		}
	}()

	wg.Wait()

	return nil
}

func (ua *UnboundArbiter) GetStat() interface{} {
	result := make(map[string]interface{})
	result["inflight"] = atomic.LoadUint64(&ua.inflight)
	result["uncommit"] = atomic.LoadUint64(&ua.uncommit)
	result["processed"] = atomic.LoadUint64(&ua.processed)
	result["window_base"] = ua.offsetBase
	result["window_size"] = len(ua.offsetWindow)
	result["start_time"] = ua.startTime
	return result
}

func init() {
	RegisterArbiter("Unbound", NewUnboundArbiter)
}
