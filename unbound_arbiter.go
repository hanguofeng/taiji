package main

import (
	"sync"
	"sync/atomic"

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
}

func NewUnboundArbiter() Arbiter {
	return &UnboundArbiter{
		StartStopControl: &StartStopControl{},
	}
}

func (*UnboundArbiter) PreferredWorkerNum(workerNum int) int {
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

	consumer := ua.manager.GetConsumer()

	ua.messages = make(chan *sarama.ConsumerMessage, 256)
	ua.offsets = make(chan int64, 256)
	offsetBase := int64(-1)
	offsetWindow := make([]bool, 0, 10)

	// counters
	var inflight, uncommit uint64

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
					if offset-offsetBase >= int64(len(offsetWindow)) {
						glog.V(1).Infof("Extend offsetWindow [topic:%s][partition:%d][url:%s][offset:%d][offsetBase:%d][offsetWindowSize:%d]",
							ua.manager.Topic, ua.manager.Partition, ua.config.Url, offset, offsetBase, len(offsetWindow))
						// extend offsetWindow
						newOffsetWindow := make([]bool, (offset-offsetBase+1)*2)
						copy(newOffsetWindow, offsetWindow)
						offsetWindow = newOffsetWindow
					}

					// decrement inflight, increment uncommit
					atomic.AddUint64(&inflight, ^uint64(0))
					atomic.AddUint64(&uncommit, 1)

					offsetWindow[offset-offsetBase] = true
					if offset == offsetBase {
						// rebase
						advanceCount := 0

						for advanceCount = 0; advanceCount != len(offsetWindow); advanceCount++ {
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

						// decrement uncommit
						atomic.AddUint64(&uncommit, ^uint64(advanceCount-1))

						glog.V(1).Infof("Fast-forward offsetBase [topic:%s][partition:%d][url:%s][originOffsetBase:%d][offsetBase:%d][offsetWindowSize:%d][advance:%d]",
							ua.manager.Topic, ua.manager.Partition, ua.config.Url, offsetBase-int64(advanceCount),
							offsetBase, len(offsetWindow), advanceCount)
					}

					glog.V(1).Infof("Current unbound offset window status [topic:%s][partition:%d][url:%s][offsetBase:%d][inflight:%d][uncommit:%d]",
						ua.manager.Topic, ua.manager.Partition, ua.config.Url, offsetBase,
						atomic.LoadUint64(&inflight),
						atomic.LoadUint64(&uncommit))
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
				if -1 == offsetBase {
					offsetBase = message.Offset
				}

				// feed message to transporter
				select {
				case <-ua.WaitForCloseChannel():
					glog.V(1).Infof("Stop event triggered [url:%s]", ua.config.Url)
					break arbiterMessageLoop
				case ua.messages <- message:
				}

				// increment counter
				atomic.AddUint64(&inflight, 1)
			}
		}
	}()

	wg.Wait()

	// trigger transporter exit
	close(ua.messages)

	return nil
}

func init() {
	RegisterArbiter("Unbound", NewUnboundArbiter)
}
