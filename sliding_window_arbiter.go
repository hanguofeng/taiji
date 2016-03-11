package main

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

const CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE = "window_size"

var (
	ERROR_SLIDING_WINDOW_ARBITER_NO_CONFIG         = errors.New("ArbiterConfig is required for SlidingWindowArbiter")
	ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_EXISTS = errors.New("ArbiterConfig.window_size is required for SlidingWindowArbiter")
	ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_VALID  = errors.New("ArbiterConfig.window_size is invalid")
)

type SlidingWindowArbiter struct {
	*StartStopControl

	// input/output
	offsets  chan int64
	messages chan *sarama.ConsumerMessage

	// parent
	manager *PartitionManager

	// config
	config        *CallbackItemConfig
	arbiterConfig ArbiterConfig
	windowSize    int

	// sliding window context
	offsetBase   int64
	offsetWindow []bool

	// stat variables
	inflight  uint64
	uncommit  uint64
	processed uint64
	startTime time.Time
}

func NewSlidingWindowArbiter() Arbiter {
	return &SlidingWindowArbiter{
		StartStopControl: NewStartStopControl(),
	}
}

func (swa *SlidingWindowArbiter) PreferredTransporterWorkerNum(workerNum int) int {
	if workerNum > swa.windowSize || workerNum <= 0 {
		return swa.windowSize
	} else {
		return workerNum
	}
}

func (swa *SlidingWindowArbiter) OffsetChannel() chan<- int64 {
	return swa.offsets
}

func (swa *SlidingWindowArbiter) MessageChannel() <-chan *sarama.ConsumerMessage {
	return swa.messages
}

func (swa *SlidingWindowArbiter) Init(config *CallbackItemConfig, arbiterConfig ArbiterConfig, manager *PartitionManager) error {
	swa.manager = manager
	swa.config = config
	swa.arbiterConfig = arbiterConfig

	if arbiterConfig == nil {
		glog.Fatal(ERROR_SLIDING_WINDOW_ARBITER_NO_CONFIG.Error())
		return ERROR_SLIDING_WINDOW_ARBITER_NO_CONFIG
	}

	if arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE] == nil {
		glog.Fatal(ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_EXISTS.Error())
		return ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_EXISTS
	}

	switch arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE].(type) {
	case float32:
		swa.windowSize = int(arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE].(float32))
	case float64:
		swa.windowSize = int(arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE].(float64))
	case int:
		swa.windowSize = int(arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE].(int))
	case int32:
		swa.windowSize = int(arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE].(int32))
	case int64:
		swa.windowSize = int(arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE].(int64))
	case uint32:
		swa.windowSize = int(arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE].(uint32))
	case uint64:
		swa.windowSize = int(arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE].(uint64))
	default:
		return ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_VALID
	}

	if swa.windowSize <= 0 {
		return ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_VALID
	} else if swa.windowSize == 1 {
		glog.Warningf("ArbiterConfig.window_size is 1, prefer using SequentialArbiter instead")
	}

	return nil
}

func (swa *SlidingWindowArbiter) Run() error {
	if err := swa.ensureStart(); err != nil {
		return err
	}
	defer swa.markStop()

	consumer := swa.manager.GetKafkaPartitionConsumer()

	swa.messages = make(chan *sarama.ConsumerMessage, swa.windowSize*2)
	swa.offsets = make(chan int64, swa.windowSize*2)
	trigger := make(chan struct{}, swa.windowSize*2)
	swa.offsetBase = int64(-1)
	swa.offsetWindow = make([]bool, swa.windowSize)

	// reset stat variables
	atomic.StoreUint64(&swa.inflight, 0)
	atomic.StoreUint64(&swa.uncommit, 0)
	atomic.StoreUint64(&swa.processed, 0)
	swa.startTime = time.Now().Local()

	// cold start
	for i := 0; i != swa.windowSize; i++ {
		trigger <- struct{}{}
	}
	swa.markReady()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
	arbiterOffsetLoop:
		for {
			select {
			case <-swa.WaitForCloseChannel():
				glog.V(1).Infof("Stop event triggered [url:%s]", swa.config.Url)
				break arbiterOffsetLoop
			case offset := <-swa.offsets:
				glog.V(1).Infof("Read offset from Transporter [topic:%s][partition:%d][url:%s][offset:%d]",
					swa.manager.Topic, swa.manager.Partition, swa.config.Url, offset)
				if offset >= 0 {
					swa.offsetWindow[offset-swa.offsetBase] = true

					// decrement inflight, increment uncommit
					atomic.AddUint64(&swa.inflight, ^uint64(0))
					atomic.AddUint64(&swa.uncommit, 1)

					if offset == swa.offsetBase {
						// rebase
						advanceCount := 0

						for advanceCount = 0; advanceCount != swa.windowSize; advanceCount++ {
							if !swa.offsetWindow[advanceCount] {
								break
							}
						}

						// trigger offset commit
						swa.manager.GetCallbackManager().GetOffsetManager().CommitOffset(
							swa.manager.Topic, swa.manager.Partition, swa.offsetBase+int64(advanceCount)-1)

						// rebase to idx + offsetBase
						// TODO, use ring buffer instead of array slicing
						swa.offsetBase += int64(advanceCount)
						newOffsetWindow := make([]bool, swa.windowSize)
						copy(newOffsetWindow, swa.offsetWindow[advanceCount:])
						swa.offsetWindow = newOffsetWindow

						// decrement uncommit
						atomic.AddUint64(&swa.uncommit, ^uint64(advanceCount-1))

						glog.V(1).Infof("Fast-forward offsetBase [topic:%s][partition:%d][url:%s][originOffsetBase:%d][offsetBase:%d][advance:%d]",
							swa.manager.Topic, swa.manager.Partition, swa.config.Url, swa.offsetBase-int64(advanceCount),
							swa.offsetBase, advanceCount)

						// send advance count trigger
						for i := 0; i != advanceCount; i++ {
							trigger <- struct{}{}
						}
					}

					glog.V(1).Infof("Current sliding window status [topic:%s][partition:%d][url:%s][offsetBase:%d][inflight:%d][uncommit:%d]",
						swa.manager.Topic, swa.manager.Partition, swa.config.Url, swa.offsetBase,
						atomic.LoadUint64(&swa.inflight),
						atomic.LoadUint64(&swa.uncommit))
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		offsetBaseInit := sync.Once{}

	arbiterMessageLoop:
		for {
			select {
			case <-swa.WaitForCloseChannel():
				glog.V(1).Infof("Stop event triggered [url:%s]", swa.config.Url)
				break arbiterMessageLoop
			case <-trigger:
				select {
				case <-swa.WaitForCloseChannel():
					glog.V(1).Infof("Stop event triggered [url:%s]", swa.config.Url)
					break arbiterMessageLoop
				case message := <-consumer.Messages():
					glog.V(1).Infof("Read message from PartitionConsumer [topic:%s][partition:%d][url:%s][offset:%d]",
						message.Topic, message.Partition, swa.config.Url, message.Offset)

					// set base
					offsetBaseInit.Do(func() {
						if -1 == swa.offsetBase {
							swa.offsetBase = message.Offset
						}
					})

					// feed message to transporter
					select {
					case <-swa.WaitForCloseChannel():
						glog.V(1).Infof("Stop event triggered [url:%s]", swa.config.Url)
						break arbiterMessageLoop
					case swa.messages <- message:
					}

					// increment counter
					atomic.AddUint64(&swa.inflight, 1)
					atomic.AddUint64(&swa.processed, 1)
				}
			}
		}
	}()

	wg.Wait()

	return nil
}

func (swa *SlidingWindowArbiter) GetStat() interface{} {
	result := make(map[string]interface{})
	result["inflight"] = atomic.LoadUint64(&swa.inflight)
	result["uncommit"] = atomic.LoadUint64(&swa.uncommit)
	result["processed"] = atomic.LoadUint64(&swa.processed)
	result["window_base"] = swa.offsetBase
	result["window_size"] = len(swa.offsetWindow)
	result["start_time"] = swa.startTime
	return result
}

func init() {
	RegisterArbiter("SlidingWindow", NewSlidingWindowArbiter)
	RegisterArbiter("sliding_window", NewSlidingWindowArbiter)
	RegisterArbiter("sliding", NewSlidingWindowArbiter)
}
