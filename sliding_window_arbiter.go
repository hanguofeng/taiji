package main

import (
	"errors"
	"reflect"

	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
)

const CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE = "window_size"

var (
	ERROR_SLIDING_WINDOW_ARBITER_NO_CONFIG         = errors.New("ArbiterConfig is required for SlidingWindowArbiter")
	ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_EXISTS = errors.New("ArbiterConfig.window_size is required for SlidingWindowArbiter")
	ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_VALID  = errors.New("ArbiterConfig.window_size is not greater than zero")
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
}

func NewSlidingWindowArbiter() Arbiter {
	return &SlidingWindowArbiter{
		StartStopControl: &StartStopControl{},
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
		seelog.Critical(ERROR_SLIDING_WINDOW_ARBITER_NO_CONFIG.Error())
		return ERROR_SLIDING_WINDOW_ARBITER_NO_CONFIG
	}

	if arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE] == nil {
		seelog.Critical(ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_EXISTS.Error())
		return ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_EXISTS
	}

	windowSizeValue := reflect.ValueOf(arbiterConfig[CONFIG_SLIDING_WINDOW_ARBITER_WINDOW_SIZE])

	switch windowSizeValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		swa.windowSize = int(windowSizeValue.Int())
	default:
		seelog.Critical(ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_VALID.Error())
		return ERROR_SLIDING_WINDOW_ARBITER_CONFIG_NOT_VALID
	}

	return nil
}

func (swa *SlidingWindowArbiter) Run() error {
	if err := swa.ensureStart(); err != nil {
		return err
	}
	defer swa.markStop()
	swa.initReady()

	consumer := swa.manager.GetConsumer()

	swa.messages = make(chan *sarama.ConsumerMessage, swa.windowSize*2)
	swa.offsets = make(chan int64, swa.windowSize*2)
	trigger := make(chan struct{}, swa.windowSize*2)
	offsetBase := int64(-1)
	offsetWindow := make([]bool, swa.windowSize)

	// cold start
	for i := 0; i != swa.windowSize; i++ {
		trigger <- struct{}{}
	}
	swa.markReady()

arbiterLoop:
	for {
		select {
		case <-swa.WaitForCloseChannel():
			seelog.Debugf("Stop event triggered [url:%s]", swa.config.Url)
			break arbiterLoop
		case err := <-consumer.Errors():
			seelog.Errorf("Read error from PartitionConsumer [topic:%s][partition:%d][url:%s][err:%s]",
				err.Topic, err.Partition, swa.config.Url, err.Err.Error())
		case offset := <-swa.offsets:
			seelog.Debugf("Read offset from Transporter [topic:%s][partition:%d][url:%s][offset:%d]",
				swa.manager.Topic, swa.manager.Partition, swa.config.Url, offset)
			if offset >= 0 {
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
					swa.manager.GetCallbackManager().GetOffsetManager().CommitOffset(
						swa.manager.Topic, swa.manager.Partition, offsetBase+int64(advanceCount)-1)

					// rebase to idx + offsetBase
					// TODO, use ring buffer instead of array slicing
					offsetBase += int64(advanceCount)
					newOffsetWindow := make([]bool, swa.windowSize)
					copy(newOffsetWindow, offsetWindow[advanceCount:])
					offsetWindow = newOffsetWindow

					// send advance count trigger
					for i := 0; i != advanceCount; i++ {
						trigger <- struct{}{}
					}
				}
			}
		case <-trigger:
			select {
			case <-swa.WaitForCloseChannel():
				seelog.Debugf("Stop event triggered [url:%s]", swa.config.Url)
				break arbiterLoop
			case message := <-consumer.Messages():
				seelog.Debugf("Read message from PartitionConsumer [topic:%s][partition:%d][url:%s][offset:%d]",
					message.Topic, message.Partition, swa.config.Url, message.Offset)
				// feed message to transporter
				select {
				case <-swa.WaitForCloseChannel():
					seelog.Debugf("Stop event triggered [url:%s]", swa.config.Url)
					break arbiterLoop
				case swa.messages <- message:
				}

				// set base
				if -1 == offsetBase {
					offsetBase = message.Offset
				}
			}
		}
	}

	return nil
}

func init() {
	RegisterArbiter("SlidingWindow", NewSlidingWindowArbiter)
}
