package main

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

type SlidingWindowArbiterPartitionConsumerMock struct {
	stopper  chan struct{}
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

func (pcm *SlidingWindowArbiterPartitionConsumerMock) Init() {
	pcm.messages = make(chan *sarama.ConsumerMessage, 5)
	pcm.errors = make(chan *sarama.ConsumerError)

	go func() {
		i := int64(1)

	partitionConsumerMockLoop:
		for {
			newMsg := &sarama.ConsumerMessage{
				Topic:     "test_topic",
				Partition: 1,
				Offset:    i,
			}
			select {
			case pcm.messages <- newMsg:
				i++
			case <-pcm.stopper:
				break partitionConsumerMockLoop
			}
		}
	}()
}

func (pcm *SlidingWindowArbiterPartitionConsumerMock) Errors() <-chan *sarama.ConsumerError {
	return pcm.errors
}

func (pcm *SlidingWindowArbiterPartitionConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	return pcm.messages
}

func (pcm *SlidingWindowArbiterPartitionConsumerMock) Close() error {
	close(pcm.stopper)
	return nil
}

func (*SlidingWindowArbiterPartitionConsumerMock) AsyncClose() {
}

func (*SlidingWindowArbiterPartitionConsumerMock) HighWaterMarkOffset() int64 {
	return 0
}

func TestSlidingWindowArbiter(t *testing.T) {
	t.Log("Allocated new SlidingWindowArbiter")
	arbiter, err := NewArbiter("SlidingWindow")
	assert.Nil(t, err, "Create SlidingWindowArbiter failed")

	// config/callbackManager init
	callbackConfig := &CallbackItemConfig{
		Url: "http://invalid_url_that_should_never_deliver",
	}
	callbackConfig.OffsetConfig.StorageName = "null"
	callbackConfig.ArbiterConfig = make(ArbiterConfig)
	callbackConfig.ArbiterConfig["window_size"] = 10
	offsetManager := NewOffsetManager()
	callbackManager := &CallbackManager{
		offsetManager: offsetManager,
	}
	offsetManager.Init(callbackConfig.OffsetConfig, callbackManager)
	partitionConsumerMock := &SlidingWindowArbiterPartitionConsumerMock{
		stopper: make(chan struct{}),
	}
	partitionConsumerMock.Init()
	manager := &PartitionManager{
		partitionConsumer: &PartitionConsumer{
			kafkaPartitionConsumer: partitionConsumerMock,
		},
		manager: callbackManager,
	}

	// arbiter init
	t.Log("Initializing SlidingWindowArbiter")
	err = arbiter.Init(callbackConfig, callbackConfig.ArbiterConfig, manager)
	assert.Nil(t, err, "Init SlidingWindowArbiter failed")
	t.Log("Started SlidingWindowArbiter")
	go arbiter.Run()
	defer arbiter.Close()
	defer manager.GetKafkaPartitionConsumer().Close()
	if err := arbiter.Ready(); err != nil {
		t.Fatalf("Arbiter start failed [err:%s]", err)
	}
	messages := arbiter.MessageChannel()
	offsets := arbiter.OffsetChannel()
	assert.NotNil(t, messages, "Arbiter MessageChannel should not be nil")

	t.Logf("PartitionConsumerMock channel length [length:%d][capacity:%d]",
		len(manager.GetKafkaPartitionConsumer().Messages()), cap(manager.GetKafkaPartitionConsumer().Messages()))
	t.Logf("Arbiter message channel length and capacity [length:%d][capacity:%d]", len(messages), cap(messages))
	t.Logf("Current arbiter offsetBase [offsetBase:%d]", arbiter.(*SlidingWindowArbiter).offsetBase)
	t.Logf("Arbiter stats [stat:%v]", arbiter.GetStat())

	// test all nocommit case
	for i := 0; i != 10; i++ {
		select {
		case message := <-messages:
			t.Logf("Message received [offset:%d]", message.Offset)
		case <-time.After(time.Second):
			// force yield, wait for arbiter goroutine to process
			t.Fatalf("Ready message not received when window is not full")
		}
	}

	select {
	case <-messages:
		t.Fatalf("Ready message count exceeded window size")
	case <-time.After(time.Second):
		// force yield, wait for arbiter goroutine to process
	}

	// commit a non-base offset
	offsets <- 5

	select {
	case <-messages:
		t.Fatalf("Message offset exceeded window size + offsetBase")
		t.Logf("Current arbiter offsetBase [offsetBase:%d]", arbiter.(*SlidingWindowArbiter).offsetBase)
		t.Logf("Arbiter stats [stat:%v]", arbiter.GetStat())
	case <-time.After(time.Second):
		// force yield, wait for arbiter goroutine to process
	}

	// commit base offset
	offsets <- 1

	select {
	case message := <-messages:
		t.Logf("Message received [offset:%d]", message.Offset)
	case <-time.After(time.Second):
		// force yield, wait for arbiter goroutine to process
		t.Fatalf("No available message even offsetBase is received")
		t.Logf("Current arbiter offsetBase [offsetBase:%d]", arbiter.(*SlidingWindowArbiter).offsetBase)
		t.Logf("Arbiter stats [stat:%v]", arbiter.GetStat())
	}

	select {
	case <-messages:
		t.Fatalf("Message offset exceeded window size + offsetBase")
		t.Logf("Current arbiter offsetBase [offsetBase:%d]", arbiter.(*SlidingWindowArbiter).offsetBase)
		t.Logf("Arbiter stats [stat:%v]", arbiter.GetStat())
	case <-time.After(time.Second):
		// force yield, wait for arbiter goroutine to process
	}

	// commit several offset, including previous offset 5
	offsets <- 3
	offsets <- 4
	offsets <- 2

	for i := 0; i != 4; i++ {
		select {
		case message := <-messages:
			t.Logf("Message received [offset:%d]", message.Offset)
		case <-time.After(time.Second):
			// force yield, wait for arbiter goroutine to process
			t.Fatalf("Ready message not received when window is not full")
		}
	}

	t.Logf("PartitionConsumerMock channel length [length:%d][capacity:%d]",
		len(manager.GetKafkaPartitionConsumer().Messages()), cap(manager.GetKafkaPartitionConsumer().Messages()))
	t.Logf("Arbiter message channel length and capacity [length:%d][capacity:%d]", len(messages), cap(messages))
	t.Logf("Current arbiter offsetBase [offsetBase:%d]", arbiter.(*SlidingWindowArbiter).offsetBase)
	t.Logf("Arbiter stats [stat:%v]", arbiter.GetStat())

	t.Logf("SlidingWindowArbiter working normally")
}
