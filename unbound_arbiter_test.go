package main

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

type UnboundArbiterPartitionConsumerMock struct {
	stopper  chan struct{}
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

func (pcm *UnboundArbiterPartitionConsumerMock) Init() {
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

func (pcm *UnboundArbiterPartitionConsumerMock) Errors() <-chan *sarama.ConsumerError {
	return pcm.errors
}

func (pcm *UnboundArbiterPartitionConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	return pcm.messages
}

func (pcm *UnboundArbiterPartitionConsumerMock) Close() error {
	close(pcm.stopper)
	return nil
}

func (*UnboundArbiterPartitionConsumerMock) AsyncClose() {
}

func (*UnboundArbiterPartitionConsumerMock) HighWaterMarkOffset() int64 {
	return 0
}

func TestUnboundArbiter(t *testing.T) {
	t.Log("Allocated new UnboundArbiter")
	arbiter, err := NewArbiter("Unbound")
	assert.Nil(t, err, "Create UnboundArbiter failed")

	// config/callbackManager init
	callbackConfig := &CallbackItemConfig{
		Url: "http://invalid_url_that_should_never_deliver",
	}
	callbackConfig.OffsetConfig.StorageName = "null"
	offsetManager := NewOffsetManager()
	callbackManager := &CallbackManager{
		offsetManager: offsetManager,
	}
	offsetManager.Init(callbackConfig.OffsetConfig, callbackManager)
	partitionConsumerMock := &UnboundArbiterPartitionConsumerMock{
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
	t.Log("Initializing UnboundArbiter")
	err = arbiter.Init(callbackConfig, callbackConfig.ArbiterConfig, manager)
	assert.Nil(t, err, "Init UnboundArbiter failed")
	t.Log("Started UnboundArbiter")
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
	t.Logf("Current arbiter offsetBase [offsetBase:%d]", arbiter.(*UnboundArbiter).offsetBase)
	t.Logf("Arbiter stats [stat:%v]", arbiter.GetStat())

	for i := 0; i != 10; i++ {
		select {
		case message := <-messages:
			t.Logf("Message received [offset:%d]", message.Offset)
		case <-time.After(time.Second):
			// force yield, wait for arbiter goroutine to process
			t.Fatalf("Ready message not received when window is not full")
		}
	}

	// test infinite consuming
	go func() {
		for {
			select {
			case <-messages:
			case <-time.After(time.Second):
				// force yield, wait for arbiter goroutine to process
				t.Fatalf("Ready message not received when window is not full")
			}
			time.Sleep(time.Microsecond)
		}
	}()

	// commit a non-base offset
	offsets <- 5
	time.Sleep(time.Second)
	assert.Equal(t, int64(1), arbiter.(*UnboundArbiter).offsetBase, "Arbiter offsetBase is not 1")

	// commit base offset
	offsets <- 1
	time.Sleep(time.Second)
	assert.NotEqual(t, int64(1), arbiter.(*UnboundArbiter).offsetBase, "Arbiter offsetBase is 1")

	// commit several offset, including previous offset 5
	offsets <- 3
	offsets <- 4
	offsets <- 2
	time.Sleep(time.Second)
	assert.Equal(t, int64(6), arbiter.(*UnboundArbiter).offsetBase, "Arbiter offsetBase is not 6")

	t.Logf("PartitionConsumerMock channel length [length:%d][capacity:%d]",
		len(manager.GetKafkaPartitionConsumer().Messages()), cap(manager.GetKafkaPartitionConsumer().Messages()))
	t.Logf("Arbiter message channel length and capacity [length:%d][capacity:%d]", len(messages), cap(messages))
	t.Logf("Current arbiter offsetBase [offsetBase:%d]", arbiter.(*UnboundArbiter).offsetBase)
	t.Logf("Arbiter stats [stat:%v]", arbiter.GetStat())

	t.Logf("UnboundArbiter working normally")
}
