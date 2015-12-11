package main

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

type PartitionConsumerMock struct {
	stopper  chan struct{}
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

func (pcm *PartitionConsumerMock) Init() {
	pcm.messages = make(chan *sarama.ConsumerMessage, 5)
	pcm.errors = make(chan *sarama.ConsumerError)

	go func() {
		i := int64(-5)

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

func (pcm *PartitionConsumerMock) Errors() <-chan *sarama.ConsumerError {
	return pcm.errors
}

func (pcm *PartitionConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	return pcm.messages
}

func (pcm *PartitionConsumerMock) Close() error {
	close(pcm.stopper)
	return nil
}

func (*PartitionConsumerMock) AsyncClose() {
}

func (*PartitionConsumerMock) HighWaterMarkOffset() int64 {
	return 0
}

func TestSequentialArbiter(t *testing.T) {
	t.Log("Allocated new SequentialArbiter")
	arbiter, err := NewArbiter("Sequential")
	assert.Nil(t, err, "Create SequentialArbiter failed")

	callbackConfig := &CallbackItemConfig{
		Url: "http://invalid_url_that_should_never_deliver",
	}
	arbiterConfig := make(ArbiterConfig)
	callbackManager := &CallbackManager{}
	partitionConsumerMock := &PartitionConsumerMock{
		stopper: make(chan struct{}),
	}
	partitionConsumerMock.Init()
	manager := &PartitionManager{
		partitionConsumer: &PartitionConsumer{
			consumer: partitionConsumerMock,
		},
		manager: callbackManager,
	}
	t.Log("Initializing SequentialArbiter")
	arbiter.Init(callbackConfig, arbiterConfig, manager)
	t.Log("Started SequentialArbiter")
	go arbiter.Run()
	defer arbiter.Close()
	defer manager.GetConsumer().Close()
	if err := arbiter.Ready(); err != nil {
		t.Fatalf("Arbiter start failed [err:%s]", err)
	}
	messages := arbiter.MessageChannel()
	offsets := arbiter.OffsetChannel()
	lastOffset := int64(-6)
	lastCommit := int64(-6)

	// first try loop get 3 messages for test
	for i := 0; i != 3; i++ {
		select {
		case message := <-messages:
			if message.Offset != lastOffset+1 {
				t.Fatalf("Offset is not sequential [lastOffset:%d][currentOffset:%d]",
					lastOffset, message.Offset)
			}
			if lastCommit != lastOffset {
				t.Fatalf("Offset commit is not sequential [lastCommit:%d][lastOffset:%d]",
					lastCommit, lastOffset)
			}
			t.Logf("Message received [offset:%d]", message.Offset)
			lastOffset = message.Offset
		}

		select {
		case message := <-messages:
			t.Fatalf("Ready two message without offset commit [offset:%d]", message.Offset)
		default:
			t.Logf("No message is received before commit offset, SequentialArbiter take effect, sending offset [offset:%d]",
				lastOffset)
			offsets <- lastOffset
			lastCommit = lastOffset
		}
	}

	if lastOffset != -3 || lastCommit != -3 {
		t.Fatalf("SequentialArbiter not working normally [lastOffset:%d][lastCommit:%d]",
			lastOffset, lastCommit)
	}

	t.Logf("SequentialArbiter working normally")
}
