package main

import (
	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
)

type PartitionManager struct {
	Topic       string
	Partition   int32
	consumer    sarama.PartitionConsumer
	arbiter     Arbiter
	transporter []Transporter
	manager     *CallbackManager
	messages    chan *sarama.ConsumerMessage
	errors      chan *sarama.ConsumerError
}

func NewPartitionManager(topic string) *PartitionManager {
	return &PartitionManager{
		Topic: topic,
	}
}

func (this *PartitionManager) Init(config *CallbackItemConfig, topic string, partition int32, manager *CallbackManager) (err error) {
	this.Topic = topic
	this.Partition = partition

	// init arbiter
	this.arbiter = nil
	// init worker
	this.transporter = nil

	for maxRetries, tries := 5, 0; tries < maxRetries; tries++ {
		if err := this.manager.kazooGroupInstance.ClaimPartition(this.Topic, this.Partition); err == nil {
			break
		} else if err == kazoo.ErrPartitionClaimedByOther && tries+1 < maxRetries {
			time.Sleep(time.Duration(5*tries) * time.Second)
		} else {
			seelog.Errorf("Failed to claim the partition [topic:%s][partition:%d][err:%s]", this.Topic, this.Partition, err)
			return err
		}
	}

	nextOffset, err := this.manager.GetOffsetManager().InitializePartition(this.Topic, this.Partition)
	if err != nil {
		seelog.Errorf("Failed to determine initial offset [topic:%s][partition:%d][err:%s]", this.Topic, this.Partition, err)
		return
	}

	if nextOffset >= 0 {
		seelog.Infof("Partition consumer starting at offset [topic:%s][partition:%d][nextoffset:%d]", this.Topic, this.Partition, nextOffset)
	} else {
		nextOffset = this.manager.cgConfig.Offsets.Initial
		if nextOffset == sarama.OffsetOldest {
			seelog.Infof("Partition consumer starting at the oldest available offset [topic:%s][partition:%d]", this.Topic, this.Partition)
		} else if nextOffset == sarama.OffsetNewest {
			seelog.Infof("Partition consumer listening for new messages only [topic:%s][partition:%d]", this.Topic, this.Partition)
		}
	}

	this.consumer, err = this.manager.saramaConsumer.ConsumePartition(this.Topic, this.Partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
		seelog.Infof("Partition consumer offset out of range [topic:%s][partition:%d]", this.Topic, this.Partition)
		// if the offset is out of range, simplistically decide whether to use OffsetNewest or OffsetOldest
		// if the configuration specified offsetOldest, then switch to the oldest available offset, else
		// switch to the newest available offset.
		if this.manager.cgConfig.Offsets.Initial == sarama.OffsetOldest {
			nextOffset = sarama.OffsetOldest
			seelog.Infof("Partition consumer offset reset to oldest available offset [topic:%s][partition:%d]", this.Topic, this.Partition)
		} else {
			nextOffset = sarama.OffsetNewest
			seelog.Infof("Partition consumer offset reset to newest available offset [topic:%s][partition:%d]", this.Topic, this.Partition)
		}
		// retry the consumePartition with the adjusted offset
		this.consumer, err = this.manager.saramaConsumer.ConsumePartition(this.Topic, this.Partition, nextOffset)
	}
	if err != nil {
		seelog.Errorf("Failed to start partition consumer [topic:%s][partition:%d][err:%s]", this.Topic, this.Partition, err)
		return
	}

	return nil
}

func (this *PartitionManager) Run(wg *sync.WaitGroup, stopper <-chan struct{}) {
	for {
		defer wg.Done()

		select {
		case <-stopper:
			return
		default:
		}

		var lastOffset int64 = -1 // aka unknown
	partitionConsumerLoop:
		for {
			select {
			case <-stopper:
				break partitionConsumerLoop

			case err := <-this.consumer.Errors():
				select {
				case this.errors <- err:
					continue partitionConsumerLoop

				case <-stopper:
					break partitionConsumerLoop
				}

			case message := <-this.consumer.Messages():
				select {
				case <-stopper:
					break partitionConsumerLoop

				case this.messages <- message:
					lastOffset = message.Offset
				}
			}
		}

		seelog.Infof("Stopping partition consumer at offset [topic:%s][partition:%d][lastoffset:%d]", this.Topic, this.Partition, lastOffset)
		if err := this.manager.GetOffsetManager().FinalizePartition(this.Topic, this.Partition); err != nil {
			seelog.Errorf("Finalize parttion consumer [topic:%s][partition:%d][err:%s]", this.Topic, this.Partition, err)
		}
	}

	// run consumer
	// run arbiter
	// run worker
}

func (this *PartitionManager) Close() {
	// close consumer
	// close worker
	// close arbiter
	this.consumer.Close()
	this.manager.kazooGroupInstance.ReleasePartition(this.Topic, this.Partition)

}

func (this *PartitionManager) GetConsumer() sarama.PartitionConsumer {
	return this.consumer
}

func (*PartitionManager) GetArbiter() Arbiter {
	return nil
}

func (this *PartitionManager) GetCallbackManager() *CallbackManager {
	return this.manager

}
