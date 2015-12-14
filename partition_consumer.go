package main

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
	"github.com/wvanbergen/kazoo-go"
)

const PARTITION_CONSUMER_CHANNEL_BUFFER_SIZE = 256

type PartitionConsumer struct {
	*StartStopControl
	Topic     string
	Partition int32
	consumer  sarama.PartitionConsumer
	manager   *PartitionManager
	config    *CallbackItemConfig
}

func NewPartitionConsumer() *PartitionConsumer {
	return &PartitionConsumer{
		StartStopControl: &StartStopControl{},
	}
}

func (pc *PartitionConsumer) Init(config *CallbackItemConfig, topic string, partition int32, manager *PartitionManager) error {
	pc.config = config
	pc.Topic = topic
	pc.Partition = partition
	pc.manager = manager

	return nil
}

func (pc *PartitionConsumer) Run() error {
	// start stop control
	if err := pc.ensureStart(); err != nil {
		return err
	}

	// defer is executed reversed to declaration order
	defer pc.markStop()

	// claim partition, zk /consumers/CG/owners
	for maxRetries, tries := 5, 0; tries < maxRetries; tries++ {
		if err := pc.manager.GetCallbackManager().GetKazooGroupInstance().ClaimPartition(pc.Topic, pc.Partition); err == nil {
			break
		} else if err == kazoo.ErrPartitionClaimedByOther && tries+1 < maxRetries {
			time.Sleep(time.Duration(5*tries) * time.Second)
		} else {
			seelog.Errorf("Failed to claim the partition [topic:%s][partition:%d][err:%s]", pc.Topic, pc.Partition, err)
			return err
		}
	}

	defer pc.manager.GetCallbackManager().GetKazooGroupInstance().ReleasePartition(pc.Topic, pc.Partition)

	// initialize partition offset manager instance
	nextOffset, err := pc.manager.GetCallbackManager().GetOffsetManager().InitializePartition(pc.Topic, pc.Partition)
	if err != nil {
		seelog.Errorf("Failed to determine initial offset [topic:%s][partition:%d][err:%s]", pc.Topic, pc.Partition, err)
		return err
	}

	defer pc.manager.GetCallbackManager().GetOffsetManager().FinalizePartition(pc.Topic, pc.Partition)

	// start partition consumer instance
	if nextOffset >= 0 {
		seelog.Infof("Partition consumer starting at offset [topic:%s][partition:%d][nextoffset:%d]", pc.Topic, pc.Partition, nextOffset)
	} else {
		if pc.config.InitialFromOldest {
			seelog.Infof("Partition consumer starting at the oldest available offset [topic:%s][partition:%d]", pc.Topic, pc.Partition)
			nextOffset = sarama.OffsetOldest
		} else {
			seelog.Infof("Partition consumer listening for new messages only [topic:%s][partition:%d]", pc.Topic, pc.Partition)
			nextOffset = sarama.OffsetNewest
		}
	}

	pc.consumer, err = pc.manager.GetCallbackManager().GetConsumer().ConsumePartition(pc.Topic, pc.Partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
		seelog.Infof("Partition consumer offset out of range [topic:%s][partition:%d]", pc.Topic, pc.Partition)
		// if the offset is out of range, simplistically decide whether to use OffsetNewest or OffsetOldest
		// if the configuration specified offsetOldest, then switch to the oldest available offset, else
		// switch to the newest available offset.
		if pc.config.InitialFromOldest {
			nextOffset = sarama.OffsetOldest
			seelog.Infof("Partition consumer offset reset to oldest available offset [topic:%s][partition:%d]", pc.Topic, pc.Partition)
		} else {
			nextOffset = sarama.OffsetNewest
			seelog.Infof("Partition consumer offset reset to newest available offset [topic:%s][partition:%d]", pc.Topic, pc.Partition)
		}
		// retry the consumePartition with the adjusted offset
		pc.consumer, err = pc.manager.GetCallbackManager().GetConsumer().ConsumePartition(pc.Topic, pc.Partition, nextOffset)
	}

	if err != nil {
		seelog.Errorf("Failed to start partition consumer [topic:%s][partition:%d][err:%s]", pc.Topic, pc.Partition, err)
		return err
	}

	defer pc.consumer.Close()

	// start stop control
	if !pc.Running() {
		return nil
	}
	pc.markReady()

partitionConsumerLoop:
	for {
		select {
		case <-pc.WaitForCloseChannel():
			break partitionConsumerLoop
		case err := <-pc.consumer.Errors():
			seelog.Warnf("Received consumer error message [topic:%s][partition:%d][err:%s]",
				pc.Topic, pc.Partition, err)
		}
	}

	seelog.Infof("Stopping partition consumer at offset [topic:%s][partition:%d]", pc.Topic, pc.Partition)

	return nil
}

func (pc *PartitionConsumer) GetConsumer() sarama.PartitionConsumer {
	return pc.consumer
}
