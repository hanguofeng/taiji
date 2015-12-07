package main

import "github.com/Shopify/sarama"

type PartitionManager struct {
	Topic       string
	Partition   int32
	consumer    sarama.PartitionConsumer
	arbiter     Arbiter
	transporter []Transporter
	manager     *CallbackManager
}

func NewPartitionManager() *PartitionManager {
	return nil
}

func (*PartitionManager) Init(config *CallbackItemConfig, manager *CallbackManager) {
	// init arbiter
	// init worker
}

func (*PartitionManager) Run() {
	// run consumer
	// run arbiter
	// run worker
}

func (*PartitionManager) Close() {
	// close consumer
	// close worker
	// close arbiter
}

func (*PartitionManager) GetConsumer() sarama.PartitionConsumer {
	return nil
}

func (*PartitionManager) GetArbiter() Arbiter {
	return nil
}
