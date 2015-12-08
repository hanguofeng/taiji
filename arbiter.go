package main

import "github.com/Shopify/sarama"

type Arbiter interface {
	OffsetChannel() chan<- int64
	MessageChannel() <-chan *sarama.ConsumerMessage
	Init(config *CallbackItemConfig, arbiterConfig ArbiterConfig, manager *PartitionManager) error
	Run()
	Close()
}

// 以下为本次开发的可选内容

type LeakyBucketArbiter struct {
}
