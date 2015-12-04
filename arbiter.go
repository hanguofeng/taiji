package main

import "github.com/Shopify/sarama"

type Arbiter interface {
	OffsetChannel() chan<- int64
	MessageChannel() <-chan *sarama.ConsumerMessage
	Init(config *CallbackItemConfig, arbiterConfig *MapConfig, manager *PartitionManager)
	Run()
	Close()
}

type SequentialArbiter struct {
}

// 以下为本次开发的可选内容
type SlidingWindowArbiter struct {
}

type LeakyBucketArbiter struct {
}

type UnboundArbiter struct {
}
