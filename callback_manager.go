package main

import (
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
)

type CallbackManager struct {
	Topic              []string
	Group              string
	Url                string
	config             *CallbackItemConfig
	kazoo              *kazoo.Kazoo                 // ZK
	kazooGroup         *kazoo.Consumergroup         // ZK ConsumerGroup /consumers/<cgname>/ Object
	kazooGroupInstance *kazoo.ConsumergroupInstance // ZK ConsumerGroup /consumers/<cgname>/ids/<cginstance> Object
	saramaConsumer     sarama.Consumer              // Kafka Sarama Consumer
	partitionManagers  []*PartitionManager
	offsetManager      *OffsetManager
}

func NewCallbackManager() *CallbackManager {
	return nil
}

func (*CallbackManager) Init(config *CallbackItemConfig) {
	// connect zookeeper
	// set group as MD5(Url)
	// connect kafka using sarama
	// init OffsetManager
}

func (*CallbackManager) Run() {
	// register new kazoo.ConsumerGroup instance
	// init/run partitionManager
}

func (*CallbackManager) Close() {
	// sync close partitionManager
	// sync close offsetManager
}

func (*CallbackManager) GetOffsetManager() *OffsetManager {
	return nil
}

func (*CallbackManager) GetKazooGroup() *kazoo.Consumergroup {
	return nil
}

func (*CallbackManager) GetConsumer() *sarama.Consumer {
	return nil
}
