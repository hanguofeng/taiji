package main

import (
	"time"

	"github.com/crask/kafka/consumergroup"
	"github.com/golang/glog"
	"gopkg.in/Shopify/sarama.v1"
)

type Coordinator struct {
	Topics      []string
	Zookeeper   []string
	ZkPath      string
	CallbackUrl string
	Consumer    *consumergroup.ConsumerGroup
}

func NewCoordinator() *Coordinator {
	return &Coordinator{}
}

func (this *Coordinator) Init(config *CallbackItemConfig) error {
	this.Topics = config.Topics
	this.Zookeeper = config.Zookeepers
	this.ZkPath = config.ZkPath
	this.Consumer = nil
	this.CallbackUrl = config.Url

	cgConfig := consumergroup.NewConfig()
	cgConfig.Offsets.ProcessingTimeout = 10 * time.Second
	cgConfig.Offsets.CommitInterval = time.Duration(commitInterval) * time.Second
	cgConfig.Offsets.Initial = sarama.OffsetNewest

	if len(this.ZkPath) > 0 {
		cgConfig.Zookeeper.Chroot = this.ZkPath
	}

	cgName := getGroupName(this.CallbackUrl)
	consumer, err := consumergroup.JoinConsumerGroup(cgName, this.Topics, this.Zookeeper, cgConfig)
	if err != nil {
		glog.Errorf("Failed to join consumer group for url[%v], %v", this.CallbackUrl, err.Error())
		return err
	} else {
		glog.V(1).Infof("Join consumer group for url[%s] with UUID[%s]", this.CallbackUrl, cgName)
	}

	glog.Infoln(consumer)

	this.Consumer = consumer
	return nil
}

func (this *Coordinator) GetConsumer() *consumergroup.ConsumerGroup {
	return this.Consumer
}

func (this *Coordinator) Work() {
	for err := range this.Consumer.Errors() {
		glog.Errorln("Error working consumers", err)
	}
}

func (this *Coordinator) Closed() bool {
	return this.Consumer.Closed()
}

func (this *Coordinator) Close() {
	if err := this.Consumer.Close(); err != nil {
		glog.Errorln("Error closing consumers", err)
	}
}