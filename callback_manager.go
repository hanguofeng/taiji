package main

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
	"github.com/wvanbergen/kazoo-go"
)

const (
	GET_CONSUMER_LIST_RETRY_TIME     = 10 * time.Second
	RUN_PARTITION_MANAGER_RETRY_TIME = 10 * time.Second
	CONSUMER_LIST_CHANGE_RELOAD_TIME = 5 * time.Second
	WATCH_INSTANCE_CHANGE_DELAY_TIME = 5 * time.Second
)

type CallbackManager struct {
	*StartStopControl
	Topics    []string
	GroupName string
	Url       string

	// config
	config          *CallbackItemConfig
	saramaConfig    *sarama.Config
	zookeeperConfig *kazoo.Config

	// zk instances
	kazoo              *kazoo.Kazoo                 // ZK
	kazooGroup         *kazoo.Consumergroup         // ZK ConsumerGroup /consumers/<cgname>/ Object
	kazooGroupInstance *kazoo.ConsumergroupInstance // ZK ConsumerGroup /consumers/<cgname>/ids/<cginstance> Object

	// kafka sarama consumer
	saramaConsumer sarama.Consumer

	// partition manager
	partitionManagers      []*PartitionManager
	partitionManagerRunner *ServiceRunner

	// offset manager
	offsetManager *OffsetManager
}

func NewCallbackManager() *CallbackManager {
	return &CallbackManager{}
}

func (this *CallbackManager) Init(config *CallbackItemConfig) error {
	var err error

	this.config = config
	this.Topics = config.Topics
	this.Url = config.Url

	// set group as MD5(Url)
	this.GroupName = getGroupName(this.Url)
	this.saramaConfig = sarama.NewConfig()
	this.zookeeperConfig = kazoo.NewConfig()
	this.saramaConfig.ClientID = this.GroupName

	// init OffsetManager
	this.offsetManager = NewOffsetManager()
	if err = this.offsetManager.Init(config.OffsetConfig, this); err != nil {
		return err
	}

	this.partitionManagerRunner = NewServiceRunner()

	return nil
}

func (this *CallbackManager) Run() error {
	// mark service as started
	if err := this.ensureStart(); err != nil {
		return err
	}

	defer this.markStop()

	// init zookeeper
	if err := this.connectZookeeper(); err != nil {
		return err
	}

	// init kafka sarama consumer
	if err := this.connectKafka(); err != nil {
		return err
	}

callbackManagerFailoverLoop:
	for {
		if !this.Running() {
			break callbackManagerFailoverLoop
		}

		if err := this.registerConsumergroup(); err != nil {
			return err
		}

		seelog.Infof("Waiting for %v to avoid consumer register rebalance herd",
			WATCH_INSTANCE_CHANGE_DELAY_TIME)
		time.Sleep(WATCH_INSTANCE_CHANGE_DELAY_TIME)

		consumers, consumerChanges, err := this.kazooGroup.WatchInstances()

		if err != nil {
			seelog.Errorf("Failed to get list of registered consumer instances [err:%s]", err)
			time.Sleep(GET_CONSUMER_LIST_RETRY_TIME)
			continue
		}

		seelog.Infof("Currently registered consumers [totalconsumers:%d]", len(consumers))

		// get partitionConsuming assignments
		// start ServiceRunner of PartitionManager
		// TODO
		if err := this.partitionRun(consumers); err != nil {
			seelog.Errorf("Failed to init partition consumer [err:%s]", err)
			time.Sleep(RUN_PARTITION_MANAGER_RETRY_TIME)
			continue
		}

		select {
		case <-this.WaitForCloseChannel():
			this.partitionManagerRunner.Close()
			break callbackManagerFailoverLoop

		case <-consumerChanges:
			seelog.Infof("Triggering rebalance due to consumer list change")
			this.partitionManagerRunner.Close()
			seelog.Infof("Waiting for %v to avoid consumer inflight rebalance herd",
				CONSUMER_LIST_CHANGE_RELOAD_TIME)
			time.Sleep(CONSUMER_LIST_CHANGE_RELOAD_TIME)
		case <-this.partitionManagerRunner.WaitForExitChannel():
			seelog.Warn("PartitionManager unexpectedly stopped")
		}
	}

	// sync close offsetManager
	if err := this.offsetManager.Close(); err != nil {
		seelog.Errorf("Failed closing the offset manager [err:%s]", err)
	}

	// deregister Consumergroup instance from zookeeper
	if err := this.kazooGroupInstance.Deregister(); err != nil {
		seelog.Errorf("Failed deregistering consumer instance [err:%s]", err)
	} else {
		seelog.Infof("Deregistered consumer instance [instanceid:%s]", this.kazooGroupInstance.ID)
	}

	// close sarama Consumer
	if err := this.saramaConsumer.Close(); err != nil {
		seelog.Errorf("Failed closing the Sarama client [err:%s]", err)
	}

	// close zookeeper connection
	if err := this.kazoo.Close(); err != nil {
		seelog.Errorf("Failed closing the Zookeeper connection [err:%s]", err)
	}

	return nil
}

func (this *CallbackManager) GetOffsetManager() *OffsetManager {
	return this.offsetManager
}

func (this *CallbackManager) GetKazooGroup() *kazoo.Consumergroup {
	return this.kazooGroup
}

func (this *CallbackManager) GetKazooGroupInstance() *kazoo.ConsumergroupInstance {
	return this.kazooGroupInstance
}

func (this *CallbackManager) GetConsumer() sarama.Consumer {
	return this.saramaConsumer
}

func (this *CallbackManager) connectZookeeper() error {
	var err error

	// zookeeper ConsumerGroup instance initialization
	if this.kazoo, err = kazoo.NewKazoo(this.config.Zookeepers, this.zookeeperConfig); err != nil {
		return err
	}
	this.kazooGroup = this.kazoo.Consumergroup(this.GroupName)
	this.kazooGroupInstance = this.kazooGroup.NewInstance()

	return nil
}

func (this *CallbackManager) connectKafka() error {
	var err error

	// kafka consumer initialization
	brokers, err := this.kazoo.BrokerList()
	if err != nil {
		return err
	}

	// connect kafka using sarama Consumer
	if this.saramaConsumer, err = sarama.NewConsumer(brokers, this.saramaConfig); err != nil {
		return err
	}

	return nil
}

func (this *CallbackManager) registerConsumergroup() error {
	// Register Consumergroup zk node
	if exists, err := this.kazooGroup.Exists(); err != nil {
		seelog.Errorf("Failed to check for existence of consumergroup [err:%s]", err)
		this.saramaConsumer.Close()
		this.kazoo.Close()
		return err
	} else if !exists {
		seelog.Infof("Consumergroup does not yet exists, creating [consumergroup:%s] ", this.GroupName)
		if err := this.kazooGroup.Create(); err != nil {
			seelog.Errorf("Failed to create consumergroup in zookeeper [err:%s]", err)
			this.saramaConsumer.Close()
			this.kazoo.Close()
			return err
		}
	}

	// register new kazoo.ConsumerGroup instance
	if err := this.kazooGroupInstance.Register(this.Topics); err != nil {
		seelog.Errorf("Failed to register consumer instance [err:%s]", err)
		return err
	} else {
		seelog.Infof("Consumer instance registered [insatanceid:%s]", this.kazooGroupInstance.ID)
	}

	return nil
}

func (this *CallbackManager) partitionRun(consumers kazoo.ConsumergroupInstanceList) error {
	this.partitionManagers = make([]*PartitionManager, 0)

	for _, topic := range this.Topics {
		// Fetch a list of partition IDs
		partitions, err := this.kazoo.Topic(topic).Partitions()
		if err != nil {
			seelog.Errorf("Failed to get list of partitions [topic:%s][err:%s]", this.Topics[0], err)
			return err
		}

		partitionLeaders, err := retrievePartitionLeaders(partitions)
		if err != nil {
			seelog.Errorf("Failed to get leaders of partitions [topic:%s][err:%s]", topic, err)
			return err
		}

		// divide partition for each callback manager instance
		dividedPartitions := dividePartitionsBetweenConsumers(consumers, partitionLeaders)
		myPartitions := dividedPartitions[this.kazooGroupInstance.ID]

		for i := 0; i < len(myPartitions); i++ {
			partitionManager := NewPartitionManager()
			if err := partitionManager.Init(this.config, topic, myPartitions[i].ID, this); err != nil {
				seelog.Criticalf("Init partition manager failed [url:%s][err:%s]", this.Url, err)
				return err
			}
			this.partitionManagers = append(this.partitionManagers, partitionManager)
		}
	}

	this.partitionManagerRunner.RetryTimes = len(this.partitionManagers) * 3
	if _, err := this.partitionManagerRunner.RunAsync(this.partitionManagers); err != nil {
		return err
	}

	return nil
}
