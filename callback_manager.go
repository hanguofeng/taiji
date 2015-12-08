package main

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
	"github.com/wvanbergen/kazoo-go"
	"sync"
	"time"
)

var (
	AlreadyClosing = errors.New("The consumer group is already shutting down.")
)

type CallbackManager struct {
	Topics                 []string
	Group                  string
	Url                    string
	config                 *CallbackItemConfig
	cgConfig               *CgConfig
	kazoo                  *kazoo.Kazoo                 // ZK
	kazooGroup             *kazoo.Consumergroup         // ZK ConsumerGroup /consumers/<cgname>/ Object
	kazooGroupInstance     *kazoo.ConsumergroupInstance // ZK ConsumerGroup /consumers/<cgname>/ids/<cginstance> Object
	saramaConsumer         sarama.Consumer              // Kafka Sarama Consumer
	partitionManagers      []*PartitionManager
	offsetManager          *OffsetManager
	callbackManagerStopper chan struct{}
	wg                     *sync.WaitGroup
}

type CgConfig struct {
	*sarama.Config

	Zookeeper *kazoo.Config

	Offsets struct {
		Initial           int64         // The initial offset method to use if the consumer has no previously stored offset. Must be either sarama.OffsetOldest (default) or sarama.OffsetNewest.
		ProcessingTimeout time.Duration // Time to wait for all the offsets for a partition to be processed after stopping to consume from it. Defaults to 1 minute.
		CommitInterval    time.Duration // The interval between which the processed offsets are commited.
		ResetOffsets      bool          // Resets the offsets for the consumergroup so that it won't resume from where it left off previously.
	}
}

func NewConfig() *CgConfig {
	cgConfig := &CgConfig{}
	cgConfig.Config = sarama.NewConfig()
	cgConfig.Zookeeper = kazoo.NewConfig()
	cgConfig.Offsets.Initial = sarama.OffsetOldest
	cgConfig.Offsets.ProcessingTimeout = 60 * time.Second
	cgConfig.Offsets.CommitInterval = 10 * time.Second

	return cgConfig
}

func (cgConfig *CgConfig) Validate() error {
	if cgConfig.Zookeeper.Timeout <= 0 {
		return sarama.ConfigurationError("ZookeeperTimeout should have a duration > 0")
	}

	if cgConfig.Offsets.CommitInterval <= 0 {
		return sarama.ConfigurationError("CommitInterval should have a duration > 0")
	}

	if cgConfig.Offsets.Initial != sarama.OffsetOldest && cgConfig.Offsets.Initial != sarama.OffsetNewest {
		return errors.New("Offsets.Initial should be sarama.OffsetOldest or sarama.OffsetNewest")
	}

	if cgConfig.Config != nil {
		if err := cgConfig.Config.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func NewCallbackManager() *CallbackManager {
	return &CallbackManager{}
}

func (this *CallbackManager) Init(config *CallbackItemConfig) (err error) {
	this.Topics = config.Topics
	this.Url = config.Url
	this.Group = getGroupName(this.Url)
	this.config = config

	// connect zookeeper
	if this.Group == "" {
		return sarama.ConfigurationError("Empty consumergroup name")
	}

	if len(this.Topics) == 0 {
		return sarama.ConfigurationError("No topics provided")
	}

	if len(this.config.Zookeepers) == 0 {
		return errors.New("You need to provide at least one zookeeper node address!")
	}

	if this.cgConfig == nil {
		this.cgConfig = NewConfig()
	}
	this.cgConfig.ClientID = this.Group

	// Validate configuration
	if err := this.cgConfig.Validate(); err != nil {
		return err
	}

	var kz *kazoo.Kazoo
	if kz, err = kazoo.NewKazoo(this.config.Zookeepers, this.cgConfig.Zookeeper); err != nil {
		return err
	}

	brokers, err := kz.BrokerList()
	if err != nil {
		kz.Close()
		return err
	}

	group := kz.Consumergroup(this.Group)

	if this.cgConfig.Offsets.ResetOffsets {
		err = group.ResetOffsets()
		if err != nil {
			seelog.Errorf("Failed to reset offsets of consumergroup [err:%s]", err)
			kz.Close()
			return
		}
	}

	//connect sarama consumer
	this.kazooGroupInstance = group.NewInstance()

	if this.saramaConsumer, err = sarama.NewConsumer(brokers, this.cgConfig.Config); err != nil {
		kz.Close()
		return
	}

	// set group as MD5(Url)
	// init OffsetManager

	offsetManager := NewOffsetManager()
	offsetManager.Init(config.OffsetConfig, this)

	return nil
}

func (this *CallbackManager) Run() (err error) {
	for {
		select {
		case <-this.callbackManagerStopper:
			return
		default:
		}

		// Register consumer group
		if exists, err := this.kazooGroup.Exists(); err != nil {
			seelog.Errorf("Failed to check for existence of consumergroup [err:%s]", err)
			_ = this.saramaConsumer.Close()
			_ = this.kazoo.Close()
			return err
		} else if !exists {
			seelog.Infof("Consumergroup does not yet exists, creating [consumergroup:%s] ", this.Group)
			if err := this.kazooGroup.Create(); err != nil {
				seelog.Errorf("Failed to create consumergroup in zookeeper [err:%s]", err)
				_ = this.saramaConsumer.Close()
				_ = this.kazoo.Close()
				return err
			}
		}

		consumers, consumerChanges, err := this.kazooGroup.WatchInstances()
		if err != nil {
			seelog.Infof("Failed to get list of registered consumer instances [err:%s]", err)
			return err
		}
		seelog.Infof("Currently registered consumers [totalconsumers:%d]", len(consumers))

		if err := this.partitionRun(consumers); err != nil {
			seelog.Errorf("Failed to init partition consumer [err:%s]", err)
			return err
		}

		stopper := make(chan struct{})

	partitionManagerRespawnLoop:
		for {
			select {
			case <-this.callbackManagerStopper:
				close(stopper)
				return err

			case <-consumerChanges:
				seelog.Infof("Triggering rebalance due to consumer list change")
				close(stopper)
				this.wg.Wait()
				seelog.Infof("Waiting for 5 seconds to avoid consumer inflight rebalance herd")
				time.Sleep(5 * time.Second)
				break partitionManagerRespawnLoop
			}
		}
	}
}

func (this *CallbackManager) Close() error {
	shutdownError := AlreadyClosing
	close(this.callbackManagerStopper)
	this.wg.Wait()

	defer this.kazoo.Close()
	// sync close partitionManager
	for _, partitionManager := range this.partitionManagers {
		partitionManager.Close()
	}

	// sync close offsetManager
	if err := this.offsetManager.Close(); err != nil {
		seelog.Errorf("Failed closing the offset manager [err:%s]", err)
	}

	if shutdownError = this.kazooGroupInstance.Deregister(); shutdownError != nil {
		seelog.Errorf("Failed deregistering consumer instance [err:%s]", shutdownError)
	} else {
		seelog.Infof("Deregistered consumer instance [instanceid:%s]", this.kazooGroupInstance.ID)
	}

	if shutdownError = this.saramaConsumer.Close(); shutdownError != nil {
		seelog.Errorf("Failed closing the Sarama client [err:%s]", shutdownError)
	}

	return nil
}

func (this *CallbackManager) GetOffsetManager() *OffsetManager {
	return this.offsetManager
}

func (*CallbackManager) GetKazooGroup() *kazoo.Consumergroup {
	return nil
}

func (*CallbackManager) GetConsumer() *sarama.Consumer {
	return nil
}

func (this *CallbackManager) partitionRun(consumers kazoo.ConsumergroupInstanceList) (err error) {

	// register new kazoo.ConsumerGroup instance
	if err := this.kazooGroupInstance.Register(this.Topics); err != nil {
		seelog.Errorf("Failed to register consumer instance [err:%s]", err)
		return err
	} else {
		seelog.Infof("Consumer instance registered [insatanceid:%s]", this.kazooGroupInstance.ID)
	}

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

		// init/run partitionManager
		for _, partitionManager := range this.partitionManagers {
			partitionManager.Close()
		}
		event := make(chan int)

		for i := 0; i < len(myPartitions); i++ {
			partitionManager := NewPartitionManager(topic)
			if err := partitionManager.Init(this.config, topic, int32(i), this); err != nil {
				seelog.Criticalf("Init partition manager failed [url:%s][err:%s]", this.Url, err)
				return err
			}
			this.partitionManagers = append(this.partitionManagers, partitionManager)
			this.wg.Add(1)
			go func(topic string, idx int) {
				defer func() {
					event <- idx
				}()
				partitionManager.Run(this.wg, this.callbackManagerStopper)
			}(topic, i)
		}
	}
	return nil
}
