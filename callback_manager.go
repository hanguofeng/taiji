package main

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/wvanbergen/kazoo-go"
)

const (
	REGISTER_CONSUMER_GROUP_RETRY_TIME = 10 * time.Second
	GET_CONSUMER_LIST_RETRY_TIME       = 10 * time.Second
	RUN_PARTITION_MANAGER_RETRY_TIME   = 10 * time.Second
	CONSUMER_LIST_CHANGE_RELOAD_TIME   = 5 * time.Second
	WATCH_INSTANCE_CHANGE_DELAY_TIME   = 5 * time.Second
)

type CallbackManager struct {
	*StartStopControl
	Topics    []string
	GroupName string
	Url       string

	// config
	config          *CallbackItemConfig
	kafkaConfig     *sarama.Config
	zookeeperConfig *kazoo.Config

	// zk instances
	kazoo              *kazoo.Kazoo                 // ZK
	kazooGroup         *kazoo.Consumergroup         // ZK ConsumerGroup /consumers/<cgname>/ Object
	kazooGroupInstance *kazoo.ConsumergroupInstance // ZK ConsumerGroup /consumers/<cgname>/ids/<cginstance> Object

	// kafka sarama consumer
	kafkaConsumer sarama.Consumer

	// partition manager
	partitionManagers      []*PartitionManager
	partitionManagerRunner *ServiceRunner

	// offset manager
	offsetManager *OffsetManager
}

func NewCallbackManager() *CallbackManager {
	return &CallbackManager{
		StartStopControl: &StartStopControl{},
	}
}

func (cm *CallbackManager) Init(config *CallbackItemConfig) error {
	var err error

	cm.config = config
	cm.Topics = config.Topics
	cm.Url = config.Url

	// set group as MD5(Url)
	cm.GroupName = getGroupName(cm.Url)
	cm.kafkaConfig = sarama.NewConfig()
	cm.zookeeperConfig = kazoo.NewConfig()
	cm.zookeeperConfig.Chroot = config.ZkPath
	cm.kafkaConfig.ClientID = cm.GroupName

	// init OffsetManager
	cm.offsetManager = NewOffsetManager()
	if err = cm.offsetManager.Init(config.OffsetConfig, cm); err != nil {
		return err
	}

	cm.partitionManagerRunner = NewServiceRunner()

	return nil
}

func (cm *CallbackManager) Run() error {
	// mark service as started
	if err := cm.ensureStart(); err != nil {
		return err
	}

	defer cm.markStop()

	// init zookeeper
	if err := cm.connectZookeeper(); err != nil {
		return err
	}

	// init kafka sarama consumer
	if err := cm.connectKafka(); err != nil {
		return err
	}

	go cm.offsetManager.Run()

callbackManagerFailoverLoop:
	for {
		if !cm.Running() {
			break callbackManagerFailoverLoop
		}

		if err := cm.registerConsumergroup(); err != nil {
			time.Sleep(REGISTER_CONSUMER_GROUP_RETRY_TIME)
			continue
		}

		glog.Infof("Waiting for %v to avoid consumer register rebalance herd",
			WATCH_INSTANCE_CHANGE_DELAY_TIME)
		time.Sleep(WATCH_INSTANCE_CHANGE_DELAY_TIME)

		consumers, consumerChanges, err := cm.kazooGroup.WatchInstances()

		if err != nil {
			glog.Errorf("Failed to get list of registered consumer instances [err:%s]", err)
			time.Sleep(GET_CONSUMER_LIST_RETRY_TIME)
			continue
		}

		glog.Infof("Currently registered consumers [totalConsumers:%d]", len(consumers))

		// get partitionConsuming assignments
		// start ServiceRunner of PartitionManager
		// TODO refactor this
		if err := cm.partitionRun(consumers); err != nil {
			glog.Errorf("Failed to init partition consumer [err:%s]", err)
			time.Sleep(RUN_PARTITION_MANAGER_RETRY_TIME)
			continue
		}

		select {
		case <-cm.WaitForCloseChannel():
			cm.partitionManagerRunner.Close()
			break callbackManagerFailoverLoop

		case <-consumerChanges:
			glog.Infof("Triggering rebalance due to consumer list change")
			cm.partitionManagerRunner.Close()
			glog.Infof("Waiting for %v to avoid consumer inflight rebalance herd",
				CONSUMER_LIST_CHANGE_RELOAD_TIME)
			time.Sleep(CONSUMER_LIST_CHANGE_RELOAD_TIME)
		case <-cm.partitionManagerRunner.WaitForExitChannel():
			glog.Warning("PartitionManager unexpectedly stopped")
		}

		// deregister Consumergroup instance from zookeeper
		if err := cm.kazooGroupInstance.Deregister(); err != nil {
			glog.Errorf("Failed deregistering consumer instance [err:%s]", err)
		} else {
			glog.Infof("Deregistered consumer instance [instanceId:%s]", cm.kazooGroupInstance.ID)
		}
	}

	// sync close offsetManager
	if err := cm.offsetManager.Close(); err != nil {
		glog.Errorf("Failed closing the offset manager [err:%s]", err)
	}

	// close sarama Consumer
	if err := cm.kafkaConsumer.Close(); err != nil {
		glog.Errorf("Failed closing the Sarama client [err:%s]", err)
	}

	// close zookeeper connection
	if err := cm.kazoo.Close(); err != nil {
		glog.Errorf("Failed closing the Zookeeper connection [err:%s]", err)
	}

	return nil
}

func (cm *CallbackManager) GetPartitionManagers() []*PartitionManager {
	return cm.partitionManagers
}

func (cm *CallbackManager) GetOffsetManager() *OffsetManager {
	return cm.offsetManager
}

func (cm *CallbackManager) GetKazooGroup() *kazoo.Consumergroup {
	return cm.kazooGroup
}

func (cm *CallbackManager) GetKazooGroupInstance() *kazoo.ConsumergroupInstance {
	return cm.kazooGroupInstance
}

func (cm *CallbackManager) GetKafkaConsumer() sarama.Consumer {
	return cm.kafkaConsumer
}

func (cm *CallbackManager) connectZookeeper() error {
	var err error

	// zookeeper ConsumerGroup instance initialization
	if cm.kazoo, err = kazoo.NewKazoo(cm.config.Zookeepers, cm.zookeeperConfig); err != nil {
		return err
	}
	cm.kazooGroup = cm.kazoo.Consumergroup(cm.GroupName)
	cm.kazooGroupInstance = cm.kazooGroup.NewInstance()

	return nil
}

func (cm *CallbackManager) connectKafka() error {
	var err error

	// kafka consumer initialization
	brokers, err := cm.kazoo.BrokerList()
	if err != nil {
		return err
	}

	// connect kafka using sarama Consumer
	if cm.kafkaConsumer, err = sarama.NewConsumer(brokers, cm.kafkaConfig); err != nil {
		return err
	}

	return nil
}

func (cm *CallbackManager) registerConsumergroup() error {
	// Register Consumergroup zk node
	if exists, err := cm.kazooGroup.Exists(); err != nil {
		glog.Errorf("Failed to check for existence of consumergroup [err:%s]", err)
		return err
	} else if !exists {
		glog.Infof("Consumergroup does not yet exists, creating [consumergroup:%s] ", cm.GroupName)
		if err := cm.kazooGroup.Create(); err != nil {
			glog.Errorf("Failed to create consumergroup in zookeeper [err:%s]", err)
			return err
		}
	}

	// register new kazoo.ConsumerGroup instance
	if err := cm.kazooGroupInstance.Register(cm.Topics); err != nil {
		glog.Errorf("Failed to register consumer instance [err:%s]", err)
		return err
	} else {
		glog.Infof("Consumer instance registered [instanceId:%s]", cm.kazooGroupInstance.ID)
	}

	return nil
}

func (cm *CallbackManager) partitionRun(consumers kazoo.ConsumergroupInstanceList) error {
	cm.partitionManagers = make([]*PartitionManager, 0)

	for _, topic := range cm.Topics {
		// Fetch a list of partition IDs
		partitions, err := cm.kazoo.Topic(topic).Partitions()
		if err != nil {
			glog.Errorf("Failed to get list of partitions [topic:%s][err:%s]", cm.Topics[0], err)
			return err
		}

		partitionLeaders, err := retrievePartitionLeaders(partitions)
		if err != nil {
			glog.Errorf("Failed to get leaders of partitions [topic:%s][err:%s]", topic, err)
			return err
		}

		// divide partition for each callback manager instance
		dividedPartitions := dividePartitionsBetweenConsumers(consumers, partitionLeaders)
		myPartitions := dividedPartitions[cm.kazooGroupInstance.ID]

		for i := 0; i < len(myPartitions); i++ {
			partitionManager := NewPartitionManager()
			if err := partitionManager.Init(cm.config, topic, myPartitions[i].ID, cm); err != nil {
				glog.Fatalf("Init partition manager failed [url:%s][err:%s]", cm.Url, err)
				return err
			}
			cm.partitionManagers = append(cm.partitionManagers, partitionManager)
		}
	}

	cm.partitionManagerRunner.RetryTimes = len(cm.partitionManagers) * 3
	cm.partitionManagerRunner.Prepare()
	if _, err := cm.partitionManagerRunner.RunAsync(cm.partitionManagers); err != nil {
		return err
	}

	return nil
}
