package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	//"net/url"
	"strings"
	"time"

	"github.com/crask/kafka/consumergroup"
	"github.com/golang/glog"
	"gopkg.in/Shopify/sarama.v1"
)

type Worker struct {
	Callback  *WorkerCallback
	Topics    []string
	Zookeeper []string
	ZkPath    string
	Consumer  *consumergroup.ConsumerGroup
}

type WorkerCallback struct {
	Url          string
	RetryTimes   int
	Timeout      time.Duration
	BypassFailed bool
	FailedSleep  time.Duration
}

func NewWorker() *Worker {
	return &Worker{}
}

func (this *Worker) Init(config *CallbackItemConfig) error {
	this.Callback = &WorkerCallback{
		Url:          config.Url,
		RetryTimes:   config.RetryTimes,
		Timeout:      config.Timeout,
		BypassFailed: config.BypassFailed,
		FailedSleep:  config.FailedSleep,
	}
	this.Topics = config.Topics
	this.Zookeeper = config.Zookeepers
	this.ZkPath = config.ZkPath
	this.Consumer = nil

	cgConfig := consumergroup.NewConfig()
	cgConfig.Offsets.ProcessingTimeout = 10 * time.Second
	cgConfig.Offsets.Initial = sarama.OffsetNewest
	if len(this.ZkPath) > 0 {
		cgConfig.Zookeeper.Chroot = this.ZkPath
	}

	cgName := this.getGroupName()
	consumer, err := consumergroup.JoinConsumerGroup(cgName, this.Topics, this.Zookeeper, cgConfig)
	if err != nil {
		glog.Fatalf("Failed to join consumer group for url[%s], %s", this.Callback.Url, err.Error())
		return err
	} else {
		glog.V(1).Infof("Join consumer group for url[%s] with UUID[%s]", this.Callback.Url, cgName)
	}

	this.Consumer = consumer
	return nil
}

func (this *Worker) getGroupName() string {
	m := md5.New()
	m.Write([]byte(this.Callback.Url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}

func (this *Worker) Work() {

	consumer := this.Consumer

	go func() {
		for err := range consumer.Errors() {
			glog.Errorln("Error working consumers", err)
		}
	}()

	eventCount := 0
	offsets := make(map[string]map[int32]int64)

	for message := range consumer.Messages() {
		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		eventCount += 1
		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset-1 {
			glog.Errorf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1, message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
		}

		msg := CreateMsg(message)
		glog.V(2).Infof("received message,[topic:%s][partition:%d][offset:%d]", msg.Topic, msg.Partition, msg.Offset)

		deliverySuccessed := false
		retry_times := 0
		for {
			for !deliverySuccessed && retry_times < this.Callback.RetryTimes {
				deliverySuccessed, _ = this.delivery(msg, retry_times)
				if !deliverySuccessed {
					retry_times++
				} else {
					break
				}
			}

			if deliverySuccessed {
				break
			}

			if this.Callback.BypassFailed {
				glog.Errorf("tried to delivery message [url:%s][topic:%s][partition:%d][offset:%d] for %d times and all failed. BypassFailed is :%t ,will not retry", this.Callback.Url, msg.Topic, msg.Partition, msg.Offset, retry_times, this.Callback.BypassFailed)
				break
			} else {
				glog.Errorf("tried to delivery message [url:%s][topic:%s][partition:%d][offset:%d] for %d times and all failed. BypassFailed is :%t ,sleep %s to retry", this.Callback.Url, msg.Topic, msg.Partition, msg.Offset, retry_times, this.Callback.BypassFailed, this.Callback.FailedSleep)
				time.Sleep(this.Callback.FailedSleep)
				retry_times = 0
			}
		}

		offsets[message.Topic][message.Partition] = message.Offset
		consumer.CommitUpto(message)
		glog.Infof("commited message,[topic:%s][partition:%d][offset:%d]", msg.Topic, msg.Partition, msg.Offset)

	}

}

func (this *Worker) delivery(msg *Msg, retry_times int) (success bool, err error) {
	glog.V(2).Infof("delivery message,[url:%s][retry_times:%d][topic:%s][partition:%d][offset:%d]", this.Callback.Url, retry_times, msg.Topic, msg.Partition, msg.Offset)

	client := &http.Client{}
	client.Timeout = this.Callback.Timeout

	type RMSG struct {
		Topic        string `json:"Topic"`
		PartitionKey string `json:"PartitionKey"`
		TimeStamp    int    `json:"TimeStamp"`
		Data         string `json:"Data"`
	}
	var rmsg RMSG
	json.Unmarshal(msg.Value, &rmsg)
	req, _ := http.NewRequest("POST", this.Callback.Url, ioutil.NopCloser(strings.NewReader(rmsg.Data)))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	req.Header.Set("User-Agent", "Taiji pusher consumer(go)/v"+VERSION)
	req.Header.Set("X-Retry-Times", fmt.Sprintf("%d", retry_times))
	req.Header.Set("X-Kmq-Topic", msg.Topic)
	req.Header.Set("X-Kmq-Partition", fmt.Sprintf("%d", msg.Partition))
	req.Header.Set("X-Kmq-Partition-Key", rmsg.PartitionKey)
	req.Header.Set("X-Kmq-Timestamp", fmt.Sprintf("%d", rmsg.TimeStamp))
	resp, err := client.Do(req)
	suc := true
	if nil == err {
		defer resp.Body.Close()
		suc = (resp.StatusCode == 200)
	} else {
		glog.Errorf("delivery failed,[retry_times:%d][topic:%s][partition:%d][offset:%d][error:%s]", retry_times, msg.Topic, msg.Partition, msg.Offset, err.Error())
		suc = false
	}
	return suc, err
}

func (this *Worker) Closed() bool {
	return this.Consumer.Closed()
}

func (this *Worker) Close() {
	if err := this.Consumer.Close(); err != nil {
		glog.Errorln("Error closing consumers", err)
	}
}
