package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/wvanbergen/kafka/consumergroup"
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

func NewWorker(callback *WorkerCallback, topics []string, zookeeper []string, zkPath string) *Worker {
	return &Worker{
		Callback:  callback,
		Topics:    topics,
		Zookeeper: zookeeper,
		ZkPath:    zkPath,
		Consumer:  nil,
	}
}

func (this *Worker) Init() error {
	config := consumergroup.NewConfig()
	config.Offsets.ProcessingTimeout = 10 * time.Second
	config.Offsets.Initial = sarama.OffsetNewest
	if len(this.ZkPath) > 0 {
		config.Zookeeper.Chroot = this.ZkPath
	}

	consumerGroup := this.getGroupName()
	topics := this.Topics
	zookeeper := this.Zookeeper

	consumer, consumerErr := consumergroup.JoinConsumerGroup(consumerGroup, topics, zookeeper, config)
	if consumerErr != nil {
		return consumerErr
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
			log.Println(err)
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
			log.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition]+1, message.Offset, message.Offset-offsets[message.Topic][message.Partition]+1)
		}

		msg := CreateMsg(message)
		log.Printf("received message,[topic:%s][partition:%d][offset:%d]", msg.Topic, msg.Partition, msg.Offset)

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
				log.Printf("tried to delivery message [url:%s][topic:%s][partition:%d][offset:%d] for %d times and all failed. BypassFailed is :%t ,will not retry", this.Callback.Url, msg.Topic, msg.Partition, msg.Offset, retry_times, this.Callback.BypassFailed)
				break
			} else {
				log.Printf("tried to delivery message [url:%s][topic:%s][partition:%d][offset:%d] for %d times and all failed. BypassFailed is :%t ,sleep %s to retry", this.Callback.Url, msg.Topic, msg.Partition, msg.Offset, retry_times, this.Callback.BypassFailed, this.Callback.FailedSleep)
				time.Sleep(this.Callback.FailedSleep)
				retry_times = 0
			}
		}

		offsets[message.Topic][message.Partition] = message.Offset
		consumer.CommitUpto(message)
		log.Printf("commited message,[topic:%s][partition:%d][offset:%d]", msg.Topic, msg.Partition, msg.Offset)

	}

}

func (this *Worker) delivery(msg *Msg, retry_times int) (success bool, err error) {
	log.Printf("delivery message,[url:%s][retry_times:%d][topic:%s][partition:%d][offset:%d]", this.Callback.Url, retry_times, msg.Topic, msg.Partition, msg.Offset)
	v := url.Values{}

	v.Set("_topic", msg.Topic)
	v.Set("_key", fmt.Sprintf("%s", msg.Key))
	v.Set("_offset", fmt.Sprintf("%d", msg.Offset))
	v.Set("_partition", fmt.Sprintf("%d", msg.Partition))
	v.Set("message", fmt.Sprintf("%s", msg.Value))

	body := ioutil.NopCloser(strings.NewReader(v.Encode()))
	client := &http.Client{}
	client.Timeout = this.Callback.Timeout
	req, _ := http.NewRequest("POST", this.Callback.Url, body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	req.Header.Set("User-Agent", "Taiji pusher consumer(go)/v"+VERSION)
	req.Header.Set("X-Retry-Times", fmt.Sprintf("%d", retry_times))
	resp, err := client.Do(req)
	suc := true
	if nil == err {
		defer resp.Body.Close()
		suc = (resp.StatusCode == 200)
	} else {
		log.Printf("delivery failed,[retry_times:%d][topic:%s][partition:%d][offset:%d][error:%s]", retry_times, msg.Topic, msg.Partition, msg.Offset, err.Error())
		suc = false
	}
	return suc, err
}

func (this *Worker) Closed() bool {
	return this.Consumer.Closed()
}

func (this *Worker) Close() {
	if err := this.Consumer.Close(); err != nil {
		sarama.Logger.Println("Error closing the consumer", err)
	}
}
