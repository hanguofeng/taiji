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

type PusherWorkerCallback struct {
	url         string
	retry_times int
	timeout     time.Duration
}
type PusherWorker struct {
	callback  *PusherWorkerCallback
	topics    []string
	zookeeper []string
	zkPath    string
	consumer  *consumergroup.ConsumerGroup
}

func CreatePusherWorker(callback *PusherWorkerCallback, topics []string, zookeeper []string, zkPath string) *PusherWorker {
	worker := new(PusherWorker)
	worker.callback = callback
	worker.topics = topics
	worker.zookeeper = zookeeper
	worker.zkPath = zkPath
	return worker
}

func (this *PusherWorker) init() error {

	config := consumergroup.NewConfig()
	config.Offsets.ProcessingTimeout = 10 * time.Second
	config.Offsets.Initial = sarama.OffsetNewest
	if len(this.zkPath) > 0 {
		config.Zookeeper.Chroot = this.zkPath
	}

	consumerGroup := this.getGroupName()
	topics := this.topics
	zookeeper := this.zookeeper

	consumer, consumerErr := consumergroup.JoinConsumerGroup(consumerGroup, topics, zookeeper, config)
	if consumerErr != nil {
		return consumerErr
	}

	this.consumer = consumer

	return nil
}

func (this *PusherWorker) getGroupName() string {
	m := md5.New()
	m.Write([]byte(this.callback.url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}

func (this *PusherWorker) work() {

	consumer := this.consumer

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
		log.Printf("received message,[partition:%d][offset:%d][value:%s]", msg.Partition, msg.Offset, msg.Value)

		deliverySuccessed := false
		retry_times := 0
		for !deliverySuccessed && retry_times < this.callback.retry_times {
			deliverySuccessed, _ = this.delivery(msg, retry_times)
			if !deliverySuccessed {
				retry_times++
			}
		}

		offsets[message.Topic][message.Partition] = message.Offset
		consumer.CommitUpto(message)
		log.Printf("commited message,[partition:%d][offset:%d][value:%s]", msg.Partition, msg.Offset, msg.Value)

	}

}

func (this *PusherWorker) delivery(msg *Msg, retry_times int) (success bool, err error) {
	log.Printf("delivery message,[retry_times:%d][partition:%d][offset:%d][value:%s]", retry_times, msg.Partition, msg.Offset, msg.Value)
	v := url.Values{}

	v.Set("_topic", msg.Topic)
	v.Set("_key", fmt.Sprintf("%s", msg.Key))
	v.Set("_offset", fmt.Sprintf("%d", msg.Offset))
	v.Set("_partition", fmt.Sprintf("%d", msg.Partition))
	v.Set("message", fmt.Sprintf("%s", msg.Value))

	body := ioutil.NopCloser(strings.NewReader(v.Encode()))
	client := &http.Client{}
	client.Timeout = this.callback.timeout
	req, _ := http.NewRequest("POST", this.callback.url, body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	req.Header.Set("User-Agent", "Taiji pusher consumer(go)/v"+VERSION)
	req.Header.Set("X-Retry-Times", fmt.Sprintf("%d", retry_times))
	resp, err := client.Do(req)
	defer resp.Body.Close()
	suc := (resp.StatusCode == 200)
	return suc, err
}

func (this *PusherWorker) close() {
	if err := this.consumer.Close(); err != nil {
		sarama.Logger.Println("Error closing the consumer", err)
	}
}
