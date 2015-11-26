package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/crask/kafka/consumergroup"
	"github.com/golang/glog"
)

type Worker struct {
	Callback    *WorkerCallback
	Consumer    *consumergroup.ConsumerGroup
	Serializer  string
	ContentType string
	Transport   http.RoundTripper
}

type RMSG struct {
	Topic        string `json:"Topic"`
	PartitionKey string `json:"PartitionKey"`
	TimeStamp    int64  `json:"TimeStamp"`
	Data         string `json:"Data"`
	LogId        string `json:"LogId"`
	ContentType  string `json:"ContentType"`
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

func (this *Worker) Init(config *CallbackItemConfig, coordinator *Coordinator, transport http.RoundTripper) error {
	this.Callback = &WorkerCallback{
		Url:          config.Url,
		RetryTimes:   config.RetryTimes,
		Timeout:      config.Timeout,
		BypassFailed: config.BypassFailed,
		FailedSleep:  config.FailedSleep,
	}
	this.Consumer = nil
	this.Serializer = config.Serializer
	this.ContentType = config.ContentType
	this.Transport = transport

	cgConfig := consumergroup.NewConfig()
	cgConfig.Offsets.ProcessingTimeout = 10 * time.Second
	cgConfig.Offsets.CommitInterval = time.Duration(commitInterval) * time.Second
	cgConfig.Offsets.Initial = sarama.OffsetNewest

	this.Consumer = coordinator.GetConsumer()
	return nil
}

func (this *Worker) Work() {
	var tsrpc, terpc time.Time

	consumer := this.Consumer

	go func() {
		for err := range consumer.Errors() {
			glog.Errorln("Error working consumers", err)
		}
	}()

	for message := range consumer.Messages() {
		msg := CreateMsg(message)
		glog.V(2).Infof("received message,[topic:%s][partition:%d][offset:%d]", msg.Topic, msg.Partition, msg.Offset)

		deliverySuccessed := false
		retry_times := 0
		tsrpc = time.Now()
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
		terpc = time.Now()

		consumer.CommitUpto(message)
		glog.Infof("commited message,[topic:%s][partition:%d][offset:%d][url:%s][cost:%vms]", msg.Topic, msg.Partition, msg.Offset, this.Callback.Url, fmt.Sprintf("%.2f", terpc.Sub(tsrpc).Seconds()*1000))

	}

}

func (this *Worker) delivery(msg *Msg, retry_times int) (success bool, err error) {
	var tsrpc, terpc time.Time

	client := &http.Client{Transport: this.Transport}
	client.Timeout = this.Callback.Timeout

	var rmsg RMSG

	switch this.Serializer {
	case "":
		fallthrough
	case "raw":
		rmsg.Data = string(msg.Value)
	case "json":
		fallthrough
	default:
		json.Unmarshal(msg.Value, &rmsg)
	}

	if this.ContentType != "" {
		rmsg.ContentType = this.ContentType
	} else if rmsg.ContentType == "" {
		rmsg.ContentType = "application/x-www-form-urlencoded"
	}

	req, _ := http.NewRequest("POST", this.Callback.Url, ioutil.NopCloser(strings.NewReader(rmsg.Data)))
	req.Header.Set("Content-Type", rmsg.ContentType)
	req.Header.Set("User-Agent", "Taiji pusher consumer(go)/v"+VERSION)
	req.Header.Set("X-Retry-Times", fmt.Sprintf("%d", retry_times))
	req.Header.Set("X-Kmq-Topic", msg.Topic)
	req.Header.Set("X-Kmq-Partition", fmt.Sprintf("%d", msg.Partition))
	req.Header.Set("X-Kmq-Partition-Key", rmsg.PartitionKey)
	req.Header.Set("X-Kmq-Offset", fmt.Sprintf("%d", msg.Offset))
	req.Header.Set("X-Kmq-Logid", fmt.Sprintf("%s", rmsg.LogId))
	req.Header.Set("X-Kmq-Timestamp", fmt.Sprintf("%d", rmsg.TimeStamp))
	req.Header.Set("Meilishuo", "uid:0;ip:0.0.0.0;v:0;master:0")
	tsrpc = time.Now()
	resp, err := client.Do(req)
	terpc = time.Now()
	suc := true
	if nil == err {
		defer resp.Body.Close()
		suc = (resp.StatusCode == 200)
		if suc == false {
			rbody, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				rbody = []byte{}
			}
			glog.Errorf("delivery failed,[retry_times:%d][topic:%s][partition:%d][offset:%d][msg:%s][url:%s][http_code:%d][cost:%vms][response_body:%s]",
				retry_times, msg.Topic, msg.Partition, msg.Offset, rmsg.Data, this.Callback.Url, resp.StatusCode, fmt.Sprintf("%.2f", terpc.Sub(tsrpc).Seconds()*1000), rbody)
		} else {
			// consume all data in response body to enable connection reusing
			discardBody(resp.Body)
		}
	} else {
		glog.Errorf("delivery failed,[retry_times:%d][topic:%s][partition:%d][offset:%d][msg:%s][url:%s][error:%s][cost:%vms]",
			retry_times, msg.Topic, msg.Partition, msg.Offset, rmsg.Data, this.Callback.Url, err.Error(), fmt.Sprintf("%.2f", terpc.Sub(tsrpc).Seconds()*1000))
		suc = false
	}
	glog.V(2).Infof("delivery message,[url:%s][retry_times:%d][topic:%s][partition:%d][offset:%d][cost:%vms][content-type:%s]", this.Callback.Url, retry_times, msg.Topic, msg.Partition, msg.Offset, fmt.Sprintf("%.2f", terpc.Sub(tsrpc).Seconds()*1000), rmsg.ContentType)

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
