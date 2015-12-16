package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

const HTTP_FORM_ENCODING = "application/x-www-form-urlencoded"

type HTTPTransporter struct {
	Callback          *WorkerCallback
	Serializer        string
	ContentType       string
	config            *CallbackItemConfig
	transporterConfig TransporterConfig
	discardBuffer     []byte
	httpClient        *http.Client
	manager           *PartitionManager
}

type MessageBody struct {
	Topic        string `json:"Topic"`
	PartitionKey string `json:"PartitionKey"`
	TimeStamp    int64  `json:"TimeStamp"`
	Data         string `json:"Data"`
	LogId        string `json:"LogId"`
	ContentType  string `json:"ContentType"`
}

func NewHTTPTransporter() Transporter {
	return &HTTPTransporter{}
}

func (ht *HTTPTransporter) Init(config *CallbackItemConfig, transporterConfig TransporterConfig, manager *PartitionManager) error {
	ht.Callback = &WorkerCallback{
		Url:          config.Url,
		RetryTimes:   config.RetryTimes,
		Timeout:      config.Timeout,
		BypassFailed: config.BypassFailed,
		FailedSleep:  config.FailedSleep,
	}
	ht.Serializer = config.Serializer
	ht.ContentType = config.ContentType
	ht.transporterConfig = transporterConfig
	ht.discardBuffer = make([]byte, 4096)
	ht.manager = manager

	// build http client
	ht.httpClient = &http.Client{Transport: GetServer().GetHttpTransport()}
	ht.httpClient.Timeout = ht.Callback.Timeout

	return nil
}

func (ht *HTTPTransporter) Run() error {
	arbiter := ht.manager.GetArbiter()
	messages := arbiter.MessageChannel()
	offsets := arbiter.OffsetChannel()

	for message := range messages {
		glog.V(1).Infof("Recevied message [topic:%s][partition:%d][url:%s][offset:%d]",
			message.Topic, message.Partition, ht.Callback.Url, message.Offset)

		var messageData MessageBody

		// deserialize message
		switch ht.Serializer {
		case "", "raw":
			messageData.Data = string(message.Value)
		case "json":
			fallthrough
		default:
			json.Unmarshal(message.Value, &messageData)
			// ignore message json decode failure
		}

		// delivery Content-Type
		if "" != ht.ContentType {
			messageData.ContentType = ht.ContentType
		} else if "" == ht.ContentType {
			ht.ContentType = HTTP_FORM_ENCODING
		}

		rpcStartTime := time.Now()

		retried := 0

		for {
			deliveryState := false

			for i := 0; i <= ht.Callback.RetryTimes; i++ {
				deliveryState = ht.delivery(&messageData, message, retried)

				if deliveryState {
					// success
					break
				}

				retried++
			}

			if deliveryState {
				// success
				break
			} else if ht.Callback.BypassFailed {
				// failed
				glog.Errorf(
					"Message skipped due to delivery retryTimes exceeded [topic:%s][partition:%d][url:%s][offset:%d][retryTimes:%d][bypassFailed:%t]",
					message.Topic, message.Partition, ht.Callback.Url, message.Offset, ht.Callback.RetryTimes, ht.Callback.BypassFailed)
				break
			}

			glog.Errorf(
				"Retry delivery after %s due to delivery retryTime exceeded [topic:%s][partition:%d][url:%s][offset:%d][retryTimes:%d][bypassFailed:%t][failedSleep:%.2fms]",
				ht.Callback.FailedSleep.String(), message.Topic, message.Partition, ht.Callback.Url, message.Offset, ht.Callback.RetryTimes, ht.Callback.BypassFailed,
				ht.Callback.FailedSleep.Seconds()*1000)

			// wait for FailedSleep times for another retry round
			time.Sleep(ht.Callback.FailedSleep)
		}

		rpcStopTime := time.Now()

		// total time from proxy to pusher complete sending
		totalTime := float64(-1)
		if ht.Serializer == "json" {
			totalTime = float64(rpcStopTime.UnixNano()/1000000 - messageData.TimeStamp)
		}

		glog.Infof("Committed message [topic:%s][partition:%d][url:%s][offset:%d][cost:%.2fms][totalCost:%.2fms][retried:%d]",
			message.Topic, message.Partition, ht.Callback.Url, message.Offset,
			rpcStopTime.Sub(rpcStartTime).Seconds()*1000,
			totalTime, retried)

		glog.V(1).Infof("HTTP Transporter commit message to arbiter [topic:%s][partition:%d][url:%s][offset:%d]",
			message.Topic, message.Partition, ht.Callback.Url, message.Offset)

		offsets <- message.Offset

		glog.V(1).Infof("HTTP Transporter processed message [topic:%s][partition:%d][url:%s][offset:%d]",
			message.Topic, message.Partition, ht.Callback.Url, message.Offset)
	}

	glog.V(1).Infof("HTTPTransporter exited [topic:%s][partition:%d][url:%s]", ht.manager.Topic, ht.manager.Partition, ht.Callback.Url)

	return nil
}

func (ht *HTTPTransporter) Close() error {
	// dummy
	return nil
}

func (ht *HTTPTransporter) delivery(messageData *MessageBody, message *sarama.ConsumerMessage, retryTime int) bool {
	req, _ := http.NewRequest("POST", ht.Callback.Url, strings.NewReader(messageData.Data))
	req.Header.Set("Content-Type", messageData.ContentType)
	req.Header.Set("User-Agent", "Taiji pusher consumer(go)/v"+VERSION)
	req.Header.Set("X-Retry-Times", fmt.Sprintf("%d", retryTime))
	req.Header.Set("X-Kmq-Topic", message.Topic)
	req.Header.Set("X-Kmq-Partition", fmt.Sprintf("%d", message.Partition))
	req.Header.Set("X-Kmq-Partition-Key", messageData.PartitionKey)
	req.Header.Set("X-Kmq-Offset", fmt.Sprintf("%d", message.Offset))
	req.Header.Set("X-Kmq-Logid", fmt.Sprintf("%s", messageData.LogId))
	req.Header.Set("X-Kmq-Timestamp", fmt.Sprintf("%d", messageData.TimeStamp))
	req.Header.Set("Meilishuo", "uid:0;ip:0.0.0.0;v:0;master:0")

	rpcStartTime := time.Now()
	res, err := ht.httpClient.Do(req)
	rpcStopTime := time.Now()

	rpcTime := rpcStopTime.Sub(rpcStartTime).Seconds() * 1000

	success := false

	if err == nil {
		defer res.Body.Close()

		if 200 == res.StatusCode {
			// success
			success = true
			// discard body
			// better use io.Copy(ioutil.Discard, res.Body), but io.Copy/ioutil.Discard is too slow
			for {
				_, e := res.Body.Read(ht.discardBuffer)
				if e != nil {
					break
				}
			}
		} else {
			// error response code, read body
			responseBody, err := ioutil.ReadAll(res.Body)
			if err != nil {
				responseBody = []byte{}
			}
			// TODO, never let responseBody corrupt my log
			glog.Errorf(
				"Delivery failed [topic:%s][partition:%d][url:%s][offset:%d][retryTime:%d][responseCode:%d][cost:%.2fms][responseBody:%s']",
				message.Topic, message.Partition, ht.Callback.Url, message.Offset, retryTime, res.StatusCode, rpcTime, responseBody)
		}
	} else {
		glog.Errorf(
			"Delivery failed [topic:%s][partition:%d][url:%s][offset:%d][retryTime:%d][cost:%.2fms][err:%s]",
			message.Topic, message.Partition, ht.Callback.Url, message.Offset, retryTime, rpcTime, err.Error())
	}

	return success
}

func init() {
	RegisterTransporter("HTTP", NewHTTPTransporter)
}
