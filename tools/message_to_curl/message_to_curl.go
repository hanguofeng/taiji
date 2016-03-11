package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
)

var (
	zkAddr      string
	topic       string
	partition   int
	offset      int64
	serializer  string
	contentType string
	callback    string
)

type MessageBody struct {
	Topic        string `json:"Topic"`
	PartitionKey string `json:"PartitionKey"`
	TimeStamp    int64  `json:"TimeStamp"`
	Data         string `json:"Data"`
	LogId        string `json:"LogId"`
	ContentType  string `json:"ContentType"`
}

func init() {
	flag.StringVar(&zkAddr, "zk", "127.0.0.1:2181", "zookeeper address which kafka belongs to")
	flag.StringVar(&topic, "topic", "", "topic name")
	flag.IntVar(&partition, "partition", -1, "partition id")
	flag.Int64Var(&offset, "offset", -1, "offset number")
	flag.StringVar(&serializer, "serializer", "raw", "message serializer name")
	flag.StringVar(&contentType, "type", "", "message content type")
	flag.StringVar(&callback, "callback", "http://url_to_replace", "callback url to build")
	zk.DefaultLogger = log.New(ioutil.Discard, "[Zookeeper] ", log.LstdFlags)
}

func escapeShellArg(arg string) string {
	return "'" + strings.Replace(arg, "'", "'\\''", -1) + "'"
}

func main() {
	flag.Parse()

	if topic == "" {
		panic("topic should not be empty")
	}

	if partition < 0 {
		panic("partition should be specified")
	}

	if offset < 0 {
		panic("offset should be specified")
	}

	kazooConf := kazoo.NewConfig()
	kazooInstance, err := kazoo.NewKazooFromConnectionString(zkAddr, kazooConf)
	if err != nil {
		panic(fmt.Sprintf("connect zookeeper failed: %v", err))
	}
	defer kazooInstance.Close()

	brokers, err := kazooInstance.BrokerList()
	if err != nil {
		panic(fmt.Sprintf("read broker list from zookeeper failed: %v", err))
	}

	saramaConfig := sarama.NewConfig()
	saramaConsumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("connect to kafka failed: %v", err))
	}
	defer saramaConsumer.Close()

	partitionConsumer, err := saramaConsumer.ConsumePartition(topic, int32(partition), offset)
	if err != nil {
		panic(fmt.Sprintf("consume message failed, could not get partition consumer: %v", err))
	}
	defer partitionConsumer.Close()

	var message *sarama.ConsumerMessage

	select {
	case message = <-partitionConsumer.Messages():
	case <-time.After(10 * time.Second):
		panic("message not received after 10 seconds")
	}

	var messageBody MessageBody

	// got message
	switch serializer {
	case "raw":
		messageBody.Data = string(message.Value)
	case "json":
		fallthrough
	default:
		json.Unmarshal(message.Value, &messageBody)
	}

	if contentType != "" {
		messageBody.ContentType = contentType
	} else if messageBody.ContentType == "" {
		messageBody.ContentType = "application/x-www-form-urlencoded"
	}

	// build curl command
	var curlCommand []string

	curlCommand = append(curlCommand, "curl", escapeShellArg(callback))

	// append headers
	curlCommand = append(curlCommand, "-H", escapeShellArg(fmt.Sprintf("Content-Type: %s", messageBody.ContentType)))
	curlCommand = append(curlCommand, "-H", escapeShellArg("User-Agent: Taiji pusher consumer(go)/v2.0.0-simulator"))
	curlCommand = append(curlCommand, "-H", escapeShellArg("X-Retry-Times: 0"))
	curlCommand = append(curlCommand, "-H", escapeShellArg(fmt.Sprintf("X-Kmq-Topic: %s", message.Topic)))
	curlCommand = append(curlCommand, "-H", escapeShellArg(fmt.Sprintf("X-Kmq-Partition: %d", message.Partition)))
	curlCommand = append(curlCommand, "-H", escapeShellArg(fmt.Sprintf("X-Kmq-Partition-Key: %s", messageBody.PartitionKey)))
	curlCommand = append(curlCommand, "-H", escapeShellArg(fmt.Sprintf("X-Kmq-Offset: %d", message.Offset)))
	curlCommand = append(curlCommand, "-H", escapeShellArg(fmt.Sprintf("X-Kmq-Logid: %s", messageBody.LogId)))
	curlCommand = append(curlCommand, "-H", escapeShellArg(fmt.Sprintf("X-Kmq-Timestamp: %d", messageBody.TimeStamp)))
	curlCommand = append(curlCommand, "-H", escapeShellArg("Meilishuo uid:0;ip:0.0.0.0;v:0;master:0"))

	// append body
	curlCommand = append(curlCommand, "--data", escapeShellArg(messageBody.Data))

	fmt.Println(strings.Join(curlCommand, " "))
}
