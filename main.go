package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"gopkg.in/Shopify/sarama.v1"
)

const (
	VERSION = "1.0.0"
)

var (
	configFile     = flag.String("c", "config.json", "the config file")
	showVersion    = flag.Bool("v", false, "show version")
	testConfigMode = flag.Bool("t", false, "test config")
)

func main() {

	flag.Parse()

	if true == *showVersion {
		fmt.Printf("taiji v%s \n", VERSION)
		flag.Usage()
		os.Exit(0)
	}

	config, err := loadConfig(*configFile)

	if nil != err {
		log.Fatalf("load config err:%s", err.Error())
	}

	if true == *testConfigMode {
		fmt.Println("config test ok")
		fmt.Printf("config:%v\n", config)
		os.Exit(0)
	}

	if len(config.LogFile) > 0 {
		os.MkdirAll(filepath.Dir(config.LogFile), 0777)
		f, err := os.OpenFile(config.LogFile, os.O_RDWR|os.O_CREATE, 0666)
		if nil != err {
			log.Fatalf("write log failed")
		}
		log.SetOutput(f)
		sarama.Logger = log.New(f, "[Sarama] ", log.LstdFlags)
	} else {
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	}

	manager := CreatePusherWorkerManager()

	var callbackConfig CallbackItemConfig
	for _, callbackConfig = range config.Callbacks {
		callback := new(PusherWorkerCallback)
		callback.retry_times = callbackConfig.RetryTimes
		callback.url = callbackConfig.Url
		callback.timeout, err = time.ParseDuration(callbackConfig.Timeout)
		if nil != err {
			callback.timeout = time.Second
			log.Printf("callback config timeout error,using default.config value:%s", callbackConfig.Timeout)
		}
		kafkaTopics := callbackConfig.Topics
		zookeeper := callbackConfig.Zookeepers
		zkPath := callbackConfig.ZkPath

		worker := CreatePusherWorker(callback, kafkaTopics, zookeeper, zkPath)
		manager.AddWorker(worker)
	}

	err = manager.InitAll()

	if nil != err {
		log.Fatalf("init all error:%s", err.Error())
	}

	manager.WorkAll()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-c:
		log.Print("catch exit signal")
		manager.CloseAll()
		log.Print("exit done")
	}

}
