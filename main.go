package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"gopkg.in/Shopify/sarama.v1"
)

const (
	VERSION = "1.0.0"
)

var (
	configFile string
	version    bool
	testMode   bool
)

func init() {
	flag.StringVar(&configFile, "c", "config.json", "the config file")
	flag.BoolVar(&version, "v", false, "show version")
	flag.BoolVar(&testMode, "t", false, "test config")
}

func getVersion() string {
	return VERSION
}

func showVersion() {
	fmt.Println(getVersion())
	flag.Usage()
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	if version {
		showVersion()
		os.Exit(0)
	}

	config, err := loadConfig(configFile)
	if nil != err {
		log.Fatalf("load config err:%s", err.Error())
	}

	if testMode {
		fmt.Println("config test ok")
		fmt.Printf("config:%#v\n", config)
		os.Exit(0)
	}

	if len(config.LogFile) > 0 {
		os.MkdirAll(filepath.Dir(config.LogFile), 0777)
		f, err := os.OpenFile(config.LogFile, os.O_RDWR|os.O_CREATE, 0666)
		defer f.Close()
		if nil != err {
			log.Fatalf("write log failed")
		}
		log.SetOutput(f)
		sarama.Logger = log.New(f, "[Sarama] ", log.LstdFlags)
	} else {
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	}

	manager := NewManager()

	var callbackConfig CallbackItemConfig
	for _, callbackConfig = range config.Callbacks {
		callback := &WorkerCallback{
			RetryTimes:   callbackConfig.RetryTimes,
			Url:          callbackConfig.Url,
			Timeout:      callbackConfig.Timeout,
			BypassFailed: callbackConfig.BypassFailed,
			FailedSleep:  callbackConfig.FailedSleep,
		}
		kafkaTopics := callbackConfig.Topics
		zookeeper := callbackConfig.Zookeepers
		zkPath := callbackConfig.ZkPath

		worker := NewWorker(callback, kafkaTopics, zookeeper, zkPath)
		manager.AddWorker(worker)
	}

	err = manager.InitAll()

	if nil != err {
		log.Fatalf("init all error:%s", err.Error())
	}

	manager.WorkAll()
	manager.Supervise()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-c:
		log.Print("catch exit signal")
		manager.CloseAll()
		log.Print("exit done")
	}

}
