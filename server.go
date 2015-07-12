package main

import (
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"gopkg.in/Shopify/sarama.v1"
)

type Server struct {
	managers []*Manager
}

func NewServer() *Server {
	return &Server{}
}

func (this *Server) Init(configFile string) error {
	runtime.GOMAXPROCS(runtime.NumCPU())

	config, err := loadConfig(configFile)
	if err != nil {
		log.Fatalf("Load Config err: %s", err.Error())
		return err
	}

	// init log
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

	for _, callbackConfig := range config.Callbacks {
		log.Println(callbackConfig)
		manager := NewManager()
		if e := manager.Init(&callbackConfig); e != nil {
			log.Fatalf("Init manager for url[%s] failed, %s", callbackConfig.Url, e.Error())
			return e
		}
		this.managers = append(this.managers, manager)
	}

	return nil
}

func (this *Server) Run() error {
	for _, mgr := range this.managers {
		mgr.Work()
	}
	log.Println("managers get to work!")

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-c:
		log.Print("catch exit signal")
		for _, mgr := range this.managers {
			mgr.Close()
		}
		log.Print("exit done")
	}

	return nil
}
