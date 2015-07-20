package main

import (
	"log"
	"os"
	"os/signal"
	//"path/filepath"
	"runtime"
	"syscall"

	"github.com/golang/glog"
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
		glog.Fatalf("Load Config err: %s", err.Error())
		return err
	}

	// init sarama logger
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	for _, callbackConfig := range config.Callbacks {
		glog.Infoln(callbackConfig)
		manager := NewManager()
		if e := manager.Init(&callbackConfig); e != nil {
			glog.Fatalf("Init manager for url[%s] failed, %s", callbackConfig.Url, e.Error())
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
	glog.Info("managers get to work!")

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-c:
		glog.Info("catch exit signal")
		for _, mgr := range this.managers {
			mgr.Close()
		}
		glog.Info("exit done")
	}

	return nil
}
