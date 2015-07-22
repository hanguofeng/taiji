package main

import (
	"time"

	"github.com/golang/glog"
)

type Manager struct {
	Topic             string
	Url               string
	workers           []*Worker
	superviseInterval time.Duration
}

func NewManager() *Manager {
	return &Manager{
		superviseInterval: 20,
	}
}

func (this *Manager) Init(config *CallbackItemConfig) error {
	glog.V(1).Infof("Init worker success. %v", config)
	this.Topic = config.Topics[0]
	this.Url = config.Url

	for i := 0; i < config.WorkerNum; i++ {
		worker := NewWorker()
		if err := worker.Init(config); err != nil {
			glog.Fatalf("Init worker for url[%v] failed, %v", config.Url, err.Error())
			return err
		}
		glog.V(1).Info("Init worker success.")

		this.workers = append(this.workers, worker)
	}
	return nil
}

func (this *Manager) Work() error {
	for _, worker := range this.workers {
		if nil != worker.Consumer {
			go worker.Work()
		}
	}
	return nil
}

func (this *Manager) Supervise() {
	go func() {
		for {
			this.checkAndRestart()
			time.Sleep(this.superviseInterval * time.Second)
		}
	}()
}

func (this *Manager) checkAndRestart() error {
	glog.V(1).Info("checking workers begin...")
	for _, worker := range this.workers {
		if worker.Closed() {
			//worker.Init()
			worker.Work()
			glog.V(1).Info("found worker closed,already restarted")
		}
	}
	return nil
}

func (this *Manager) Close() error {
	for _, worker := range this.workers {
		worker.Close()
	}

	return nil
}
