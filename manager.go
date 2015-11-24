package main

import (
	"errors"
	"time"

	"github.com/golang/glog"
)

type Manager struct {
	Topic             string
	Group             string
	Url               string
	coordinator       *Coordinator
	workers           []*Worker
	superviseInterval time.Duration
	config            *CallbackItemConfig
}

func NewManager() *Manager {
	return &Manager{
		superviseInterval: 5,
	}
}

func (this *Manager) Init(config *CallbackItemConfig) error {
	this.config = config
	this.Topic = config.Topics[0]
	this.Url = config.Url
	this.Group = getGroupName(this.Url)
	this.coordinator = NewCoordinator()

	if err := this.coordinator.Init(config); err != nil {
		glog.Fatalf("Init coordinator for url[%v] failed, %v", config.Url, err.Error())
		return err
	}

	for i := 0; i < config.WorkerNum; i++ {
		worker := NewWorker()
		if err := worker.Init(config, this.coordinator); err != nil {
			glog.Fatalf("Init worker for url[%v] failed, %v", config.Url, err.Error())
			return err
		}
		glog.V(1).Info("Init worker success.")

		this.workers = append(this.workers, worker)
	}
	this.Supervise()
	glog.V(1).Infoln("[Pusher]Init manager success. %v", config)
	return nil
}

func (this *Manager) Work() error {
	go this.coordinator.Work()
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
	if this.coordinator.Closed() {
		this.coordinator.Init(this.config)
		for _, worker := range this.workers {
			worker.Init(this.config, this.coordinator)
		}
		glog.V(1).Info("found coordinator closed, already restarted")
	}
	return nil
}

func (this *Manager) Close() error {
	this.coordinator.Close()
	return nil
}

func (this *Manager) Restart() {
	this.Close()
	this.checkAndRestart()
}

func (this *Manager) Find(partition int) (*Coordinator, error) {
	if offset, err := this.coordinator.GetConsumer().OffsetManager().Offsets(this.Topic); err == nil {
		if _, ok := offset[int32(partition)]; ok {
			return this.coordinator, nil
		}
	}

	return nil, errors.New("Worker not found")
}
