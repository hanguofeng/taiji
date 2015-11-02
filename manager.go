package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"time"

	"github.com/golang/glog"
)

type Manager struct {
	Topic             string
	Group             string
	Url               string
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
	this.Group = this.getGroupName()

	for i := 0; i < config.WorkerNum; i++ {
		worker := NewWorker()
		if err := worker.Init(config); err != nil {
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
			worker.Init(this.config)
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

func (this *Manager) Restart() {
	this.Close()
	this.checkAndRestart()
}

func (this *Manager) Find(partition int) (*Worker, error) {
	for _, worker := range this.workers {
		if offset, err := worker.Consumer.OffsetManager().Offsets(this.Topic); err != nil {
			continue
		} else {
			if _, ok := offset[int32(partition)]; ok {
				return worker, nil
			}
		}
	}

	return nil, errors.New("Worker not found")
}

func (this *Manager) getGroupName() string {
	m := md5.New()
	m.Write([]byte(this.Url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}
