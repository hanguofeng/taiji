package main

import (
	"errors"
	"time"

	"github.com/golang/glog"
	"net"
	"net/http"
)

type Manager struct {
	Topic             string
	Group             string
	Url               string
	httpTransport     http.RoundTripper
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
	this.httpTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConnsPerHost: config.ConnectionPoolSize,
	}

	for i := 0; i < config.WorkerNum; i++ {
		worker := NewWorker()
		if err := worker.Init(config, this.httpTransport); err != nil {
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
			worker.Init(this.config, this.httpTransport)
			go worker.Work()
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
