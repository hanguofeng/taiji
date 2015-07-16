package main

import (
	"log"
	"time"
)

type Manager struct {
	workers           []*Worker
	superviseInterval time.Duration
}

func NewManager() *Manager {
	return &Manager{
		superviseInterval: 20,
	}
}

func (this *Manager) Init(config *CallbackItemConfig) error {
	log.Printf("Init worker success. %v", config)

	for i := 0; i < config.WorkerNum; i++ {
		worker := NewWorker()
		if err := worker.Init(config); err != nil {
			log.Fatalf("Init worker for url[%s] failed, %s", config.Url, err.Error())
			return err
		}
		log.Println("Init worker success.")

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
	log.Printf("checking workers begin...")
	for _, worker := range this.workers {
		if worker.Closed() {
			//worker.Init()
			worker.Work()
			log.Printf("found worker closed,already restarted")
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
