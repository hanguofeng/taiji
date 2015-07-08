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

func (this *Manager) AddWorker(worker *Worker) {
	this.workers = append(this.workers, worker)
}

func (this *Manager) InitAll() error {
	for _, worker := range this.workers {
		if err := worker.Init(); err != nil {
			return err
		}
	}
	return nil
}

func (this *Manager) WorkAll() error {
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
			this.CheckAndRestart()
			time.Sleep(this.superviseInterval * time.Second)
		}
	}()
}

func (this *Manager) CheckAndRestart() error {
	log.Printf("checking workers begin...")
	for _, worker := range this.workers {
		if worker.Closed() {
			worker.Init()
			worker.Work()
			log.Printf("found worker closed,already restarted")
		}
	}
	log.Printf("checking workers done")
	return nil
}

func (this *Manager) CloseAll() error {
	for _, worker := range this.workers {
		worker.Close()
	}

	return nil
}
