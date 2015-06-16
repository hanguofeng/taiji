package main

import (
	"log"
)

type PusherWorkerManager struct {
	workers []*PusherWorker
}

func CreatePusherWorkerManager() *PusherWorkerManager {
	m := new(PusherWorkerManager)
	return m
}

func (this *PusherWorkerManager) AddWorker(worker *PusherWorker) {
	this.workers = append(this.workers, worker)
}

func (this *PusherWorkerManager) InitAll() error {
	for _, worker := range this.workers {
		err := worker.init()
		if nil != err {
			return err
		}
	}

	return nil
}

func (this *PusherWorkerManager) WorkAll() error {

	for _, worker := range this.workers {
		if nil != worker.Consumer {
			go worker.work()
		}
	}

	return nil
}

func (this *PusherWorkerManager) CheckAndRestart() error {

	log.Printf("checking workers begin...")
	for _, worker := range this.workers {
		if worker.closed() {
			worker.init()
			worker.work()
			log.Printf("found worker closed,already restarted")
		}
	}
	log.Printf("checking workers done")

	return nil
}

func (this *PusherWorkerManager) CloseAll() error {
	for _, worker := range this.workers {
		worker.close()
	}

	return nil
}
