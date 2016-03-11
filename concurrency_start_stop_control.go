package main

import "sync"

type ConcurrencyStartStopControl struct {
	*StartStopControl
	instanceCount uint
	instanceLock  sync.Mutex
}

func NewConcurrencyStartStopControl() *ConcurrencyStartStopControl {
	return &ConcurrencyStartStopControl{
		StartStopControl: NewStartStopControl(),
	}
}

func (cssc *ConcurrencyStartStopControl) markStart() {
	cssc.instanceLock.Lock()
	defer cssc.instanceLock.Unlock()
	cssc.instanceCount++
	if !cssc.Running() {
		// already start
		cssc.StartStopControl.markStart()
	}
}

func (cssc *ConcurrencyStartStopControl) markStop() {
	cssc.instanceLock.Lock()
	defer cssc.instanceLock.Unlock()
	cssc.instanceCount--
	if cssc.instanceCount == 0 {
		cssc.StartStopControl.markStop()
	}
}

func (cssc *ConcurrencyStartStopControl) markReady() {
	cssc.instanceLock.Lock()
	defer cssc.instanceLock.Unlock()
	select {
	case <-cssc.ReadyChannel():
	default:
		cssc.StartStopControl.markReady()
	}
}
