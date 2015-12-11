package main

import (
	"errors"
	"time"
)

type StartStopControl struct {
	ready   chan struct{}
	stopper chan struct{}
	stopped chan struct{}
}

func (ssc *StartStopControl) ensureStart() error {
	if ssc.Running() {
		return errors.New("Service already started")
	}

	ssc.markStart()

	return nil
}

func (ssc *StartStopControl) markStart() {
	ssc.stopper = make(chan struct{})
	ssc.stopped = make(chan struct{})
}

func (ssc *StartStopControl) markStop() {
	ssc.markClosing()

	if ssc.stopped != nil {
		select {
		case <-ssc.stopped:
		default:
			close(ssc.stopped)
		}
	}
}

func (ssc *StartStopControl) markClosing() {
	if ssc.stopper != nil {
		select {
		case <-ssc.stopper:
		default:
			close(ssc.stopper)
		}
	}
}

func (ssc *StartStopControl) WaitForClose() {
	select {
	case <-ssc.WaitForCloseChannel():
	}
}

func (ssc *StartStopControl) WaitForExit() {
	select {
	case <-ssc.WaitForExitChannel():
	}
}

func (ssc *StartStopControl) WaitForCloseChannel() <-chan struct{} {
	// TODO, use atomic semaphore wait
	if ssc.stopper != nil {
		time.Sleep(1 * time.Second)
	}
	return ssc.stopper
}

func (ssc *StartStopControl) WaitForExitChannel() <-chan struct{} {
	// TODO, use atomic semaphore wait
	if ssc.stopped != nil {
		time.Sleep(1 * time.Second)
	}
	return ssc.stopped
}

func (ssc *StartStopControl) Close() error {
	ssc.AsyncClose()
	if !ssc.Closed() {
		ssc.WaitForExit()
	}
	return nil
}

func (ssc *StartStopControl) AsyncClose() error {
	if !ssc.Running() {
		return errors.New("Service is not running")
	}

	ssc.markClosing()
	return nil
}

func (ssc *StartStopControl) Closed() bool {
	if ssc.stopped == nil || ssc.stopper == nil {
		return true
	}

	select {
	case <-ssc.stopped:
		return true
	default:
		return false
	}
}

func (ssc *StartStopControl) Closing() bool {
	if ssc.Closed() {
		return false
	}

	select {
	case <-ssc.stopper:
		return true
	default:
		return false
	}
}

func (ssc *StartStopControl) Running() bool {
	return !ssc.Closed() && !ssc.Closing()
}

func (ssc *StartStopControl) initReady() {
	ssc.ready = make(chan struct{})
}

func (ssc *StartStopControl) markReady() {
	if ssc.ready != nil {
		select {
		case <-ssc.ready:
		default:
			close(ssc.ready)
		}
	}
}

func (ssc *StartStopControl) Ready() error {
	select {
	case <-ssc.ReadyChannel():
	case <-ssc.WaitForExitChannel():
		return errors.New("Service stopped")
	}

	return nil
}

func (ssc *StartStopControl) ReadyChannel() <-chan struct{} {
	// TODO, use atomic semaphore wait
	for ssc.ready == nil {
		time.Sleep(1 * time.Second)
	}

	return ssc.ready
}
