package main

import (
	"errors"
	"time"
)

type StartStopControl struct {
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
	if ssc.stopper != nil {
		select {
		case <-ssc.stopper:
		}
	}
}

func (ssc *StartStopControl) WaitForExit() {
	if ssc.stopped != nil {
		select {
		case <-ssc.stopped:
		}
	}
}

func (ssc *StartStopControl) WaitForCloseChannel() <-chan struct{} {
	return ssc.stopper
}

func (ssc *StartStopControl) WaitForExitChannel() <-chan struct{} {
	return ssc.stopped
}

func (ssc *StartStopControl) Close() error {
	ssc.AsyncClose()
	ssc.WaitForExit()
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
