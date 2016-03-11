package main

import (
	"errors"
	"sync"
	"time"
)

type State uint

var (
	WAIT_FOR_STATE_TIMEOUT     = errors.New("Wait for state timeout")
	SERVICE_NOT_RUNNING        = errors.New("Service is not running")
	SERVICE_ALREADY_STARTED    = errors.New("Service already started")
	SERVICE_UNEXPECTED_STOPPED = errors.New("Service unexpected stopped")
)

const (
	PREPARED State = iota
	STARTED
	READY
	CLOSING
	CLOSED
)

type StartStopControl struct {
	// service state
	state     State
	stateLock sync.RWMutex

	// service state triggers
	triggers    map[State]chan struct{}
	triggerLock sync.Mutex
}

func NewStartStopControl() *StartStopControl {
	return &StartStopControl{}
}

func (ssc *StartStopControl) getState() State {
	ssc.stateLock.RLock()
	defer ssc.stateLock.RUnlock()
	return ssc.state
}

func (ssc *StartStopControl) switchState(state State) {
	ssc.stateLock.Lock()
	defer ssc.stateLock.Unlock()
	ssc.removeTrigger(ssc.state)
	ssc.state = state
	ssc.doTrigger(state)
}

func (ssc *StartStopControl) removeTrigger(state State) {
	ssc.triggerLock.Lock()
	defer ssc.triggerLock.Unlock()

	if ssc.triggers == nil {
		ssc.triggers = make(map[State]chan struct{})
	} else {
		_, exists := ssc.triggers[state]

		if exists {
			delete(ssc.triggers, state)
		}
	}
}

func (ssc *StartStopControl) requireTrigger(state State) chan struct{} {
	ssc.triggerLock.Lock()
	defer ssc.triggerLock.Unlock()

	if ssc.triggers == nil {
		ssc.triggers = make(map[State]chan struct{})
	}

	trigger, exists := ssc.triggers[state]

	if !exists {
		trigger = make(chan struct{})
		ssc.triggers[state] = trigger
	}

	return trigger
}

func (ssc *StartStopControl) doTrigger(state State) {
	trigger := ssc.requireTrigger(state)

	select {
	case <-trigger:
	default:
		close(trigger)
	}
}

func (ssc *StartStopControl) waitForState(state State) {
	trigger := ssc.requireTrigger(state)

	select {
	case <-trigger:
	}
}

func (ssc *StartStopControl) waitForStateTimeout(state State, timeout time.Duration) error {
	trigger := ssc.requireTrigger(state)

	select {
	case <-trigger:
		return nil
	case <-time.After(timeout):
		return WAIT_FOR_STATE_TIMEOUT
	}
}

func (ssc *StartStopControl) waitForStateChannel(state State) <-chan struct{} {
	return ssc.requireTrigger(state)
}

func (ssc *StartStopControl) ensureStart() error {
	if ssc.Running() {
		return SERVICE_ALREADY_STARTED
	}

	ssc.markStart()

	return nil
}

func (ssc *StartStopControl) markStart() {
	ssc.switchState(STARTED)
}

func (ssc *StartStopControl) markReady() {
	ssc.switchState(READY)
}

func (ssc *StartStopControl) markClosing() {
	ssc.switchState(CLOSING)
}

func (ssc *StartStopControl) markStop() {
	ssc.switchState(CLOSED)
}

func (ssc *StartStopControl) Prepare() {
	ssc.switchState(PREPARED)
}

func (ssc *StartStopControl) Ready() error {
	select {
	case <-ssc.ReadyChannel():
	case <-ssc.WaitForExitChannel():
		return SERVICE_UNEXPECTED_STOPPED
	}

	return nil
}

func (ssc *StartStopControl) WaitForClose() {
	ssc.waitForState(CLOSING)
}

func (ssc *StartStopControl) WaitForExit() {
	ssc.waitForState(CLOSED)
}

func (ssc *StartStopControl) ReadyChannel() <-chan struct{} {
	return ssc.waitForStateChannel(READY)
}

func (ssc *StartStopControl) WaitForCloseChannel() <-chan struct{} {
	return ssc.waitForStateChannel(CLOSING)
}

func (ssc *StartStopControl) WaitForExitChannel() <-chan struct{} {
	return ssc.waitForStateChannel(CLOSED)
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
		return SERVICE_NOT_RUNNING
	}

	ssc.markClosing()
	return nil
}

func (ssc *StartStopControl) Closed() bool {
	switch ssc.getState() {
	case PREPARED, CLOSED:
		return true
	default:
		return false
	}
}

func (ssc *StartStopControl) Closing() bool {
	return ssc.getState() == CLOSING
}

func (ssc *StartStopControl) Running() bool {
	switch ssc.getState() {
	case STARTED, READY:
		return true
	default:
		return false
	}
}
