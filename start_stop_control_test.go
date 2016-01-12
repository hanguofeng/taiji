package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStartStopControlBlockOnWaitForClose(t *testing.T) {
	type BlockOnWaitForCloseTest struct {
		*StartStopControl
	}

	waitForCloseTest := &BlockOnWaitForCloseTest{StartStopControl: NewStartStopControl()}

	waitForCloseTest.Prepare()

	t.Logf("Monitor Routine, Try to start BlockOnWaitForCloseTest worker routine")

	go func() {
		waitForCloseTest.markStart()
		defer waitForCloseTest.markStop()

		t.Logf("Worker Routine, BlockOnWaitForCloseTest marked start")

		// do something before process become ready to service
		time.Sleep(1 * time.Second)

		waitForCloseTest.markReady()

		t.Logf("Worker Routine, BlockOnWaitForCloseTest marked ready")

		// do something before process work
	workerLoop:
		for {
			select {
			case <-waitForCloseTest.WaitForCloseChannel():
				break workerLoop
			case <-time.After(100 * time.Millisecond):
				t.Logf("Worker Routine, work work work, sleep sleep sleep")
			}
		}

		t.Logf("Worker Routine, BlockOnWaitForCloseTest caught close event")

		// do something before process ready to quit
		time.Sleep(1 * time.Second)

		t.Logf("Worker Routine, BlockOnWaitForCloseTest worker routine exit")
	}()

	t.Logf("Monitor Routine, Started BlockOnWaitForCloseTest worker routine")
	t.Logf("Monitor Routine, Try to wait BlockOnWaitForCloseTest become Ready")

	select {
	case <-waitForCloseTest.ReadyChannel():
	}

	assert.Equal(t, READY, waitForCloseTest.getState(), "Service state is not ready")

	t.Logf("Monitor Routine, BlockOnWaitForCloseTest become Ready")
	t.Logf("Monitor Routine, Try to close BlockOnWaitForCloseTest")

	// let worker work for 2 seconds
	time.Sleep(2 * time.Second)

	waitForCloseTest.AsyncClose()

	t.Logf("Monitor Routine, Sent BlockOnWaitForCloseTest.AsyncClose")

	select {
	case <-waitForCloseTest.WaitForExitChannel():
	}

	assert.Equal(t, CLOSED, waitForCloseTest.getState(), "Service state is not closed")

	t.Logf("Monitor Routine, Wait BlockOnWaitForCloseTest exit success")
}
