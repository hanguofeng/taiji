package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestRunnableService struct {
	StartTimes  uint64
	CloseTimes  uint64
	waitForStop sync.Mutex
}

func (trs *TestRunnableService) Run() error {
	trs.waitForStop.Lock()
	defer trs.waitForStop.Unlock()
	atomic.AddUint64(&trs.StartTimes, 1)
	time.Sleep(time.Microsecond)
	return nil
}

func (trs *TestRunnableService) Close() error {
	trs.waitForStop.Lock()
	defer trs.waitForStop.Unlock()
	atomic.AddUint64(&trs.CloseTimes, 1)
	return nil
}

func TestServiceRunner(t *testing.T) {
	runner := NewServiceRunner()

	// test 0 retry times
	runner.RetryTimes = 0
	service := &TestRunnableService{}
	runner.Run([]*TestRunnableService{service})
	assert.Equal(t, uint64(1), service.StartTimes, "Service does not start 1 times")
	assert.Equal(t, uint64(1), service.CloseTimes, "Service does not close 1 times")

	// test 100 retry times
	runner.RetryTimes = 100
	service = &TestRunnableService{}
	runner.Run([]*TestRunnableService{service})
	assert.True(t, 101 >= service.StartTimes, "Service does not start not over 101 times")
	assert.Equal(t, uint64(1), service.CloseTimes, "Service does not close 1 times")

	// test multiple services
	var services []*TestRunnableService
	runner.RetryTimes = 100
	for i := 0; i != 10; i++ {
		services = append(services, &TestRunnableService{})
	}
	runner.Run(services)
	// accumulate retryTimes
	var totalRetries uint64
	var totalCloseTimes uint64
	for i := 0; i != 10; i++ {
		totalRetries += services[i].StartTimes
		totalCloseTimes += services[i].CloseTimes
		if services[i].StartTimes <= 0 {
			// not started ever
			t.Fatal("Some service is never started")
		}
	}
	assert.True(t, (100+10) >= totalRetries, "Service does not start not over 100 + 10 times")
	assert.Equal(t, uint64(10), totalCloseTimes, "Service does not close 10 times")

	// test error channel
	errorChannel, err := runner.Run(services)
	time.Sleep(time.Second)
	assert.Nil(t, err, "Runner should run successfully")
	assert.Equal(t, 100+10, len(errorChannel), "Runner must contain 100 + 10 errors in error channel")

	// test async
	errorChannel, err = runner.RunAsync(services)
	assert.Nil(t, err, "Runner should run successfully")
	// wait for exit
	runner.WaitForExit()
	time.Sleep(time.Second)
	assert.Equal(t, 100+10, len(errorChannel), "Runner must contain 100 + 10 errors in error channel")
}
