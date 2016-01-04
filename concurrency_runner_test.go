package main

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestConcurrencyRunnableService struct {
	StartTimes  uint64
	CloseTimes  uint64
	waitForStop sync.RWMutex
}

func (trs *TestConcurrencyRunnableService) Run() error {
	trs.waitForStop.RLock()
	defer trs.waitForStop.RUnlock()
	atomic.AddUint64(&trs.StartTimes, 1)
	time.Sleep(time.Microsecond)
	return nil
}

func (trs *TestConcurrencyRunnableService) Close() error {
	trs.waitForStop.Lock()
	defer trs.waitForStop.Unlock()
	atomic.AddUint64(&trs.CloseTimes, 1)
	return nil
}

func TestConcurrencyRunner(t *testing.T) {
	runner := NewConcurrencyRunner()
	var service *TestConcurrencyRunnableService

	// test 0 retry times, 1 concurrency
	service = &TestConcurrencyRunnableService{}
	runner.Concurrency = 1
	runner.RetryTimes = 0
	runner.Run(service)
	assert.Equal(t, uint64(1), service.StartTimes, "Service should start 1 times")
	assert.Equal(t, uint64(1), service.CloseTimes, "Service should close 1 times")

	// test 1 retry times, 10 concurrency
	service = &TestConcurrencyRunnableService{}
	runner.Concurrency = 10
	runner.RetryTimes = 1
	runner.Run(service)
	assert.True(t, 11 >= service.StartTimes, "Service should start not over 11 times")
	assert.Equal(t, uint64(1), service.CloseTimes, "Service should close 1 times")

	// test error channel
	errorChannel, err := runner.Run(service)
	time.Sleep(time.Second)
	assert.Nil(t, err, "Runner should run successfully")
	assert.Equal(t, 11, len(errorChannel), "Runner must contain not over 11 errors in error channel")

	// test async
	errorChannel, err = runner.RunAsync(service)
	assert.Nil(t, err, "Runner should run successfully")
	runner.WaitForExit()
	time.Sleep(time.Second)
	assert.Equal(t, 11, len(errorChannel), "Runner must contain not over 11 errors in error channel")
}
