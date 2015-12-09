package main

import (
	"errors"
	"reflect"
)

type Runnable interface {
	Run() error
	Close() error
}

type ServiceRunner struct {
	RetryTimes int
	started    bool
	stopper    chan struct{}
}

func NewServiceRunner(args ...int) *ServiceRunner {
	var retryTimes int

	if len(args) > 0 {
		retryTimes = args[0]
	} else {
		retryTimes = 0
	}

	return &ServiceRunner{
		RetryTimes: retryTimes,
		stopper:    make(chan struct{}),
	}
}

func (sr *ServiceRunner) run(services []Runnable, daemon bool) (<-chan error, error) {
	if sr.started {
		return nil, errors.New("ServiceRunner already started")
	}

	sr.started = true
	event := make(chan int)
	errorEvent := make(chan error, sr.RetryTimes+1)

	defer func() {
		sr.started = false
	}()

	servicesWatcher := func(idx int) {
		defer func() {
			event <- idx
		}()
		err := services[idx].Run()
		errorEvent <- err
	}

	controlRoutine := func() {
		leftRetryTimes := sr.RetryTimes

	serviceRespawnLoop:
		for {
			select {
			case <-sr.stopper:
				// close each runnable
				for _, service := range services {
					service.Close()
				}
				break serviceRespawnLoop
			case idx := <-event:
				if leftRetryTimes > 0 {
					go servicesWatcher(idx)
				}
			}

			if leftRetryTimes--; leftRetryTimes <= 0 {
				// not applicable for another respawn
				break serviceRespawnLoop
			}
		}
	}

	for idx, _ := range services {
		go servicesWatcher(idx)
	}

	if daemon {
		go controlRoutine()
	} else {
		controlRoutine()
	}

	return errorEvent, nil
}

func (sr *ServiceRunner) Run(rawServices interface{}) (<-chan error, error) {
	services, err := sr.sanitizeInput(rawServices)

	if nil != err {
		return nil, err
	}

	return sr.run(
		services,
		false, // blocking, not-daemonize
	)
}

func (sr *ServiceRunner) RunAsync(rawServices interface{}) (<-chan error, error) {
	services, err := sr.sanitizeInput(rawServices)

	if nil != err {
		return nil, err
	}

	return sr.run(
		services,
		true, // non-blocking, daemonize
	)
}

func (sr *ServiceRunner) Close() error {
	close(sr.stopper)
	return nil
}

func (sr *ServiceRunner) sanitizeInput(services interface{}) ([]Runnable, error) {
	// main function, use to handle recover result
	runnable, err := func() ([]Runnable, error) {
		// anonymous function 1, use to handle go recover
		return func() ([]Runnable, error) {
			// anonymous function 2, use to do input sanitize work
			defer func() {
				if r := recover(); nil != r {
					// panic
				}
			}()

			servicesValue := reflect.ValueOf(services)

			switch servicesValue.Kind() {
			case reflect.Array:
				fallthrough
			case reflect.Slice:
				break
			default:
				return nil, errors.New("Invalid input, must be array")
			}

			servicesCount := servicesValue.Len()
			runnableServices := make([]Runnable, 0, servicesCount)

			for i := 0; i != servicesCount; i++ {
				if !servicesValue.Index(i).CanInterface() {
					return nil, errors.New("Invalid input, array item should be interface capable")
				}

				runnableServices = append(runnableServices, servicesValue.Index(i).Interface().(Runnable))
			}

			return runnableServices, nil
		}()
	}()

	if nil == runnable && nil == err {
		// panic
		return nil, errors.New("Invalid input, array item must support Run/Close method")

	} else {
		return runnable, err
	}
}
