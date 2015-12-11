package main

import (
	"errors"
	"reflect"
	"sync"
)

type Runnable interface {
	Run() error
	Close() error
}

type ServiceRunner struct {
	*StartStopControl
	RetryTimes int
}

func NewServiceRunner() *ServiceRunner {
	return &ServiceRunner{
		StartStopControl: &StartStopControl{},
	}
}

func (sr *ServiceRunner) Run(rawServices interface{}) (<-chan error, error) {
	services, err := sr.sanitizeInput(rawServices)

	if err != nil {
		return nil, err
	}

	return sr.run(
		services,
		false, // blocking, not-daemonize
	)
}

func (sr *ServiceRunner) RunAsync(rawServices interface{}) (<-chan error, error) {
	services, err := sr.sanitizeInput(rawServices)

	if err != nil {
		return nil, err
	}

	return sr.run(
		services,
		true, // non-blocking, daemonize
	)
}

func (sr *ServiceRunner) sanitizeInput(services interface{}) ([]Runnable, error) {
	// main function, use to handle recover result
	runnable, err := func() ([]Runnable, error) {
		// anonymous function 1, use to handle go recover
		return func() ([]Runnable, error) {
			// anonymous function 2, use to do input sanitize work
			defer func() {
				if r := recover(); r != nil {
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

	if runnable == nil && err == nil {
		// panic
		return nil, errors.New("Invalid input, array item must support Run/Close method")

	} else {
		return runnable, err
	}
}

func (sr *ServiceRunner) run(services []Runnable, daemon bool) (<-chan error, error) {
	if sr.Running() {
		return nil, errors.New("Service is running")
	}

	sr.markStart()

	event := make(chan int)
	errorEvent := make(chan error, sr.RetryTimes+1)
	servicesWatcher := func(idx int) {
		defer func() {
			event <- idx
		}()
		err := services[idx].Run()
		errorEvent <- err
	}
	controlRoutine := func() {
		// flag we are done exiting
		defer sr.markStop()

		leftRetryTimes := sr.RetryTimes

	serviceRespawnLoop:
		for {
			select {
			case <-sr.WaitForCloseChannel():
				// close each runnable
				wg := sync.WaitGroup{}
				for _, service := range services {
					wg.Add(1)
					go func(service Runnable) {
						defer wg.Done()
						service.Close()
					}(service)
				}
				wg.Wait()
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
