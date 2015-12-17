package main

type ConcurrencyRunner struct {
	*StartStopControl
	Concurrency int
	RetryTimes  int
}

func NewConcurrencyRunner() *ConcurrencyRunner {
	return &ConcurrencyRunner{
		StartStopControl: NewStartStopControl(),
	}
}

func (cr *ConcurrencyRunner) Run(service Runnable) (<-chan error, error) {
	return cr.run(service, false /* daemon */)
}

func (cr *ConcurrencyRunner) RunAsync(service Runnable) (<-chan error, error) {
	return cr.run(service, true /* daemon */)
}

func (cr *ConcurrencyRunner) run(service Runnable, daemon bool) (<-chan error, error) {
	if err := cr.ensureStart(); err != nil {
		return nil, err
	}

	channelSize := cr.RetryTimes + cr.Concurrency + 1
	event := make(chan bool, channelSize)
	errorEvent := make(chan error, channelSize)
	servicesWatcher := func() {
		err := service.Run()
		errorEvent <- err
		event <- true
	}

	controlRoutine := func() {
		defer cr.markStop()
	serviceRespawnLoop:
		for i := 0; i <= cr.RetryTimes; i++ {
			select {
			case <-cr.WaitForCloseChannel():
				break serviceRespawnLoop
			case <-event:
				if i != cr.RetryTimes {
					go servicesWatcher()
				} else {
					break serviceRespawnLoop
				}
			}
		}

		service.Close()
	}

	for i := 0; i < cr.Concurrency; i++ {
		go servicesWatcher()
	}

	if daemon {
		go controlRoutine()
	} else {
		controlRoutine()
	}

	return errorEvent, nil
}
