package main

import "time"

type Transporter interface {
	Init(config *CallbackItemConfig, transporterConfig *TransporterConfig, manager *PartitionManager)
	Run()
}

type WorkerCallback struct {
	Url          string
	RetryTimes   int
	Timeout      time.Duration
	BypassFailed bool
	FailedSleep  time.Duration
}

type HTTPTransporter struct {
	Callback    *WorkerCallback
	Serializer  string
	ContentType string
}

type HTTPBatchTransporter struct {
	Callback    *WorkerCallback
	Serializer  string
	ContentType string
}
