package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

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
