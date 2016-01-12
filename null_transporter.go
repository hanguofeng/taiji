package main

import (
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

type NullTransporter struct {
	*ConcurrencyStartStopControl

	// config
	config            *CallbackItemConfig
	transporterConfig TransporterConfig

	// parent
	manager *PartitionManager

	// stat variables
	delivered uint64
	startTime time.Time
}

func NewNullTransporter() Transporter {
	return &NullTransporter{
		ConcurrencyStartStopControl: NewConcurrencyStartStopControl(),
	}
}

func (nt *NullTransporter) Init(config *CallbackItemConfig, transporterConfig TransporterConfig, manager *PartitionManager) error {
	nt.config = config
	nt.transporterConfig = transporterConfig
	nt.manager = manager

	return nil
}

func (nt *NullTransporter) ResetStat() {
	atomic.StoreUint64(&nt.delivered, 0)
	nt.startTime = time.Now().Local()
}

func (nt *NullTransporter) Run() error {
	nt.markStart()
	defer nt.markStop()

	arbiter := nt.manager.GetArbiter()
	messages := arbiter.MessageChannel()
	offsets := arbiter.OffsetChannel()

transporterLoop:
	for {
		select {
		case message := <-messages:
			atomic.AddUint64(&nt.delivered, 1)
			glog.Infof("Committed message [topic:%s][partition:%d][url:%s][offset:%d]",
				message.Topic, message.Partition, nt.config.Url, message.Offset)
			offsets <- message.Offset
		case <-nt.WaitForCloseChannel():
			break transporterLoop
		}

	}

	return nil
}

func (nt *NullTransporter) GetStat() interface{} {
	result := make(map[string]interface{})
	result["delivered"] = atomic.LoadUint64(&nt.delivered)
	result["start_time"] = nt.startTime
	return result
}

func init() {
	RegisterTransporter("Null", NewNullTransporter)
}
