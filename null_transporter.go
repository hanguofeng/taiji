package main

import "github.com/golang/glog"

type NullTransporter struct {
	*ConcurrencyStartStopControl

	// config
	config            *CallbackItemConfig
	transporterConfig TransporterConfig

	// parent
	manager *PartitionManager
}

func NewNullTransporter() Transporter {
	return &NullTransporter{
		ConcurrencyStartStopControl: &ConcurrencyStartStopControl{},
	}
}

func (nt *NullTransporter) Init(config *CallbackItemConfig, transporterConfig TransporterConfig, manager *PartitionManager) error {
	nt.config = config
	nt.transporterConfig = transporterConfig
	nt.manager = manager

	return nil
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
			glog.Infof("Committed message [topic:%s][partition:%d][url:%s][offset:%d]",
				message.Topic, message.Partition, nt.config.Url, message.Offset)
			offsets <- message.Offset
		case <-nt.WaitForCloseChannel():
			break transporterLoop
		}

	}

	return nil
}

func init() {
	RegisterTransporter("Null", NewNullTransporter)
}
