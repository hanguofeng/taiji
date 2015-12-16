package main

import "github.com/golang/glog"

type NullTransporter struct {
	// config
	config            *CallbackItemConfig
	transporterConfig TransporterConfig

	// parent
	manager *PartitionManager
}

func NewNullTransporter() Transporter {
	return &NullTransporter{}
}

func (nt *NullTransporter) Init(config *CallbackItemConfig, transporterConfig TransporterConfig, manager *PartitionManager) error {
	nt.config = config
	nt.transporterConfig = transporterConfig
	nt.manager = manager

	return nil
}

func (nt *NullTransporter) Run() error {
	arbiter := nt.manager.GetArbiter()
	messages := arbiter.MessageChannel()
	offsets := arbiter.OffsetChannel()

	for message := range messages {
		glog.Infof("Committed message [topic:%s][partition:%d][url:%s][offset:%d]",
			message.Topic, message.Partition, nt.config.Url, message.Offset)
		offsets <- message.Offset
	}

	return nil
}

func (nt *NullTransporter) Close() error {
	// dummy
	return nil
}

func init() {
	RegisterTransporter("Null", NewNullTransporter)
}
