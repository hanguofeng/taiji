package main

import (
	"github.com/Shopify/sarama"
)

type Msg struct {
	*sarama.ConsumerMessage
}

func CreateMsg(cMessage *sarama.ConsumerMessage) *Msg {
	msg := new(Msg)
	msg.ConsumerMessage = cMessage

	return msg
}
