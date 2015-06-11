package main

import (
	"gopkg.in/Shopify/sarama.v1"
)

type Msg struct {
	*sarama.ConsumerMessage
}

func CreateMsg(cMessage *sarama.ConsumerMessage) *Msg {
	msg := new(Msg)
	msg.ConsumerMessage = cMessage

	return msg
}
