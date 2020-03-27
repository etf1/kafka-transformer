package test

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// sliceProjector will simply write the message in a slice
type sliceProjector struct {
	list []*kafka.Message
}

// Project implements Projector interface
func (cp *sliceProjector) Project(msg *kafka.Message) {
	if string(msg.Value) == "panic" {
		panic("panic !!!")
	}
	cp.list = append(cp.list, msg)
}
