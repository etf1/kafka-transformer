package transformer

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type passThrough struct{}

// Transform a kafka Message
func (p passThrough) Transform(src *kafka.Message) []*kafka.Message {
	return []*kafka.Message{src}
}

// PassThrough returns a transformer which does nothing,
// just transferring the message in a new topic
func PassThrough() Transformer {
	return passThrough{}
}
