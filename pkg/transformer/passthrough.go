package transformer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type passThrough struct{}

// Transform a kafka Message
func (p passThrough) Transform(src *kafka.Message) []*kafka.Message {
	topic := *src.TopicPartition.Topic + "-passthrough"

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:   src.Value,
		Key:     src.Key,
		Headers: src.Headers,
	}

	return []*kafka.Message{msg}
}

// PassThrough returns a transformer which does nothing,
// just transferring the message in a new topic
func PassThrough() Transformer {
	return passThrough{}
}
