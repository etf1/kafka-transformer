package transformer

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type passThrough struct{}

// Transform a kafka Message
func (p passThrough) Transform(src *kafka.Message) (*kafka.Message, error) {
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

	return msg, nil
}

// PassThrough returns a transformer which does nothing,
// just transferring the message in a new topic
func PassThrough() Transformer {
	return passThrough{}
}