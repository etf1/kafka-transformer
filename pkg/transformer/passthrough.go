package transformer

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type passThrough struct{}

// Transform a kafka Message
func (p passThrough) Transform(ctx context.Context, src *kafka.Message) []*kafka.Message {
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
