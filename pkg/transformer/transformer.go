package transformer

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Transformer is an interface which is used by Kafka.Transformer
// in order to transform a kafka Message.
// If nil is returned the message will be ignored
type Transformer interface {
	Transform(src *kafka.Message) []*kafka.Message
}
