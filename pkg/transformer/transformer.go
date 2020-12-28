package transformer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Transformer is an interface which is used by Kafka.Transformer
// in order to transform a kafka Message.
// If nil is returned the message will be ignored
type Transformer interface {
	Transform(src *kafka.Message) *kafka.Message
}
