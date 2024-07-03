package transformer

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Transformer is an interface which is used by Kafka.Transformer
// in order to transform a kafka Message.
// If nil is returned the message will be ignored
type Transformer interface {
	Transform(ctx context.Context, src *kafka.Message) []*kafka.Message
}
