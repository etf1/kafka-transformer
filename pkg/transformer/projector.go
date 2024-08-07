package transformer

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Projector is an interface which is used by Kafka.Transformer
// in order to project a kafka Message (to kafka or an external system)
// If you want to customize the projection, this is the interface to implement
type Projector interface {
	Project(ctx context.Context, message *kafka.Message)
}
