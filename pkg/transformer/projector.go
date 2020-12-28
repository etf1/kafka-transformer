package transformer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Projector is an interface which is used by Kafka.Transformer
// in order to project a kafka Message (to kafka or an external system)
// If you want to customize the projection, this is the interface to implement
type Projector interface {
	Project(message *kafka.Message)
}
