package internal

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// Operation represents a kind of Kafka operation.
type Operation string

const (
	// OperationProduce represents a Kafka produce action.
	OperationProduce Operation = "produce"

	// OperationConsume represents a Kafka consume action.
	OperationConsume Operation = "consume"

	// OperationTransform represents a transformer step.
	OperationTransform Operation = "transform"

	// OperationProject represents a projector step.
	OperationProject Operation = "project"

	// MessagingSystemKeyValue repreesents the messaging system.
	MessagingSystemKeyValue = "kafka"
)

// KafkaSystemKey is the attribute for system value.
func KafkaSystemKey() attribute.KeyValue {
	return semconv.MessagingSystemKey.String(MessagingSystemKeyValue)
}

// KafkaOperation is the attribute for operation name.
func KafkaOperation(operationName Operation) attribute.KeyValue {
	return semconv.MessagingOperationKey.String(string(operationName))
}

// KafkaDestinationTopic is the attribute for messaging destination topic.
func KafkaDestinationTopic(topic string) attribute.KeyValue {
	return semconv.MessagingDestinationKey.String(topic)
}

// KafkaMessageKey is the attribute for messaging key.
func KafkaMessageKey(messageID string) attribute.KeyValue {
	return semconv.MessagingMessageIDKey.String(messageID)
}

// KafkaMessageHeaders is the attribute for messaging headers.
func KafkaMessageHeaders(headers []kafka.Header) []attribute.KeyValue {
	var attributes []attribute.KeyValue

	for _, header := range headers {
		attributes = append(attributes, attribute.Key("messaging.headers."+header.Key).String(string(header.Value)))
	}

	return attributes
}

// KafkaConsumerGroupID is the attribute for consumer group ID.
func KafkaConsumerGroupID(consumerGroupID string) attribute.KeyValue {
	return semconv.MessagingKafkaConsumerGroupKey.String(consumerGroupID)
}
