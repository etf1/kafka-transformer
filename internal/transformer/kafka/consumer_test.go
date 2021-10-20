package kafka

import (
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	otelconfluent "github.com/eko/confluent-kafka-go/instrumentation/otel"
	_instrument "github.com/etf1/kafka-transformer/internal/instrument"
	_logger "github.com/etf1/kafka-transformer/internal/logger"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
)

func TestNewConsumer(t *testing.T) {
	// Given
	l := _logger.StdoutLogger()

	topic := "test-topic"
	bufferSize := 1

	cfg := &confluent.ConfigMap{
		"group.id": "test-group-id",
	}
	collector := _instrument.NoopCollector(false)

	// When
	consumer, err := NewConsumer(l, topic, cfg, collector, bufferSize)

	// Then
	assert.Nil(t, err)

	assert.Equal(t, topic, consumer.topic)
	assert.Equal(t, bufferSize, consumer.bufferSize)
	assert.Equal(t, l, consumer.log)
	assert.Equal(t, collector, consumer.collector)
	assert.IsType(t, new(confluent.Consumer), consumer.consumer)
}

func TestNewConsumer_WithTracerProvider(t *testing.T) {
	// Given
	l := _logger.StdoutLogger()

	topic := "test-topic"
	bufferSize := 1

	cfg := &confluent.ConfigMap{
		"group.id": "test-group-id",
	}
	collector := _instrument.NoopCollector(false)

	tracerProvider := otel.GetTracerProvider()

	// When
	consumer, err := NewConsumer(l, topic, cfg, collector, bufferSize, WithTracerProvider(tracerProvider))

	// Then
	assert.Nil(t, err)

	assert.Equal(t, topic, consumer.topic)
	assert.Equal(t, bufferSize, consumer.bufferSize)
	assert.Equal(t, l, consumer.log)
	assert.Equal(t, collector, consumer.collector)
	assert.IsType(t, new(otelconfluent.Consumer), consumer.consumer)
}
