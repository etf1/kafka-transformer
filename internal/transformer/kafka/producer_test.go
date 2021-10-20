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

func TestNewProducer(t *testing.T) {
	// Given
	l := _logger.StdoutLogger()

	cfg := &confluent.ConfigMap{}
	collector := _instrument.NoopCollector(false)

	// When
	producer, err := NewProducer(l, cfg, collector)

	// Then
	assert.Nil(t, err)

	assert.Equal(t, l, producer.log)
	assert.Equal(t, collector, producer.collector)
	assert.IsType(t, new(confluent.Producer), producer.producer)
}

func TestNewProducer_WithTracerProvider(t *testing.T) {
	// Given
	l := _logger.StdoutLogger()

	cfg := &confluent.ConfigMap{}
	collector := _instrument.NoopCollector(false)

	tracerProvider := otel.GetTracerProvider()

	// When
	producer, err := NewProducer(l, cfg, collector, WithTracerProvider(tracerProvider))

	// Then
	assert.Nil(t, err)

	assert.Equal(t, l, producer.log)
	assert.Equal(t, collector, producer.collector)
	assert.IsType(t, new(otelconfluent.Producer), producer.producer)
}
