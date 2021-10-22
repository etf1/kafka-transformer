package otel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func TestNewCollector(t *testing.T) {
	// When
	collector := NewCollector()

	// Then
	assert.IsType(t, new(Collector), collector)

	assert.Equal(t, context.Background(), collector.ctx)
	assert.Equal(t, otel.GetTextMapPropagator(), collector.propagator)
	assert.Equal(t, new(sync.Map), collector.spans)
}

func TestNewCollector_WithTracerProvider(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	// When
	collector := NewCollector(WithTracerProvider(tracerProvider))

	// Then
	assert.IsType(t, new(Collector), collector)

	assert.Equal(t, context.Background(), collector.ctx)
	assert.Equal(t, otel.GetTextMapPropagator(), collector.propagator)
	assert.Equal(t, new(sync.Map), collector.spans)
}

func TestCollector_Before_Produce(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	collector := NewCollector(WithTracerProvider(tracerProvider))

	message := &kafka.Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	// When
	collector.Before(message, instrument.KafkaProducerProduce, time.Now())

	// Then
	value, ok := collector.spans.Load(message)
	span := value.(trace.Span)

	assert.True(t, ok)
	assert.Equal(t, "00000000000000000000000000000000", span.SpanContext().TraceID().String())
	assert.Equal(t, "0000000000000000", span.SpanContext().SpanID().String())
}

func TestCollector_Before_Consume(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	collector := NewCollector(WithTracerProvider(tracerProvider))

	message := &kafka.Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	// When
	collector.Before(message, instrument.KafkaConsumerConsume, time.Now())

	// Then
	value, ok := collector.spans.Load(message)
	span := value.(trace.Span)

	assert.True(t, ok)
	assert.Equal(t, "00000000000000000000000000000000", span.SpanContext().TraceID().String())
	assert.Equal(t, "0000000000000000", span.SpanContext().SpanID().String())
}

func TestCollector_Before_Transform(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	collector := NewCollector(WithTracerProvider(tracerProvider))

	message := &kafka.Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	// When
	collector.Before(message, instrument.TransformerTransform, time.Now())

	// Then
	value, ok := collector.spans.Load(message)
	span := value.(trace.Span)

	assert.True(t, ok)
	assert.Equal(t, "00000000000000000000000000000000", span.SpanContext().TraceID().String())
	assert.Equal(t, "0000000000000000", span.SpanContext().SpanID().String())
}

func TestCollector_Before_Project(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	collector := NewCollector(WithTracerProvider(tracerProvider))

	message := &kafka.Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	// When
	collector.Before(message, instrument.ProjectorProject, time.Now())

	// Then
	value, ok := collector.spans.Load(message)
	span := value.(trace.Span)

	assert.True(t, ok)
	assert.Equal(t, "00000000000000000000000000000000", span.SpanContext().TraceID().String())
	assert.Equal(t, "0000000000000000", span.SpanContext().SpanID().String())
}

func TestCollector_Before_AnotherAction(t *testing.T) {
	// Given
	tracerProvider := otel.GetTracerProvider()

	collector := NewCollector(WithTracerProvider(tracerProvider))

	message := &kafka.Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	// When
	collector.Before(message, instrument.OverallTime, time.Now())

	// Then
	value, ok := collector.spans.Load(message)

	assert.Nil(t, value)
	assert.False(t, ok)
}
