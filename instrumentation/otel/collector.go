package otel

import (
	"context"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-transformer/instrumentation/otel/internal"
	"github.com/etf1/kafka-transformer/pkg/instrument"

	"go.opentelemetry.io/contrib"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// Collector is the OpenTelemetry instrumentation collector.
type Collector struct {
	ctx             context.Context
	tracer          oteltrace.Tracer
	propagator      propagation.TextMapPropagator
	consumerGroupID string
	spans           *sync.Map
}

// NewCollector instanciates a new Collector.
func NewCollector(opts ...Option) *Collector {
	cfg := &config{
		tracerProvider: otel.GetTracerProvider(),
		propagator:     otel.GetTextMapPropagator(),
		tracerName:     tracerName,
	}

	for _, o := range opts {
		o.apply(cfg)
	}

	return &Collector{
		ctx: context.Background(),
		tracer: cfg.tracerProvider.Tracer(
			cfg.tracerName,
			oteltrace.WithInstrumentationVersion(contrib.SemVersion()),
		),
		propagator:      cfg.propagator,
		consumerGroupID: cfg.consumerGroupID,
		spans:           &sync.Map{},
	}
}

func (c *Collector) attrsByOperationAndMessage(operation internal.Operation, msg *kafka.Message) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		internal.KafkaSystemKey(),
		internal.KafkaOperation(operation),
		semconv.MessagingDestinationKindTopic,
	}

	switch operation {
	case internal.OperationConsume:
		attributes = append(
			attributes,
			internal.KafkaConsumerGroupID(c.consumerGroupID),
		)
	}

	if msg != nil {
		attributes = append(
			attributes,
			internal.KafkaMessageKey(string(msg.Key)),
			semconv.MessagingKafkaPartitionKey.Int(int(msg.TopicPartition.Partition)),
		)
		attributes = append(attributes, internal.KafkaMessageHeaders(msg.Headers)...)

		if topic := msg.TopicPartition.Topic; topic != nil {
			attributes = append(attributes, internal.KafkaDestinationTopic(*topic))
		}
	}

	return attributes
}

func (c *Collector) startSpan(operationName internal.Operation, msg *kafka.Message) oteltrace.Span {
	opts := []oteltrace.SpanStartOption{
		oteltrace.WithSpanKind(oteltrace.SpanKindConsumer),
	}

	carrier := NewMessageCarrier(msg)
	ctx := c.propagator.Extract(c.ctx, carrier)

	ctx, span := c.tracer.Start(ctx, string(operationName), opts...)

	c.propagator.Inject(ctx, carrier)

	span.SetAttributes(c.attrsByOperationAndMessage(operationName, msg)...)

	return span
}

// Before is triggered before an event occurred.
func (c *Collector) Before(message *kafka.Message, action instrument.Action, _ time.Time) {
	if message == nil {
		return
	}

	var operation internal.Operation
	switch action {
	case instrument.KafkaProducerProduce:
		operation = internal.OperationProduce

	case instrument.KafkaConsumerConsume:
		operation = internal.OperationConsume

	case instrument.TransformerTransform:
		operation = internal.OperationTransform

	case instrument.ProjectorProject:
		operation = internal.OperationProject

	default:
		return
	}

	span := c.startSpan(operation, message)

	c.spans.Store(message, span)
}

// After is triggered before an event occurred.
func (c *Collector) After(message *kafka.Message, action instrument.Action, err error, _ time.Time) {
	if message == nil {
		return
	}

	if value, ok := c.spans.LoadAndDelete(message); ok {
		span := value.(oteltrace.Span)
		endSpan(span, err)
	}
}
