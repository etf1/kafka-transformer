package transformer

import (
	"fmt"
	"sync"
	"time"

	internal "github.com/etf1/kafka-transformer/internal/transformer"
	"github.com/etf1/kafka-transformer/internal/transformer/kafka"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/etf1/kafka-transformer/pkg/logger"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type KafkaTransformerBuilder struct {
	logger      logger.Log
	collector   instrument.Collector
	consumer    *kafka.Consumer
	producer    *kafka.Producer
	transformer *internal.Transformer
	decorateProjector *internal.Projector
	projector         Projector
}

func NewKafkaTransformerBuilder() *KafkaTransformerBuilder {
	return &KafkaTransformerBuilder{}
}

func (b *KafkaTransformerBuilder) AddLogger(l logger.Log) {
	b.logger = l
}

func (b *KafkaTransformerBuilder) AddCollector(collector instrument.Collector) {
	b.collector = collector
}

func (b *KafkaTransformerBuilder) AddConsumer(config *confluent.ConfigMap, sourceTopic string, bufferSize int) error {
	consumer, err := kafka.NewConsumer(b.logger, sourceTopic, config, b.collector, bufferSize)
	if err != nil {
		return fmt.Errorf("consumer creation failed: %w", err)
	}

	b.consumer = consumer
	return nil
}

func (b *KafkaTransformerBuilder) AddProducer(producerConfig *confluent.ConfigMap) error {
	producer, err := kafka.NewProducer(b.logger, producerConfig, b.collector)
	if err != nil {
		return fmt.Errorf("producer creation failed: %w", err)
	}

	b.producer = producer
	// no collector when kafka producer, the collect action will be made in the kafka producer
	// To avoid double collection, the collector is disabled in the projector (which is running the Projector.Project(msg) )
	// b.collector = nil

	b.decorateProjector = internal.NewProjector(b.logger, producer, b.collector)

	return nil
}

func (b *KafkaTransformerBuilder) AddTransformer(transformer Transformer, workerTimeout time.Duration, bufferSize int) {
	b.transformer = internal.NewTransformer(b.logger, transformer, bufferSize, workerTimeout, b.collector)
}

func (b *KafkaTransformerBuilder) AddProjector(projector Projector) {
	b.decorateProjector = internal.NewProjector(b.logger, projector, b.collector)
}

func (b *KafkaTransformerBuilder) Build() *KafkaTransformer {
	return &KafkaTransformer{
		consumer:    b.consumer,
		producer:    b.producer,
		transformer: b.transformer,
		projector:   b.decorateProjector,
		wg:          &sync.WaitGroup{},
	}
}
