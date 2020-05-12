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
	logger    logger.Log
	collector instrument.Collector

	hydratorFuncList []kafkaTransformerHydratorFunc
}

type kafkaTransformerHydratorFunc func(kafkaTransformer *KafkaTransformer) error

func (b *KafkaTransformerBuilder) addBuildFunc(buildFunc kafkaTransformerHydratorFunc) {
	b.hydratorFuncList = append(b.hydratorFuncList, buildFunc)
}

func (b *KafkaTransformerBuilder) SetConsumer(sourceTopic string, configConsumer *confluent.ConfigMap, bufferSizeConsumer int) *KafkaTransformerBuilder {
	f := func(kafkaTransformer *KafkaTransformer) error {
		consumer, err := kafka.NewConsumer(b.logger, sourceTopic, configConsumer, b.collector, bufferSizeConsumer)
		if err != nil {
			return fmt.Errorf("consumer creation failed: %w", err)
		}

		kafkaTransformer.Consumer = consumer
		return nil
	}

	b.addBuildFunc(f)
	return b
}

func (b *KafkaTransformerBuilder) SetProducer(producerConfig *confluent.ConfigMap) *KafkaTransformerBuilder {
	f := func(kafkaTransformer *KafkaTransformer) error {
		producer, err := kafka.NewProducer(b.logger, producerConfig, b.collector)
		if err != nil {
			return fmt.Errorf("producer creation failed: %w", err)
		}

		kafkaTransformer.Producer = producer

		// no collector when kafka producer, the collect action will be made in the kafka producer
		// To avoid double collection, the collector is disabled in the projector (which is running the Projector.Project(msg) )
		kafkaTransformer.Projector = internal.NewProjector(b.logger, producer, nil)
		return nil
	}

	b.addBuildFunc(f)
	return b
}

func (b *KafkaTransformerBuilder) SetTransformer(transformer Transformer, workerTimeout time.Duration, bufferSize int) *KafkaTransformerBuilder {
	f := func(kafkaTransformer *KafkaTransformer) error {
		kafkaTransformer.Transformer = internal.NewTransformer(b.logger, transformer, bufferSize, workerTimeout, b.collector)
		return nil
	}

	b.addBuildFunc(f)
	return b
}

func (b *KafkaTransformerBuilder) SetProjector(projector Projector) *KafkaTransformerBuilder {
	f := func(kafkaTransformer *KafkaTransformer) error {
		kafkaTransformer.Projector = internal.NewProjector(b.logger, projector, b.collector)
		return nil
	}

	b.addBuildFunc(f)
	return b
}

func (b *KafkaTransformerBuilder) Build() (*KafkaTransformer, error) {
	kafkaTransformer := &KafkaTransformer{
		wg: &sync.WaitGroup{},
	}

	for _, hydratorFunc := range b.hydratorFuncList {
		err := hydratorFunc(kafkaTransformer)
		if err != nil {
			return nil, err
		}
	}

	return kafkaTransformer, nil
}

func NewKafkaTransformerBuilder(logger logger.Log, collector instrument.Collector) *KafkaTransformerBuilder {
	return &KafkaTransformerBuilder{
		logger: logger,
		collector: collector,
		hydratorFuncList: []kafkaTransformerHydratorFunc{},
	}
}
