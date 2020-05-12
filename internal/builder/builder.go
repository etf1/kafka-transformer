package builder

import (
	"fmt"
	"sync"
	"time"

	internal "github.com/etf1/kafka-transformer/internal/transformer"
	"github.com/etf1/kafka-transformer/internal/transformer/kafka"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/etf1/kafka-transformer/pkg/logger"
	"github.com/etf1/kafka-transformer/pkg/transformer"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type KafkaTransformerBuilder struct {
	logger    logger.Log
	collector instrument.Collector

	hydratorFuncList []kafkaTransformerHydratorFunc
}

type kafkaTransformerHydratorFunc func(kafkaTransformer *transformer.KafkaTransformer) error

func (b *KafkaTransformerBuilder) addHydratorFunc(buildFunc kafkaTransformerHydratorFunc) {
	b.hydratorFuncList = append(b.hydratorFuncList, buildFunc)
}

func (b *KafkaTransformerBuilder) SetConsumer(sourceTopic string, configConsumer *confluent.ConfigMap, bufferSizeConsumer int) *KafkaTransformerBuilder {
	f := func(kafkaTransformer *transformer.KafkaTransformer) error {
		consumer, err := kafka.NewConsumer(b.logger, sourceTopic, configConsumer, b.collector, bufferSizeConsumer)
		if err != nil {
			return fmt.Errorf("consumer creation failed: %w", err)
		}

		kafkaTransformer.Consumer = consumer
		return nil
	}

	b.addHydratorFunc(f)
	return b
}

func (b *KafkaTransformerBuilder) SetProducer(producerConfig *confluent.ConfigMap) *KafkaTransformerBuilder {
	f := func(kafkaTransformer *transformer.KafkaTransformer) error {
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

	b.addHydratorFunc(f)
	return b
}

func (b *KafkaTransformerBuilder) SetTransformer(t transformer.Transformer, workerTimeout time.Duration, bufferSize int) *KafkaTransformerBuilder {
	f := func(kafkaTransformer *transformer.KafkaTransformer) error {
		kafkaTransformer.Transformer = internal.NewTransformer(b.logger, t, bufferSize, workerTimeout, b.collector)
		return nil
	}

	b.addHydratorFunc(f)
	return b
}

func (b *KafkaTransformerBuilder) SetProjector(projector transformer.Projector) *KafkaTransformerBuilder {
	f := func(kafkaTransformer *transformer.KafkaTransformer) error {
		kafkaTransformer.Projector = internal.NewProjector(b.logger, projector, b.collector)
		return nil
	}

	b.addHydratorFunc(f)
	return b
}

func (b *KafkaTransformerBuilder) Build() (*transformer.KafkaTransformer, error) {
	kafkaTransformer := &transformer.KafkaTransformer{
		Wg: &sync.WaitGroup{},
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
