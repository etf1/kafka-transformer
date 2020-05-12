package transformer

import (
	"fmt"
	"log"
	"sync"

	internal "github.com/etf1/kafka-transformer/internal/transformer"
	"github.com/etf1/kafka-transformer/internal/transformer/kafka"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Transformer is the orchestrator ot the tree main components: consumer, transformer, producer
type KafkaTransformer struct {
	consumer    *kafka.Consumer
	producer    *kafka.Producer
	transformer *internal.Transformer
	projector   *internal.Projector
	wg          *sync.WaitGroup
}

// NewKafkaTransformer constructor for Transformer
func NewKafkaTransformer(topic string, consumerConfig *confluent.ConfigMap, configs ...configFunc) (*KafkaTransformer, error) {
	config := NewConfig()
	for _, configFunc := range configs {
		configFunc(config)
	}

	builder := NewKafkaTransformerBuilder()
	err := builder.AddConsumer(consumerConfig, topic, config.BufferSize)
	if err != nil {
		return nil, err
	}

	builder.AddLogger(config.Log)
	builder.AddCollector(config.Collector)
	builder.AddTransformer(config.Transformer, config.WorkerTimeout, config.BufferSize)

	if config.ProducerConfig != nil {
		err = builder.AddProducer(config.ProducerConfig)
		if err != nil {
			return nil, err
		}
	}

	if config.Projector != nil {
		builder.AddProjector(config.Projector)
	}

	return builder.Build(), nil
}

// Stop will stop Transformer components (consumer, transformer, producer)
func (k KafkaTransformer) Stop() {
	log.Println("stopping kafka transformer ...")
	// stopping consumer will make other component stops
	k.consumer.Stop()
	k.wg.Wait()
	if k.producer != nil {
		k.producer.Close()
	}
	log.Println("kafka transformer stopped")
}

// Run will start the Transformer with all the components (consumer, transformer, producer)
func (k KafkaTransformer) Run() error {
	log.Println("starting kafka transformer ...")

	k.wg.Add(3)

	// First run the consumer
	log.Println("starting consumer ...")
	consumerChan, err := k.consumer.Run(k.wg)
	if err != nil {
		return fmt.Errorf("consumer start failed: %w", err)
	}

	// Then transformer
	log.Println("starting transformer ...")
	transformerChan := k.transformer.Run(k.wg, consumerChan)

	// Finally, producer
	log.Println("starting projector ...")
	k.projector.Run(k.wg, transformerChan)

	k.wg.Wait()

	return nil
}
