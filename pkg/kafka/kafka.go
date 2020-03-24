package kafka

import (
	"fmt"
	"log"
	"sync"

	internal "github.com/etf1/kafka-transformer/internal/transformer"
	"github.com/etf1/kafka-transformer/internal/transformer/kafka"
	"github.com/etf1/kafka-transformer/pkg/logger"
	"github.com/etf1/kafka-transformer/pkg/transformer"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Config is the configuration used by KafkaTransformer
type Config struct {
	SourceTopic    string
	BufferSize     int
	ConsumerConfig *confluent.ConfigMap
	ProducerConfig *confluent.ConfigMap
	Transformer    transformer.Transformer
	Log            logger.Log
}

// Transformer is the orchestrator ot the tree main components: consumer, transformer, producer
type Transformer struct {
	consumer    *kafka.Consumer
	producer    *kafka.Producer
	transformer *internal.Transformer
	config      Config
	wg          *sync.WaitGroup
}

// NewKafkaTransformer constructor for Transformer
func NewKafkaTransformer(config Config) (Transformer, error) {

	l := logger.DefaultLogger()
	if config.Log != nil {
		l = config.Log
	}

	t := transformer.PassThrough()
	if config.Transformer != nil {
		t = config.Transformer
	}

	bufferSize := 200
	if config.BufferSize != 0 {
		bufferSize = config.BufferSize
	}

	consumer, err := kafka.NewConsumer(l, config.SourceTopic, config.ConsumerConfig, bufferSize)
	if err != nil {
		return Transformer{}, fmt.Errorf("consumer creation failed: %w", err)
	}

	transformer := internal.NewTransformer(l, t, bufferSize)

	producer, err := kafka.NewProducer(l, config.ProducerConfig)
	if err != nil {
		return Transformer{}, fmt.Errorf("consumer creation failed: %w", err)
	}

	return Transformer{
		consumer:    &consumer,
		producer:    &producer,
		transformer: &transformer,
		wg:          &sync.WaitGroup{},
	}, nil
}

// Stop will stop Transformer components (consumer, transformer, producer)
func (k Transformer) Stop() {
	log.Println("stopping kafka transformer ...")
	// stopping consumer will make other component stops
	k.consumer.Stop()
	k.wg.Wait()
	log.Println("kafka transformer stoppped")
}

// Run will start the Transformer with all the components (consumer, transformer, producer)
func (k Transformer) Run() error {
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
	log.Println("starting producer ...")
	k.producer.Run(k.wg, transformerChan)

	k.wg.Wait()

	return nil
}
