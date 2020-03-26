package kafka

import (
	"fmt"
	"log"
	"sync"

	internal "github.com/etf1/kafka-transformer/internal/transformer"
	"github.com/etf1/kafka-transformer/internal/transformer/kafka"
	"github.com/etf1/kafka-transformer/pkg/logger"
	pkg "github.com/etf1/kafka-transformer/pkg/transformer"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Config is the configuration used by KafkaTransformer
type Config struct {
	SourceTopic    string
	BufferSize     int
	ConsumerConfig *confluent.ConfigMap
	ProducerConfig *confluent.ConfigMap
	Transformer    pkg.Transformer
	Projector      pkg.Projector
	Log            logger.Log
}

// Transformer is the orchestrator ot the tree main components: consumer, transformer, producer
type Transformer struct {
	consumer    *kafka.Consumer
	producer    *kafka.Producer
	transformer *internal.Transformer
	projector   *internal.Projector
	config      Config
	wg          *sync.WaitGroup
}

// NewKafkaTransformer constructor for Transformer
func NewKafkaTransformer(config Config) (Transformer, error) {

	if config.ProducerConfig == nil && config.Projector == nil {
		return Transformer{}, fmt.Errorf("missing configuration: ProducerConfig or Projector")
	}

	if config.ProducerConfig != nil && config.Projector != nil {
		return Transformer{}, fmt.Errorf("configuration must be set as either 'ProducerConfig' or 'Projector'")
	}

	l := logger.DefaultLogger()
	if config.Log != nil {
		l = config.Log
	}

	t := pkg.PassThrough()
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

	kafkaTransformer := Transformer{
		consumer:    &consumer,
		transformer: &transformer,
		wg:          &sync.WaitGroup{},
	}

	var projector pkg.Projector

	if config.ProducerConfig != nil {
		producer, err := kafka.NewProducer(l, config.ProducerConfig)
		if err != nil {
			return Transformer{}, fmt.Errorf("producer creation failed: %w", err)
		}
		kafkaTransformer.producer = &producer
		projector = &producer
	} else if config.Projector != nil {
		projector = config.Projector
	}

	p := internal.NewProjector(l, projector)
	kafkaTransformer.projector = &p

	return kafkaTransformer, nil
}

// Stop will stop Transformer components (consumer, transformer, producer)
func (k Transformer) Stop() {
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
	log.Println("starting projector ...")
	k.projector.Run(k.wg, transformerChan)

	k.wg.Wait()

	return nil
}
