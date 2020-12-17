package transformer

import (
	"fmt"
	"log"
	"sync"

	"github.com/etf1/kafka-transformer/internal/builder"
	internal "github.com/etf1/kafka-transformer/internal/transformer"
	"github.com/etf1/kafka-transformer/internal/transformer/kafka"
)

// Transformer is the orchestrator ot the tree main components: consumer, transformer, producer
type KafkaTransformer struct {
	Wg          *sync.WaitGroup

	Consumer    *kafka.Consumer
	Producer    *kafka.Producer
	Transformer *internal.Transformer
	Projector   *internal.Projector
}

// NewKafkaTransformer constructor for Transformer
func NewKafkaTransformer(config *Config) (*KafkaTransformer, error) {
	b := builder.NewKafkaTransformerBuilder(config.Log, config.Collector)
	b.SetConsumer(config.SourceTopic, config.ConsumerConfig, config.BufferSize).
		SetTransformer(config.Transformer, config.WorkerTimeout, config.BufferSize)

	if config.ProducerConfig != nil {
		b.SetProducer(config.ProducerConfig)
	}

	if config.Projector != nil {
		b.SetProjector(config.Projector)
	}

	return b.Build()
}

// Stop will stop Transformer components (consumer, transformer, producer)
func (k KafkaTransformer) Stop() {
	log.Println("stopping kafka transformer ...")
	// stopping consumer will make other component stops
	k.Consumer.Stop()
	k.wg.Wait()
	if k.Producer != nil {
		k.Producer.Close()
	}
	log.Println("kafka transformer stopped")
}

// Run will start the Transformer with all the components (consumer, transformer, producer)
func (k KafkaTransformer) Run() error {
	log.Println("starting kafka transformer ...")

	k.wg.Add(3)

	// First run the consumer
	log.Println("starting consumer ...")
	consumerChan, err := k.Consumer.Run(k.wg)
	if err != nil {
		return fmt.Errorf("consumer start failed: %w", err)
	}

	// Then transformer
	log.Println("starting transformer ...")
	transformerChan := k.Transformer.Run(k.wg, consumerChan)

	// Finally, producer
	log.Println("starting projector ...")
	k.Projector.Run(k.wg, transformerChan)

	k.wg.Wait()

	return nil
}
