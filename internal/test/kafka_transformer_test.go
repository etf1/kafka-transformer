// +build integration

package test

import (
	"testing"

	"github.com/etf1/kafka-transformer/pkg/transformer/kafka"
)

// Checks the transformer behaviour when a confluent.Error occurs on consumer side
func TestTransformer_resilient_error_consumer(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	consumerConfig := getConsumerConfig(t, "integration-test-group")
	consumerConfig.SetKey("bootstrap.servers", "unknown:9092")

	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: consumerConfig,
		ProducerConfig: getProducerConfig(),
	}

	transformer, err := kafka.NewKafkaTransformer(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer transformer.Stop()

	go func() {
		err = transformer.Run()
	}()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Checks the transformer behaviour when a confluent.Error occurs on producer side
func TestTransformer_resilient_error_producer(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	producerConfig := getProducerConfig()
	producerConfig.SetKey("bootstrap.servers", "unknown:9092")

	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		ProducerConfig: producerConfig,
	}

	transformer, err := kafka.NewKafkaTransformer(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer transformer.Stop()

	go func() {
		err = transformer.Run()
	}()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	messages := messages(srcTopic, 5)
	produceMessages(t, messages)
}
