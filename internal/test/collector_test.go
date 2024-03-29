//go:build integration
// +build integration

package test

import (
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-transformer/pkg/transformer/kafka"
)

// Default case with a simple collector
func TestCollector_default(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	collector := NewSliceCollector()
	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		ProducerConfig: getProducerConfig(),
		BufferSize:     10,
		Collector:      collector,
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

	msgCount := 5
	messages := messages(srcTopic, msgCount)

	produceMessages(t, messages)
	collector.assertExpectedItems(t, msgCount, true)
}

func TestCollector_with_custom_projector(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	projector := NewSliceProjector()
	collector := NewSliceCollector()
	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		Projector:      projector,
		BufferSize:     10,
		Collector:      collector,
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

	msgCount := 5
	messages := messages(srcTopic, msgCount)

	produceMessages(t, messages)
	collector.assertExpectedItems(t, msgCount, false)
}

// Check collector behaviour when panic has occured
func TestCollector_recover_transformer_panic(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	collector := NewSliceCollector()
	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		ProducerConfig: getProducerConfig(),
		Transformer:    NewUnstableTransformer(),
		Collector:      collector,
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

	messages := []*confluent.Message{message(srcTopic, "panic")}

	produceMessages(t, messages)

	collector.ReadItems(t, 3, 3)

	if len(collector.before) != 3 {
		t.Fatalf("unexpected size for before, should be 3 but got %v", len(collector.before))
	}
	if len(collector.after) != 3 {
		t.Fatalf("unexpected size for after, should be 3 but got %v", len(collector.after))
	}
	if collector.after[1].err == nil || collector.after[1].err.Error() != "panic from transformer" {
		t.Fatalf("unexpected behaviour, should got an error")
	}
	if collector.after[2].err == nil || collector.after[2].err.Error() != "panic from transformer" {
		t.Fatalf("unexpected behaviour, should got an error")
	}
}

func TestCollector_recover_project_panic(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	collector := NewSliceCollector()
	projector := NewSliceProjector()
	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		Projector:      projector,
		Collector:      collector,
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

	messages := []*confluent.Message{message(srcTopic, "panic")}

	produceMessages(t, messages)

	collector.ReadItems(t, 4, 4)

	collector.Print(t)

	if len(collector.before) != 4 {
		t.Fatalf("unexpected size for before should be 4, got %v", len(collector.before))
	}
	if len(collector.after) != 4 {
		t.Fatalf("unexpected size for after should be 4, got %v", len(collector.after))
	}
	if collector.after[2].err == nil || collector.after[2].err.Error() != "panic from projector" {
		t.Fatalf("unexpected behaviour, should get an error")
	}
	if collector.after[3].err == nil || collector.after[2].err.Error() != "panic from projector" {
		t.Fatalf("unexpected behaviour, should get an error")
	}
}
