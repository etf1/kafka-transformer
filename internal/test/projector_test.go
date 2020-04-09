// +build integration

package test

import (
	"testing"

	"github.com/etf1/kafka-transformer/pkg/transformer/kafka"
)

// Default case with a simple projector
func TestProjector_default(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	projector := NewSliceProjector()
	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		Projector:      projector,
		BufferSize:     10,
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
	projector.assertEquals(t, messages)
}

// Projector should recover from a panic
func TestProjector_recover_panic(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	projector := NewSliceProjector()
	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		Projector:      projector,
		BufferSize:     10,
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
	messages[2].Value = []byte("panic")

	produceMessages(t, messages)
	projector.assertEquals(t, append(messages[0:2], messages[3:5]...))
}
