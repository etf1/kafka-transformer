// +build integration

package test

import (
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-transformer/pkg/transformer/kafka"
)

// Default case with a simple transformer
func TestTransformer_default(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")
	dstTopic := srcTopic + "-passthrough"

	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
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

	messages := messages(srcTopic, 5)
	produceMessages(t, messages)
	assertMessagesinTopic(t, dstTopic, messages)
}

// Default case with a simple transformer
func TestTransformer_duplicator(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")
	dstTopic := srcTopic + "-passthrough"

	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		ProducerConfig: getProducerConfig(),
		Transformer:    NewDuplicatorTransformer(),
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

	// 10 because each messages are duplicated
	expectedMessages := make([]*confluent.Message, 0, 10)

	for _, msg := range messages {
		expectedMessages = append(expectedMessages, msg, msg)
	}

	produceMessages(t, messages)
	assertMessagesinTopic(t, dstTopic, expectedMessages)
}

// Transformer should recover from a panic
func TestTransformer_recover_panic(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")
	dstTopic := srcTopic + "-passthrough"

	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		ProducerConfig: getProducerConfig(),
		Transformer:    NewUnstableTransformer(),
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

	// failed message #2 should not be produce
	assertMessagesinTopic(t, dstTopic, append(messages[0:2], messages[3:5]...))
}

// Transformer workers should keep the ordering of the messages
func TestTransformer_workers_ordering(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")
	dstTopic := srcTopic + "-passthrough"

	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		ProducerConfig: getProducerConfig(),
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

	// why 25 ? because 25 % 10 = 5 , so there will be remaining messages in the channel
	// messages are transformed by block of 10 in this case (batch of 10 goroutines, one batch after another)
	messages := messages(srcTopic, 25)

	produceMessages(t, messages)
	assertMessagesinTopic(t, dstTopic, messages)
}

// If a transformer is setting a value in the opaque field on a message, it should be kept to the projector
func TestTransformer_opaque_should_be_kept(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	projector := NewSliceProjector()
	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		Transformer:    NewOpaqueTransformer(),
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

	messages := messages(srcTopic, 1)

	produceMessages(t, messages)
	projector.assertEquals(t, messages)

	if projector.msgs[0].Opaque.(string) != "opaque" {
		t.Fatalf("unexpected opaque field, should be 'opaque' string, got %v", projector.msgs[0].Opaque)
	}
}
