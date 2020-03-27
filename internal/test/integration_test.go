package test

import (
	"testing"
	"time"

	"github.com/etf1/kafka-transformer/pkg/transformer/kafka"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
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

	messages := make([]*confluent.Message, 5)

	for i := range messages {
		messages[i] = message(srcTopic, "message"+string(i))
	}

	produceMessages(t, messages)

	finalMessages := consumeMessages(t, dstTopic)

	assertEquals(t, messages, finalMessages)
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

	messages := make([]*confluent.Message, 5)

	for i := range messages {
		messages[i] = message(srcTopic, "message"+string(i))
	}

	messages[2].Value = []byte("panic")

	produceMessages(t, messages)

	finalMessages := consumeMessages(t, dstTopic)

	assertEquals(t, append(messages[0:2], messages[3:5]...), finalMessages)
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

	// why 25 ? because 25 % 10 = 5 , so there will be remaining messages in the queue
	messages := make([]*confluent.Message, 25)

	for i := range messages {
		messages[i] = message(srcTopic, "message"+string(i))
	}

	produceMessages(t, messages)

	finalMessages := consumeMessages(t, dstTopic)

	assertEquals(t, messages, finalMessages)
}

// Default case with a simple projector
func TestProjector_default(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	projector := sliceProjector{}
	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		Projector:      &projector,
		BufferSize:     10,
	}

	transformer, err := kafka.NewKafkaTransformer(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	go func() {
		err = transformer.Run()
	}()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	messages := make([]*confluent.Message, 5)

	for i := range messages {
		messages[i] = message(srcTopic, "message"+string(i))
	}

	produceMessages(t, messages)

	time.Sleep(10 * time.Second)

	transformer.Stop()

	assertEquals(t, messages, projector.list)
}

// Projector should recover from a panic
func TestProjector_recover_panic(t *testing.T) {
	srcTopic := getTopic(t, "source-topic")

	projector := sliceProjector{}
	config := kafka.Config{
		SourceTopic:    srcTopic,
		ConsumerConfig: getConsumerConfig(t, "integration-test-group"),
		Projector:      &projector,
		BufferSize:     10,
	}

	transformer, err := kafka.NewKafkaTransformer(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	go func() {
		err = transformer.Run()
	}()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	messages := make([]*confluent.Message, 5)

	for i := range messages {
		messages[i] = message(srcTopic, "message"+string(i))
	}

	messages[2].Value = []byte("panic")

	produceMessages(t, messages)

	time.Sleep(10 * time.Second)

	transformer.Stop()

	assertEquals(t, append(messages[0:2], messages[3:5]...), projector.list)
}
