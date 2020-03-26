package test

import (
	"testing"

	"github.com/etf1/kafka-transformer/pkg/kafka"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func TestTransformer_passthrough(t *testing.T) {
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

	messages := []*confluent.Message{message(srcTopic, "message1"), message(srcTopic, "message2"), message(srcTopic, "message3")}
	produceMessages(t, messages)

	finalMessages := consumeMessages(t, dstTopic)

	assertEquals(t, messages, finalMessages)
}

func TestPassthrough_ordering_should_be_kept(t *testing.T) {
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
