package test

import (
	"testing"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-transformer/pkg/kafka"
)

func TestPassThroughTransformer(t *testing.T) {
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

	go func() {
		err = transformer.Run()
	}()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	messages := []*confluent.Message{message(srcTopic, "message1"), message(srcTopic, "message2"), message(srcTopic, "message3")}
	produceMessages(t, messages)

	time.Sleep(5 * time.Second)

	finalMessages := consumeMessages(t, dstTopic)

	t.Logf("Final messages= %v", finalMessages)

	assertEquals(t, messages, finalMessages)

	transformer.Stop()
}
