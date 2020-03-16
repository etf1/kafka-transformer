package main

import (
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type customTransformer struct{}

func (ct customTransformer) Transform(src *kafka.Message) (*kafka.Message, error) {
	topic := "custom-transformer"
	dst := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:         []byte("This is the output of custom-transformer"),
		Key:           src.Key,
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampCreateTime,
		Headers: append(src.Headers, kafka.Header{
			Key:   "x-app-id",
			Value: []byte("custom-transformer"),
		}),
	}

	return dst, nil
}
