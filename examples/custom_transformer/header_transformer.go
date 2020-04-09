package main

import (
	"time"

	"github.com/etf1/kafka-transformer/pkg/logger"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type headerTransformer struct {
	log logger.Log
}

// Add a custom header x-app-id to the message
func (ht headerTransformer) Transform(src *kafka.Message) []*kafka.Message {
	topic := "custom-transformer"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:         src.Value,
		Key:           src.Key,
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampCreateTime,
		Headers: append(src.Headers, kafka.Header{
			Key:   "x-app-id",
			Value: []byte("header-transformer"),
		}),
	}
	ht.log.Debugf("header-transformer: transforming message: %v")
	return []*kafka.Message{msg}
}
