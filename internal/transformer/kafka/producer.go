package kafka

import (
	"log"

	"github.com/etf1/kafka-transformer/pkg/logger"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Producer represents the kafka producer which will produce
// the transformed message to a topic defined in the Message
type Producer struct {
	producer *confluent.Producer
	log      logger.Log
}

// NewProducer constructor for Producer
func NewProducer(l logger.Log, config *confluent.ConfigMap) (Producer, error) {
	p, err := confluent.NewProducer(config)

	if err != nil {
		return Producer{}, err
	}

	go func() {
		defer log.Println("kafka producer stopped")
		for e := range p.Events() {
			switch ev := e.(type) {
			case *confluent.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					l.Errorf("delivery failed: %v", m.TopicPartition.Error)
				} else {
					l.Debugf("producer: delivered message to topic %s [%d] at offset %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case confluent.Error:
				l.Errorf("producer: received an error with code %v: %v", ev.Code(), ev)
			default:
				l.Debugf("producer: ignoring event: %s", ev)
			}
		}
	}()

	return Producer{
		producer: p,
		log:      l,
	}, nil
}

// Project implements the Projector interface
func (p *Producer) Project(message *confluent.Message) {
	p.producer.ProduceChannel() <- message
}

// Close will close all ressources and the kafka producer
func (p *Producer) Close() {
	// This will close .Events channel and stop the previous goroutine
	defer p.producer.Close()
	// Flushing, for remaining messages in internal buffer
	defer p.producer.Flush(10000)

	log.Println("stopping kafka producer")
}
