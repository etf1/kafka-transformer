package kafka

import (
	"log"
	"time"

	_instrument "github.com/etf1/kafka-transformer/internal/instrument"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/etf1/kafka-transformer/pkg/logger"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Producer represents the kafka producer which will produce
// the transformed message to a topic defined in the Message
type Producer struct {
	producer  *confluent.Producer
	log       logger.Log
	collector instrument.Collector
}

// NewProducer constructor for Producer
func NewProducer(l logger.Log, config *confluent.ConfigMap, collector instrument.Collector) (*Producer, error) {
	kafkaProducer, err := confluent.NewProducer(config)

	if err != nil {
		return &Producer{}, err
	}

	p := &Producer{
		producer:  kafkaProducer,
		log:       l,
		collector: collector,
	}

	go func() {
		defer log.Println("kafka producer stopped")
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *confluent.Message:
				msg := ev
				if msg.TopicPartition.Error != nil {
					p.collectAfter(msg, msg.TopicPartition.Error)
					l.Errorf("delivery failed: %v", msg.TopicPartition.Error)
				} else {
					p.collectAfter(msg, nil)
					l.Debugf("producer: delivered message to topic %s [%d] at offset %v",
						*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
				}
			case confluent.Error:
				p.collectAfter(nil, ev)
				l.Errorf("producer: received an error with code %v: %v", ev.Code(), ev)
			default:
				l.Debugf("producer: ignoring event: %s", ev)
			}
		}
	}()

	return p, nil
}

// Project implements the Projector interface
func (p Producer) Project(msg *confluent.Message) {
	p.collectBefore(msg)
	p.producer.ProduceChannel() <- msg
}

// Close will close all ressources and the kafka producer
func (p *Producer) Close() {
	// This will close .Events channel and stop the previous goroutine
	defer p.producer.Close()
	// Flushing, for remaining messages in internal buffer
	defer p.producer.Flush(10000)

	log.Println("stopping kafka producer")
}

func (p Producer) collectBefore(msg *confluent.Message) {
	if msg == nil {
		return
	}
	start := time.Now()
	th := msg.Opaque.(_instrument.TimeHolder)
	th.ProjectStart = start
	msg.Opaque = th
	p.collector.Before(msg, instrument.KafkaProducerProduce, start)
}

func (p Producer) collectAfter(msg *confluent.Message, err error) {
	if msg == nil {
		return
	}
	th := msg.Opaque.(_instrument.TimeHolder)
	p.collector.After(msg, instrument.KafkaProducerProduce, err, th.ProjectStart)
	p.collector.After(msg, instrument.OverallTime, err, th.ConsumeStart)
}
