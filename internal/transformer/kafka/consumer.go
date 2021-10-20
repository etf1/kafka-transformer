package kafka

import (
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/eko/confluent-kafka-go/instrumentation/otel"
	_instrument "github.com/etf1/kafka-transformer/internal/instrument"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/etf1/kafka-transformer/pkg/logger"
)

type KafkaConsumer interface {
	Close() error
	Poll(timeoutMs int) (event kafka.Event)
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
}

// Consumer represents the kafka consumer which will consume messages from a topic
type Consumer struct {
	topic      string
	consumer   KafkaConsumer
	stopChan   chan bool
	bufferSize int
	log        logger.Log
	collector  instrument.Collector
}

// NewConsumer constructor for Consumer
func NewConsumer(log logger.Log, topic string, config *confluent.ConfigMap, collector instrument.Collector, bufferSize int, options ...Option) (*Consumer, error) {
	c, err := confluent.NewConsumer(config)

	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		topic:      topic,
		consumer:   c,
		stopChan:   make(chan bool, 1),
		bufferSize: bufferSize,
		log:        log,
		collector:  collector,
	}

	cfg := &optionsConfig{}
	for _, option := range options {
		option(cfg)
	}

	if cfg.tracerProvider != nil {
		// In case OpenTelemetry tracing is enabled, wrap the original Kafka consumer.
		consumer.consumer = otel.NewConsumerWithTracing(c, otel.WithTracerProvider(cfg.tracerProvider))
	}

	return consumer, nil
}

// Stop stops the consumer by sending a signal
func (c *Consumer) Stop() {
	c.stopChan <- true
}

// Run will start the Consumer process by joining the WaitGroup
func (c *Consumer) Run(wg *sync.WaitGroup) (chan *confluent.Message, error) {

	err := c.consumer.SubscribeTopics([]string{c.topic}, nil)
	if err != nil {
		return nil, err
	}

	outChan := make(chan *confluent.Message, c.bufferSize)

	go func() {
		defer wg.Done()
		defer func() {
			log.Println("stopping consumer")
			close(outChan)
			err := c.consumer.Close()
			if err != nil {
				c.log.Errorf("error when closing consumer: %v", err)
			}
		}()

		for {
			select {
			case _ = <-c.stopChan:
				log.Println("received STOP signal, terminating")
				return
			default:
				start := time.Now()
				ev := c.consumer.Poll(100)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *confluent.Message:
					msg := e
					c.collectBefore(msg, start)
					c.log.Debugf("consumer: received message on %s: %s", e.TopicPartition, string(msg.Key))

					if e.Headers != nil {
						c.log.Debugf("consumer: with headers: %v", msg.Headers)
					}
					outChan <- msg

					c.collectAfter(msg, nil, start)
				case confluent.Error:
					c.log.Errorf("consumer: received an error with code %v: %v", e.Code(), e)
					c.collectAfter(nil, err, start)

					if e.Code() == confluent.ErrAllBrokersDown {
						return
					}
				default:
					c.log.Debugf("consumer: ignoring %v", e)
				}
			}
		}

	}()

	return outChan, nil
}

func (c *Consumer) collectBefore(msg *confluent.Message, start time.Time) {
	if msg != nil {
		th := _instrument.TimeHolder{
			ConsumeStart: start,
		}
		msg.Opaque = th
	}
	c.collector.Before(msg, instrument.OverallTime, start)
	c.collector.Before(msg, instrument.KafkaConsumerConsume, start)
}

func (c *Consumer) collectAfter(msg *confluent.Message, err error, start time.Time) {
	c.collector.After(msg, instrument.KafkaConsumerConsume, err, start)
}
