package kafka

import (
	"log"
	"sync"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-transformer/internal/timeout"
	"github.com/etf1/kafka-transformer/pkg/logger"
)

// Consumer represents the kafka consumer which will consume messages from a topic
type Consumer struct {
	topic      string
	consumer   *confluent.Consumer
	stopChan   chan bool
	bufferSize int
	log        logger.Log
}

// NewConsumer constructor for Consumer
func NewConsumer(log logger.Log, topic string, config *confluent.ConfigMap, bufferSize int) (Consumer, error) {
	c, err := confluent.NewConsumer(config)

	if err != nil {
		return Consumer{}, err
	}

	return Consumer{
		topic:      topic,
		consumer:   c,
		stopChan:   make(chan bool, 1),
		bufferSize: bufferSize,
		log:        log,
	}, nil
}

// Stop stops the consumer by sending a signal
func (c Consumer) Stop() {
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
		defer timeout.WithTimeout(20*time.Second, func() interface{} {
			log.Println("stopping consumer")
			close(outChan)
			err := c.consumer.Close()
			if err != nil {
				c.log.Errorf("error when closing consumer: %v", err)
			}
			return nil
		})

		for {
			select {
			case _ = <-c.stopChan:
				log.Println("received STOP signal, terminating")
				return
			default:
				ev := c.consumer.Poll(100)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *confluent.Message:
					c.log.Debugf("consumer: received message on %s: %s", e.TopicPartition, string(e.Key))
					if e.Headers != nil {
						c.log.Debugf("consumer: with headers: %v", e.Headers)
					}
					outChan <- e
				case confluent.Error:
					c.log.Errorf("consumer: received an error with code %v: %v", e.Code(), e)
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
