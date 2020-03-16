package kafka

import (
	"log"
	"sync"
	"time"

	"github.com/etf1/kafka-transformer/internal/timeout"
	"github.com/etf1/kafka-transformer/pkg/logger"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Producer represents the kafka producer which will produce
// the transformed message in a topic defined in the Message
type Producer struct {
	producer *confluent.Producer
	log      logger.Log
}

// NewProducer constructor for Producer
func NewProducer(log logger.Log, config *confluent.ConfigMap) (Producer, error) {
	p, err := confluent.NewProducer(config)

	if err != nil {
		return Producer{}, err
	}

	return Producer{
		producer: p,
		log:      log,
	}, nil
}

// Run will start the producer process
func (p *Producer) Run(wg *sync.WaitGroup, inChan chan *confluent.Message) {
	go func() {
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *confluent.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					p.log.Errorf("delivery failed: %v", m.TopicPartition.Error)
				} else {
					p.log.Debugf("producer: delivered message to topic %s [%d] at offset %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case confluent.Error:
				p.log.Errorf("producer: received an error with code %v: %v", ev.Code(), ev)
			default:
				p.log.Debugf("producer: ignoring event: %s", ev)
			}
		}
		log.Println("exiting producer events loop")
	}()

	go func() {
		defer wg.Done()
		defer timeout.WithTimeout(20*time.Second, func() interface{} {
			log.Println("stopping producer")
			// Flushing, for remaining messages in internal buffer
			p.producer.Flush(10000)
			// This will close .Events channel and stop the previous goroutine
			p.producer.Close()
			return nil
		})

		for msg := range inChan {
			p.producer.ProduceChannel() <- msg
		}

	}()
}
