package transformer

import (
	"log"
	"sync"

	"github.com/etf1/kafka-transformer/pkg/logger"
	"github.com/etf1/kafka-transformer/pkg/transformer"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Transformer represents the transformer which will perform the custom transformation
type Transformer struct {
	transformer transformer.Transformer
	bufferSize  int
	log         logger.Log
}

// NewTransformer constructor for Transformer, bufferSize is the size of the buffered channel
func NewTransformer(log logger.Log, transformer transformer.Transformer, bufferSize int) Transformer {
	return Transformer{
		transformer: transformer,
		bufferSize:  bufferSize,
		log:         log,
	}
}

// Run will start the transformer process
func (t *Transformer) Run(wg *sync.WaitGroup, inChan chan *confluent.Message) chan *confluent.Message {
	outChan := make(chan *confluent.Message, t.bufferSize)
	workers := newWorkers(t.log, t.bufferSize, inChan, t.transformer)

	go func() {
		defer wg.Done()
		defer close(outChan)
		defer log.Println("stopping transformer")

		workers.Run(outChan)
	}()

	return outChan
}
