package transformer

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/etf1/kafka-transformer/pkg/logger"
	"github.com/etf1/kafka-transformer/pkg/transformer"
)

// Transformer represents the transformer which will perform the custom transformation
type Transformer struct {
	transformer   transformer.Transformer
	bufferSize    int
	log           logger.Log
	collector     instrument.Collector
	workerTimeout time.Duration
}

// NewTransformer constructor for Transformer, bufferSize is the size of the buffered channel
func NewTransformer(log logger.Log, transformer transformer.Transformer, bufferSize int, workerTimeout time.Duration, collector instrument.Collector) Transformer {
	return Transformer{
		transformer:   transformer,
		bufferSize:    bufferSize,
		log:           log,
		collector:     collector,
		workerTimeout: workerTimeout,
	}
}

// Run will start the transformer process
func (t *Transformer) Run(ctx context.Context, wg *sync.WaitGroup, inChan chan *confluent.Message) chan *confluent.Message {
	outChan := make(chan *confluent.Message, t.bufferSize)
	workers := newWorkers(t.log, t.bufferSize, inChan, t.transformer, t.workerTimeout, t.collector)

	go func() {
		defer log.Println("transformer stopped")
		defer wg.Done()
		defer close(outChan)
		defer log.Println("stopping transformer")

		workers.Run(ctx, outChan)
	}()

	return outChan
}

func toErr(i interface{}) error {
	switch x := i.(type) {
	case string:
		return errors.New(x)
	case error:
		return x
	default:
		return errors.New("unknown panic")
	}
}
