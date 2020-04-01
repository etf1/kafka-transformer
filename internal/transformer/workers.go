package transformer

import (
	"log"
	"sync"
	"time"

	"github.com/etf1/kafka-transformer/pkg/logger"
	pkg "github.com/etf1/kafka-transformer/pkg/transformer"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Workers is a pool of goroutines used for parallel transformation
type Workers struct {
	log logger.Log
	// represents the input channel for transforming message
	workChan chan *confluent.Message
	// maxWorker is the maximum number of goroutines working together
	maxWorker int
	// transformer function to perform by a worker
	transformer pkg.Transformer
}

type chunk [][]*confluent.Message

func newChunk(size int) chunk {
	return make([][]*confluent.Message, size)
}

func (c chunk) reset() {
	for i := 0; i < len(c); i++ {
		c[i] = nil
	}
}

func newWorkers(log logger.Log, maxWorker int, workChan chan *confluent.Message, transformer pkg.Transformer) Workers {
	return Workers{
		log:         log,
		workChan:    workChan,
		maxWorker:   maxWorker,
		transformer: transformer,
	}
}

func flushChunk(resultChan chan *confluent.Message, c chunk, size int) {
	for i := 0; i < size; i++ {
		if c[i] != nil {
			for _, msg := range c[i] {
				resultChan <- msg
			}
		}
	}
	c.reset()
}

// Run starts parallel processing of messages
func (w Workers) Run(resultChan chan *confluent.Message) {
	log.Println("starting transformer workers")

	wg := sync.WaitGroup{}
	chunk := newChunk(w.maxWorker)
	counter := 0

loop:
	for {
		select {
		case msg, ok := <-w.workChan:
			if !ok {
				w.log.Debugf("worker: channel closed, breaking the loop")
				break loop
			}
			wg.Add(1)
			go func(index int, msg *confluent.Message) {
				defer wg.Done()
				defer func() {
					if err := recover(); err != nil {
						w.log.Errorf("worker: a panic has occurred: %v", err)
						chunk[index] = nil
					}
				}()
				w.log.Debugf("worker: #%v, message received %v, working...", index, msg)
				chunk[index] = w.transformer.Transform(msg)
				w.log.Debugf("worker: #%v, work done %v", index, msg)
			}(counter, msg)

			counter++

			if counter != 0 && counter%w.maxWorker == 0 {
				w.log.Debugf("worker: waiting for %v goroutines to complete...", counter)
				wg.Wait()
				flushChunk(resultChan, chunk, counter)
				counter = 0
			}
		case <-time.After(2 * time.Second):
			//w.log.Debugf("worker: timed out...")
			if counter > 0 {
				w.log.Debugf("worker: waiting for %v goroutines to complete...", counter)
				wg.Wait()
				flushChunk(resultChan, chunk, counter)
				counter = 0
			}
		}
	}

	if counter > 0 {
		w.log.Debugf("worker: waiting for %v goroutines to complete...", counter)
		wg.Wait()
		flushChunk(resultChan, chunk, counter)
		counter = 0
	}
	log.Println("transformer workers stopped")
}
