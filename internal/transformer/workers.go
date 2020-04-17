package transformer

import (
	"log"
	"runtime/debug"
	"sync"
	"time"

	_instrument "github.com/etf1/kafka-transformer/internal/instrument"
	"github.com/etf1/kafka-transformer/pkg/instrument"
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
	collector   instrument.Collector
	// worker pool timeout, when the pool is not full, not reached the maxWorker size
	// we will wait this timeout and flush the current messages
	workerTimeout time.Duration
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

func newWorkers(log logger.Log, maxWorker int, workChan chan *confluent.Message, transformer pkg.Transformer, workerTimeout time.Duration, collector instrument.Collector) Workers {
	return Workers{
		log:           log,
		workChan:      workChan,
		maxWorker:     maxWorker,
		transformer:   transformer,
		collector:     collector,
		workerTimeout: workerTimeout,
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
				var start time.Time
				var th _instrument.TimeHolder

				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						w.collectAfterOne(msg, toErr(r), start, th)
						w.log.Errorf("worker: recovered from panic: %v", string(debug.Stack()))
						chunk[index] = nil
					}
				}()

				start = time.Now()
				th = w.collectBefore(msg, start)

				w.log.Debugf("worker: #%v, message received %v, working...", index, msg)

				chunk[index] = w.transformer.Transform(msg)
				w.collectAfter(chunk[index], nil, start, th)

				w.log.Debugf("worker: #%v, work done %v", index, msg)
			}(counter, msg)

			counter++

			if counter != 0 && counter%w.maxWorker == 0 {
				w.log.Debugf("worker: waiting for %v goroutines to complete...", counter)
				wg.Wait()
				flushChunk(resultChan, chunk, counter)
				counter = 0
			}
		case <-time.After(w.workerTimeout):
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

func (w Workers) collectBefore(msg *confluent.Message, start time.Time) (th _instrument.TimeHolder) {
	th = msg.Opaque.(_instrument.TimeHolder)
	msg.Opaque = nil
	w.collector.Before(msg, instrument.TransformerTransform, start)

	return
}

func (w Workers) collectAfter(msgs []*confluent.Message, err error, start time.Time, th _instrument.TimeHolder) {
	for _, msg := range msgs {
		w.collectAfterOne(msg, err, start, th)
	}
}

func (w Workers) collectAfterOne(msg *confluent.Message, err error, start time.Time, th _instrument.TimeHolder) {
	th.Opaque = msg.Opaque
	msg.Opaque = th
	w.collector.After(msg, instrument.TransformerTransform, err, start)
	if err != nil {
		// if transformation fails, the message will not be projected so we need to set the overall event here.
		// Usually this event is dispatched in projection when no errors occurs
		w.collector.After(msg, instrument.OverallTime, err, th.ConsumeStart)
	}
}
