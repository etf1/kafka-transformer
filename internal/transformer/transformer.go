package transformer

import (
	"log"
	"sync"

	"github.com/etf1/kafka-transformer/pkg/logger"
	"github.com/etf1/kafka-transformer/pkg/transformer"
	pkg "github.com/etf1/kafka-transformer/pkg/transformer"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Transformer represents the transformer which will perform the custom transformation
type Transformer struct {
	transformer pkg.Transformer
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

	go func() {
		defer func() {
			log.Println("stopping transformer")
			close(outChan)
			wg.Done()
		}()
		for m := range inChan {
			// body of the loop is executed as a function (to allow recover from panic)
			// because we don't control the code provided for the Transformer interface
			func() {
				defer func() {
					if err := recover(); err != nil {
						t.log.Errorf("transformer recovered from panic: %v", err)
					}
				}()
				t.log.Debugf("transformer: received message %v", m.Key)
				res := t.transformer.Transform(m)
				if res != nil {
					outChan <- res
				}
			}()
		}
	}()

	return outChan
}
