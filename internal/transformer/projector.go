package transformer

import (
	"log"
	"sync"

	"github.com/etf1/kafka-transformer/pkg/logger"
	pkg "github.com/etf1/kafka-transformer/pkg/transformer"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Projector will project kafka message to kafka topic or somewhere else
type Projector struct {
	projector pkg.Projector
	log       logger.Log
}

// NewProjector is the constructor for a Projector
func NewProjector(log logger.Log, projector pkg.Projector) Projector {
	return Projector{
		projector: projector,
		log:       log,
	}
}

// Run starts the projector goroutine
func (p *Projector) Run(wg *sync.WaitGroup, inChan chan *confluent.Message) {
	go func() {
		defer log.Println("projector stopped")
		defer wg.Done()

		for m := range inChan {
			// body of the loop is executed as a function (to allow recover from panic)
			// because we don't control the implementation provided for the interface
			func() {
				defer func() {
					if err := recover(); err != nil {
						p.log.Errorf("projector: recovered from panic: %v", err)
					}
				}()
				p.log.Debugf("projector: received message %v", m)
				p.projector.Project(m)
			}()
		}

		log.Println("stopping projector")
	}()
}
