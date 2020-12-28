package transformer

import (
	"log"
	"runtime/debug"
	"sync"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	_instrument "github.com/etf1/kafka-transformer/internal/instrument"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/etf1/kafka-transformer/pkg/logger"
	pkg "github.com/etf1/kafka-transformer/pkg/transformer"
)

// Projector will project kafka message to kafka topic or somewhere else
type Projector struct {
	projector pkg.Projector
	log       logger.Log
	collector instrument.Collector
}

// NewProjector is the constructor for a Projector
func NewProjector(log logger.Log, projector pkg.Projector, collector instrument.Collector) Projector {
	return Projector{
		projector: projector,
		log:       log,
		collector: collector,
	}
}

// Run starts the projector goroutine
func (p *Projector) Run(wg *sync.WaitGroup, inChan chan *confluent.Message) {
	go func() {
		defer log.Println("projector stopped")
		defer wg.Done()

		for msg := range inChan {
			var start time.Time
			var th _instrument.TimeHolder

			// body of the loop is executed as a function (to allow recover from panic)
			// because we don't control the implementation provided for the interface
			func() {
				defer func() {
					if r := recover(); r != nil {
						p.collectAfter(msg, toErr(r), start, th)
						p.log.Errorf("projector: recovered from panic: %v", string(debug.Stack()))
					}
				}()
				p.log.Debugf("projector: received message %v", msg)

				start = time.Now()
				th = p.collectBefore(msg, start)

				p.projector.Project(msg)

				p.collectAfter(msg, nil, start, th)

			}()
		}

		log.Println("stopping projector")
	}()
}

func (p Projector) collectBefore(msg *confluent.Message, start time.Time) (th _instrument.TimeHolder) {
	if p.collector != nil {
		th = msg.Opaque.(_instrument.TimeHolder)
		msg.Opaque = th.Opaque
		p.collector.Before(msg, instrument.ProjectorProject, start)
	}
	return
}

func (p Projector) collectAfter(msg *confluent.Message, err error, start time.Time, th _instrument.TimeHolder) {
	if p.collector != nil {
		p.collector.After(msg, instrument.ProjectorProject, err, start)
		p.collector.After(msg, instrument.OverallTime, err, th.ConsumeStart)
	}
}
