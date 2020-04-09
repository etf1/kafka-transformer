package instrument

import (
	"log"
	"time"

	"github.com/etf1/kafka-transformer/pkg/instrument"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type noop struct {
	debug bool
}

// NoopCollector creates a collector which does nothing just logging
func NoopCollector(debug bool) instrument.Collector {
	return noop{debug}
}

func (nc noop) Before(msg *confluent.Message, action instrument.Action, start time.Time) {
	if nc.debug {
		log.Printf("Calling Noop Collector Before with %v %v", action, start)
	}
}

func (nc noop) After(msg *confluent.Message, action instrument.Action, err error, start time.Time) {
	if nc.debug {
		log.Printf("Calling Noop Collector After with %v %v %v", action, err, start)
	}
}
