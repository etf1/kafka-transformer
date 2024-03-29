package instrument

// Allow to wrap multiple collectors as one collector
import (
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// MultiCollector allows you to compose a list of collector
type MultiCollector struct {
	collectors []Collector
}

// NewMultiCollector is a constructor for multi collector
func NewMultiCollector(collectors ...Collector) MultiCollector {
	return MultiCollector{
		collectors: collectors,
	}
}

// Before calls all Before method of inner collectors
func (m MultiCollector) Before(message *confluent.Message, action Action, start time.Time) {
	for i := 0; i < len(m.collectors); i++ {
		m.collectors[i].Before(message, action, start)
	}
}

// After calls all After method of inner collectors
func (m MultiCollector) After(message *confluent.Message, action Action, err error, start time.Time) {
	for i := 0; i < len(m.collectors); i++ {
		m.collectors[i].After(message, action, err, start)
	}
}
