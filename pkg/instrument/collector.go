package instrument

import (
	"time"

	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Collector allows to specify a collector for all the main actions of the kafka transformer.
// Main actions are : consume, transform, produce/project
// Before is called before the action and After called after.
// warning: message can be nil, check err != nil also for errors
type Collector interface {
	Before(message *confluent.Message, action Action, start time.Time)
	After(message *confluent.Message, action Action, err error, start time.Time)
}
