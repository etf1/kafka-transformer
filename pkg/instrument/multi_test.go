package instrument_test

import (
	"testing"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-transformer/pkg/instrument"
)

// a dummy collector which appends 1 character eacy time a before/after is called
type dummyCollector struct {
	before string
	after  string
	result string
}

func newDummyCollector(before, after string) *dummyCollector {
	return &dummyCollector{
		before: before,
		after:  after,
	}
}

func (dc *dummyCollector) Before(msg *confluent.Message, action instrument.Action, start time.Time) {
	dc.result += dc.before
}

func (dc *dummyCollector) After(msg *confluent.Message, action instrument.Action, err error, start time.Time) {
	dc.result += dc.after
}

// Rule #1: all collector should be called in sequence as passed in argument
func TestMultiCollector(t *testing.T) {
	dc := newDummyCollector("a", "b")

	mc := instrument.NewMultiCollector(dc)
	mc.Before(nil, instrument.OverallTime, time.Now())
	mc.After(nil, instrument.OverallTime, nil, time.Now())
	if dc.result != "ab" {
		t.Errorf("unexpected result: %q", dc.result)
	}

	dc = newDummyCollector("a", "b")
	mc = instrument.NewMultiCollector(dc, dc)
	mc.Before(nil, instrument.OverallTime, time.Now())
	mc.After(nil, instrument.OverallTime, nil, time.Now())
	if dc.result != "aabb" {
		t.Errorf("unexpected result: %q", dc.result)
	}

	dc = newDummyCollector("b", "a")
	mc = instrument.NewMultiCollector(dc, dc)
	mc.Before(nil, instrument.OverallTime, time.Now())
	mc.After(nil, instrument.OverallTime, nil, time.Now())
	if dc.result != "bbaa" {
		t.Errorf("unexpected result: %q", dc.result)
	}
}
