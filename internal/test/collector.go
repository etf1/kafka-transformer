package test

import (
	"fmt"
	"testing"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/etf1/kafka-transformer/pkg/instrument"
)

// SliceCollector is a slice implementation of the Collector interface
type SliceCollector struct {
	before     []collectedItem
	after      []collectedItem
	beforeChan chan collectedItem
	afterChan  chan collectedItem
}

type collectedItem struct {
	message *confluent.Message
	action  instrument.Action
	err     error
	start   time.Time
}

// NewSliceCollector creates a new slice collector
func NewSliceCollector() *SliceCollector {
	return &SliceCollector{
		beforeChan: make(chan collectedItem, 100),
		afterChan:  make(chan collectedItem, 100),
	}
}

func (i collectedItem) String() string {
	return fmt.Sprintf("{key:%v\taction:%v\terr:%v\tstart:%v}", string(i.message.Key), i.action, i.err, i.start)
}

// Before implements Collector.Before
func (c *SliceCollector) Before(message *confluent.Message, action instrument.Action, start time.Time) {
	item := collectedItem{
		message: message,
		action:  action,
		start:   start,
	}
	c.beforeChan <- item
}

// After implements Collector.After
func (c *SliceCollector) After(message *confluent.Message, action instrument.Action, err error, start time.Time) {
	item := collectedItem{
		message: message,
		action:  action,
		err:     err,
		start:   start,
	}
	c.afterChan <- item
}

// Print prints the slice of the collector
func (c SliceCollector) Print(t *testing.T) {
	t.Logf("length before=%v after=%v\n", len(c.before), len(c.after))

	for i, item := range c.before {
		t.Logf("before i=%v %v\n", i, item)
	}
	for i, item := range c.after {
		t.Logf("after i=%v %v\n", i, item)
	}

}

// ReadItems "synchronously" from channels, if timeout then fatal
func (c *SliceCollector) ReadItems(t *testing.T, beforeCount, afterCount int) {
	// read and store the items
	for len(c.after) < beforeCount || len(c.before) < afterCount {
		select {
		case item := <-c.beforeChan:
			c.before = append(c.before, item)
		case item := <-c.afterChan:
			c.after = append(c.after, item)
		case <-time.After(20 * time.Second):
			t.Fatalf("unexpected timeout")
		}
	}
}

func (c SliceCollector) assertExpectedItems(t *testing.T, msgCount int, withProducer bool) {
	c.ReadItems(t, msgCount*4, msgCount*4)
	c.Print(t)
	c.checkSizes(t, msgCount, withProducer)
	c.checkActionsSequenceForBefore(t, msgCount, withProducer)
	c.checkActionsSequenceForAfter(t, msgCount, withProducer)
}

// Make sure we have the right size in the collector
// each message should have : consume/overall + transform + project/produce + overall
func (c SliceCollector) checkSizes(t *testing.T, msgCount int, withProducer bool) {
	if len(c.before) != msgCount*4 {
		t.Fatalf("unexpected before count, should be %v got %v", msgCount*4, len(c.before))
	}

	if len(c.after) != msgCount*4 {
		t.Fatalf("unexpected after count, should be %v got %v", msgCount*4, len(c.after))
	}

	c.checkSizesByAction(t, c.before, msgCount, withProducer)
}

// Make sure we have the right size in the collector
func (c SliceCollector) checkSizesByAction(t *testing.T, items []collectedItem, msgCount int, withProducer bool) {

	actions := []instrument.Action{instrument.OverallTime, instrument.KafkaConsumerConsume, instrument.TransformerTransform}

	if withProducer {
		actions = append(actions, instrument.KafkaProducerProduce)
	} else {
		actions = append(actions, instrument.ProjectorProject)
	}

	for _, action := range actions {
		count := c.getCountByAction(t, items, action)
		if count != msgCount {
			t.Fatalf("unexpected count for action %v, should be %v got %v", action, msgCount, count)
		}
	}
}

func (c SliceCollector) getCountByAction(t *testing.T, items []collectedItem, action instrument.Action) int {
	count := 0
	for _, c := range items {
		if c.action == action {
			count++
		}
	}
	return count
}

func (c SliceCollector) checkActionsSequenceForBefore(t *testing.T, msgCount int, withProducer bool) {
	projectAction := instrument.ProjectorProject
	if withProducer {
		projectAction = instrument.KafkaProducerProduce
	}

	expectedSequence := []instrument.Action{instrument.OverallTime, instrument.KafkaConsumerConsume, instrument.TransformerTransform, projectAction}

	c.checkActionsSequence(t, msgCount, c.before, expectedSequence, withProducer)
}

func (c SliceCollector) checkActionsSequenceForAfter(t *testing.T, msgCount int, withProducer bool) {
	projectAction := instrument.ProjectorProject
	if withProducer {
		projectAction = instrument.KafkaProducerProduce
	}

	expectedSequence := []instrument.Action{instrument.KafkaConsumerConsume, instrument.TransformerTransform, projectAction, instrument.OverallTime}

	c.checkActionsSequence(t, msgCount, c.before, expectedSequence, withProducer)
}

func (c SliceCollector) checkActionsSequence(t *testing.T, msgCount int, items []collectedItem, expectedSequence []instrument.Action, withProducer bool) {

	for i, item := range c.before {
		if item.action == expectedSequence[0] {
			key := string(item.message.Key)
			index := 1
			for j, item2 := range c.before {
				if j > i {
					key2 := string(item2.message.Key)
					if key != key2 {
						continue
					}
					if expectedSequence[index] == item2.action {
						index++
					} else {
						t.Fatalf("unexpected action sequence, should be %v, but got %v / %v %v %v", expectedSequence[index], item2.action, index, key, key2)
					}
				}
			}
		}
	}
}
