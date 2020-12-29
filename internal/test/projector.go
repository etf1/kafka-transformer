package test

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// SliceProjector will simply write the message in a slice
type SliceProjector struct {
	msgs     []*kafka.Message
	readChan chan *kafka.Message
}

// NewSliceProjector will create a simple projector which stores messages in memory in a slice
func NewSliceProjector() *SliceProjector {
	return &SliceProjector{
		readChan: make(chan *kafka.Message, 100),
	}
}

// Project implements Projector interface
func (sp *SliceProjector) Project(msg *kafka.Message) {
	if string(msg.Value) == "panic" {
		panic("panic from projector")
	}
	sp.msgs = append(sp.msgs, msg)
	sp.readChan <- msg
}

func (sp *SliceProjector) assertEquals(t *testing.T, msgs []*kafka.Message) {
	for _, m := range msgs {
		select {
		case msg := <-sp.readChan:
			assertMessageEquals(t, m, msg)
		case <-time.After(20 * time.Second):
			t.Fatalf("unexpected timeout when reading message: %v", m)
		}
	}
}
