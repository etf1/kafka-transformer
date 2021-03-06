package main

import (
	"sync/atomic"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type customProjector struct {
	counter uint32
}

func (c *customProjector) Project(msg *kafka.Message) {
	atomic.AddUint32(&c.counter, 1)
	/*
		if c.counter%2 == 0 {
			time.Sleep(3 * time.Second)
		}
	*/
	if c.counter%5 == 0 {
		panic("error")
	}

}
