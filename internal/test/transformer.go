package test

import (
	"github.com/etf1/kafka-transformer/pkg/transformer"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type unstableTransformer struct {
	passthrough transformer.Transformer
}

// NewUnstableTransformer will return a transformer panicking every modulo sequence
func NewUnstableTransformer() transformer.Transformer {
	return unstableTransformer{
		passthrough: transformer.PassThrough(),
	}
}

func (ut unstableTransformer) Transform(msg *kafka.Message) *kafka.Message {
	if string(msg.Value) == "panic" {
		panic("dummy panic from unstable transformer")
	}

	return ut.passthrough.Transform(msg)
}
