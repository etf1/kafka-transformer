package test

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-transformer/pkg/transformer"
)

type unstableTransformer struct {
	passthrough transformer.Transformer
}

// NewUnstableTransformer creates a transformer which will panic when a message is equal to "panic"
func NewUnstableTransformer() transformer.Transformer {
	return unstableTransformer{
		passthrough: transformer.PassThrough(),
	}
}

func (ut unstableTransformer) Transform(ctx context.Context, msg *kafka.Message) []*kafka.Message {
	if string(msg.Value) == "panic" {
		panic("panic from transformer")
	}

	return ut.passthrough.Transform(ctx, msg)
}

type duplicatorTransformer struct {
	passthrough transformer.Transformer
}

// NewDuplicatorTransformer creates a transformer which will duplicate each message 1->2
func NewDuplicatorTransformer() transformer.Transformer {
	return duplicatorTransformer{
		passthrough: transformer.PassThrough(),
	}
}

func (dt duplicatorTransformer) Transform(ctx context.Context, msg *kafka.Message) []*kafka.Message {
	result := dt.passthrough.Transform(ctx, msg)
	// return 2 times the same message, for testing duplication
	return append(result, result[0])
}

type opaqueTransformer struct {
	passthrough transformer.Transformer
}

// NewOpaqueTransformer creates a transformer which will set a value for the opaque field of a kafka message.
func NewOpaqueTransformer() transformer.Transformer {
	return opaqueTransformer{
		passthrough: transformer.PassThrough(),
	}
}

func (ot opaqueTransformer) Transform(ctx context.Context, msg *kafka.Message) []*kafka.Message {
	result := ot.passthrough.Transform(ctx, msg)
	result[0].Opaque = "opaque"
	return append(result, result[0])
}
