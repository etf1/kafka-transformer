package transformer

import (
	"time"

	_logger "github.com/etf1/kafka-transformer/internal/logger"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/etf1/kafka-transformer/pkg/logger"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Config is the configuration used by KafkaTransformer
type Config struct {
	Log            logger.Log
	Collector      instrument.Collector

	SourceTopic    string
	ConsumerConfig *confluent.ConfigMap

	ProducerConfig *confluent.ConfigMap

	BufferSize     int
	WorkerTimeout  time.Duration
	Transformer    Transformer

	Projector      Projector
}

func NewConfig() *Config {
	return &Config{
		Log:            _logger.StdoutLogger(),
		BufferSize:     200,
		WorkerTimeout:  100 * time.Millisecond,
		Transformer:    PassThrough(),
	}
}

func (c *Config) WithLogger(l logger.Log) *Config {
	c.Log = l
	return c
}

func (c *Config) WithCollector(collector instrument.Collector) *Config {
	c.Collector = collector
	return c
}


func (c *Config) WithTransformer(transformer Transformer) *Config {
	c.Transformer = transformer
	return c
}

func (c *Config) WithBufferSize(bufferSize int) *Config {
	c.BufferSize = bufferSize
	return c
}

func (c *Config) WithTimeout(timeout time.Duration) *Config {
	c.WorkerTimeout = timeout
	return c
}

func (c *Config) ToProducer(producerConfig *confluent.ConfigMap) *Config {
	c.ProducerConfig = producerConfig
	return c
}

func (c *Config) ToProjector(projector Projector) *Config {
	c.Projector = projector
	return c
}

func (c *Config) FromTopic(topic string, config *confluent.ConfigMap) *Config {
	return c
}
