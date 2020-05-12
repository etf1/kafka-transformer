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

type configFunc func(*Config)

func WithLogger(l logger.Log) configFunc {
	return func(config *Config) {
		config.Log = l
	}
}

func WithCollector(collector instrument.Collector) configFunc {
	return func(config *Config) {
		config.Collector = collector
	}
}

func WithProducer(producerConfig *confluent.ConfigMap) configFunc {
	return func(config *Config) {
		config.ProducerConfig = producerConfig
	}
}

func WithTransformer(transformer Transformer) configFunc {
	return func(config *Config) {
		config.Transformer = transformer
	}
}

func WithProjector(projector Projector) configFunc {
	return func(config *Config) {
		config.Projector = projector
	}
}

func WithBufferSize(bufferSize int) configFunc {
	return func(config *Config) {
		config.BufferSize = bufferSize
	}
}

func WithTimeout(timeout time.Duration) configFunc {
	return func(config *Config) {
		config.WorkerTimeout = timeout
	}
}
