kafka-transformer is a library for simplyfing message transformation tasks when using kafka (written proudly in GO :) ).

![Go](https://github.com/etf1/kafka-transformer/workflows/Go/badge.svg)

# Overview

The main goal of this project is to be able to consume messages from a topic, perform a customizable transformation and project the result to another topic via a kafka producer or to an external system with a custom projector.

The user of this library provides a transformation by providing an implementation of the interface `Transformer` as described below:

```golang
type Transformer interface {
	Transform(src *kafka.Message) []*kafka.Message
}
```


```
                            +-------------------+
+------------------+        |                   |         +------------------------------+
|                  |        |                   |         |                              |
|       topic      +------->+ kafka-transformer +-------->+     topic or external        |
|                  |        |                   |         |                              |
+------------------+        |                   |         +------------------------------+
                            +-------------------+

```

# Design pattern

- Passthrough: 1 topic -> 1 topic
- Splitter:    1 topic -> n topics (transformer returns n messages with different topics)
- Projector:   1 topic -> n external systems

# Architecture

kafka-transformer is based on the confluent kafka client: [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go).

Implementation details: 

- kafka-transformer is based confluent-kafka-go which is a wrapper around the C library librdkafka (for more [details](https://github.com/confluentinc/confluent-kafka-go)).
- kafka-transformer is using a double buffering mechanism between the three main components: 
    * consumer
    * transformer with workers (pool of goroutines for parallel transformation)
    * projector (kafka producer of custom projector)
- Each of this components are communicated through channels. For more details check the source code.

# Example

You can find examples in [examples](examples/).

Basically you can run your kafka-transformer as shown below:

```golang
package main

import "github.com/etf1/kafka-transformer/pkg/kafka"

func main(){

    transformer, err := kafka.NewKafkaTransformer(config)
	if err != nil {
		log.Fatalf("failed to create transformer: %v", err)
	}
    // blocking call
    transformer.Run()
}
```

You have to provide a configuration `Config`: 

```golang
type Config struct {
	SourceTopic    string
	ConsumerConfig *confluent.ConfigMap
	ProducerConfig *confluent.ConfigMap
	Transformer    transformer.Transformer
	Log            logger.Log
}
```

- SourceTopic: source topic where the messages will be read from.
- ConsumerConfig: configuration for the consumer (see [documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md))
- ProducerConfig: configuration for the kafka producer. (see [documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)). If you want custom projection provide a Projector (see below). 
- Transformer: implementation of the `Transformer` interface.
- Log: the implementation of the `Log` interface, if you want to use your own logger.
- Projector : implementation of the `Projector` interface for custom projection to an external system (eg redis). This option is incompatible with `ProducerConfig` (see [custom_projector](examples/custom_projector))

Below an example of a configuration:

```golang
broker := "localhost:9092"
config := kafka.Config{
    SourceTopic: "source-topic",
    ConsumerConfig: &confluent.ConfigMap{
        "bootstrap.servers":     broker,
        "broker.address.family": "v4",
        "group.id":              "custom-transformer",
        "session.timeout.ms":    6000,
        "auto.offset.reset":     "earliest",
    },
    ProducerConfig: &confluent.ConfigMap{
        "bootstrap.servers": broker,
    },
    Transformer: customTransformer{},
}

```

# Custom transformer

You can provide your own transformation by providing an implementation of the Transformer interface:

```golang
type Transformer interface {
	Transform(src *kafka.Message) []*kafka.Message
}
```

Here is a usage example:

```golang
type customTransformer struct{}

func (ct customTransformer) Transform(src *kafka.Message) []*kafka.Message {
	topic := "custom-transformer"
	dst := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: append([]byte("New value:"), src.Value...),
	}

	return []*kafka.Message{dst}
}

```

If you want to create multiple messages from one source message, you can achieve it in the transform function. If the messages has different topics, each message will be publish to its specified topic


# Custom projector

You can provide your own projection, if you don't want to project your message to kafka but to an external system like a database or a cache (redis). You need to implement the Projector interface. 

```golang
type Projector interface {
	Project(message *kafka.Message)
}
```
Here is an example which stores the messages in a slice (in memory):

```golang
type sliceProjector struct {
    msgs     []*kafka.Message
}

func (sp *sliceProjector) Project(msg *kafka.Message) {
	sp.msgs = append(sp.msgs, msg)
}

```

# Instrumentation

If you need to instrument each main actions of the kafka transformer, you can provide an implementation of the Collector interface:

```golang
type Collector interface {
	Before(message *confluent.Message, action Action, start time.Time)
	After(message *confluent.Message, action Action, err error, start time.Time)
}
```

Available actions for a Collector are:
	- KafkaConsumerConsume: when a message is consumed
	- KafkaProducerProduce: when a message is produced
	- TransformerTransform: when a message is transformed
	- ProjectorProject: when a message is projected (and not produced to kafka)

You can find examples in [examples](examples/).

# Requirements

* Go 1.13 (minimum)
* install [librdkafka](https://github.com/confluentinc/confluent-kafka-go#installing-librdkafka)

for development:

* docker
* docker-compose

# Local development

* git clone git@github.com:etf1/kafka-transformer.git
* to start local development environment (mainly kafka), run `make dev.up`, your kafka broker will be available at `localhost:9092`
* to run tests `make tests`
* to stop local environment, run `make dev.down`

