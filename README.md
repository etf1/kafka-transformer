kafka-transformer is a library for simplyfing message transformation tasks when using kafka (written proudly in GO :) ).

![Go](https://github.com/etf1/kafka-transformer/workflows/Go/badge.svg?branch=develop)

# Overview

The main goal of this project is to be able to consume messages from a topic, perform a customizable transformation and publish the result to another topic. 
The user of this library provides a transformation by providing an implementation of the interface `Transformer` as described below:

```golang
type Transformer interface {
	Transform(src *kafka.Message) (*kafka.Message, error)
}
```


```
                            +-------------------+
+------------------+        |                   |         +------------------+
|                  |        |                   |         |                  |
|       topic      +------->+ kafka-transformer +-------->+     topic        |
|                  |        |                   |         |                  |
+------------------+        |                   |         +------------------+
                            +-------------------+

```

# Architecture

kafka-transformer is based on the confluent kafka client: [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go).

Implementation details: 

- kafka-transformer is based confluent-kafka-go which is a wrapper around the C library librdkafka (for more [details](https://github.com/confluentinc/confluent-kafka-go)).
- kafka-transformer is using a double buffering mechanism between the three main components: 
    * consumer
    * transformer
    * producer
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

- SourceTopic: is the source topic where the messages will be read from.
- ConsumerConfig: is the configuration for the consumer (see [documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md))
- ProducerConfig: is the configuration for the producer (see [documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md))
- Transformer: is the implementation of the interface `Transformer`
- Log: the implementation of the interface `Log`, if you want to use your own logger

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

And an example of a dummy transformer implementation:

```golang
type customTransformer struct{}

func (ct customTransformer) Transform(src *kafka.Message) (*kafka.Message, error) {
    topic := "custom-transformer"
	dst := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:         append([]byte("New value:"), src.Value...),
	}

	return dst, nil
}

```

# Requirements

* GO 1.13 (minimum)
* install [librdkafka](https://github.com/confluentinc/confluent-kafka-go#installing-librdkafka)

for development:

* docker
* docker-compose

# Local development

* git clone ...
* to start local development environement (mainly kafka), run `make dev.up`, your kafka broker will be available at `localhost:9092`
* to run tests `make tests`
* to stop local environment, run `make dev.down`



