package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/etf1/kafka-transformer/pkg/kafka"
	"github.com/sirupsen/logrus"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	broker := "localhost:9092"
	config := kafka.Config{
		SourceTopic: "source-topic",
		Log: func() myLogger {
			logrus.SetFormatter(&logrus.JSONFormatter{})
			logrus.SetLevel(logrus.DebugLevel)
			return myLogger{}
		}(),
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

	transformer, err := kafka.NewKafkaTransformer(config)
	if err != nil {
		log.Fatalf("failed to create transformer: %v", err)
	}

	exitchan := make(chan bool, 1)

	go func() {
		if err = transformer.Run(); err != nil {
			log.Printf("failed to start transformer: %v", err)
		}
		exitchan <- true
	}()

	select {
	case <-sigchan:
		transformer.Stop()
	case <-exitchan:
		log.Printf("unexpected exit of the kafka transformer ...")
	}

}
