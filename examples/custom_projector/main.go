package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-transformer/pkg/transformer/kafka"
)

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	redisProjector, err := NewRedisProjector("")
	if err != nil {
		log.Fatalf("failed to create redis projector: %v", err)
	}
	defer redisProjector.Close()

	broker := "localhost:9092"
	config := kafka.Config{
		SourceTopic: "source-topic",
		ConsumerConfig: &confluent.ConfigMap{
			"bootstrap.servers":     broker,
			"broker.address.family": "v4",
			"group.id":              "custom-projector",
			"session.timeout.ms":    6000,
			"auto.offset.reset":     "earliest",
		},
		Projector: redisProjector,
	}

	transformer, err := kafka.NewKafkaTransformer(config)
	if err != nil {
		log.Fatalf("failed to create transformer: %v", err)
	}

	// Graceful shutdown
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
