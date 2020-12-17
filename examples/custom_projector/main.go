package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/etf1/kafka-transformer/pkg/transformer"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
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
	cfg := transformer.NewConfig().FromTopic("source-topic", &confluent.ConfigMap{
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              "custom-projector",
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest",
	}).ToProjector(redisProjector)

	t, err := transformer.NewKafkaTransformer(cfg)

	if err != nil {
		log.Fatalf("failed to create t: %v", err)
	}

	// Graceful shutdown
	exitchan := make(chan bool, 1)

	go func() {
		if err = t.Run(); err != nil {
			log.Printf("failed to start t: %v", err)
		}
		exitchan <- true
	}()

	select {
	case <-sigchan:
		t.Stop()
	case <-exitchan:
		log.Printf("unexpected exit of the kafka t ...")
	}
}
