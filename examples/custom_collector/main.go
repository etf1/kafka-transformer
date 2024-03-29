package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/etf1/kafka-transformer/pkg/instrument"
	"github.com/etf1/kafka-transformer/pkg/transformer/kafka"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

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
		Collector: instrument.NewMultiCollector(
			NewCollector("custom_collector"),
			// Add your other collectors here if you have multiple ones...
		),
	}

	transformer, err := kafka.NewKafkaTransformer(config)
	if err != nil {
		log.Fatalf("failed to create transformer: %v", err)
	}

	// prometheus /metrics endpoint
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":8001", nil)
	}()

	// Graceful shutdown
	exitchan := make(chan bool, 1)

	go func() {
		if err := transformer.Run(); err != nil {
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
