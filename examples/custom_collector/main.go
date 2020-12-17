package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/etf1/kafka-transformer/pkg/transformer"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	broker := "localhost:9092"
	cfg := transformer.NewConfig().FromTopic("source-topic", &confluent.ConfigMap{
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              "custom-transformer",
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest",
	}).
		WithCollector(NewCollector("custom_collector")).
		ToProducer(&confluent.ConfigMap{"bootstrap.servers": broker})

	t, err := transformer.NewKafkaTransformer(cfg)
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
		if err := t.Run(); err != nil {
			log.Printf("failed to start transformer: %v", err)
		}
		exitchan <- true
	}()

	select {
	case <-sigchan:
		t.Stop()
	case <-exitchan:
		log.Printf("unexpected exit of the kafka transformer ...")
	}
}
