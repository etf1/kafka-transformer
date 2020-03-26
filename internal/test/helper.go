package test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type dummyTransformer struct{}

func (d dummyTransformer) Transform(src *confluent.Message) (*confluent.Message, error) {
	return src, nil
}

func isRunningInDocker() bool {
	if _, err := os.Stat("/proc/self/cgroup"); os.IsNotExist(err) {
		return false
	}

	content, err := ioutil.ReadFile("/proc/self/cgroup")
	if err != nil {
		log.Print(err)
		return false
	}

	return strings.Contains(string(content), "docker")
}

func getTopic(t *testing.T, prefix string) string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%s-%d", prefix, rand.Intn(1000000))
}

func getBootstrapServers() string {
	if isRunningInDocker() {
		return "kafka:29092"
	}
	return "localhost:9092"
}

func getConsumerConfig(t *testing.T, group string) *confluent.ConfigMap {
	rand.Seed(time.Now().UnixNano())

	return &confluent.ConfigMap{
		"bootstrap.servers":     getBootstrapServers(),
		"broker.address.family": "v4",
		"group.id":              fmt.Sprintf("%s-%d", group, rand.Intn(1000000)),
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest",
	}
}

func getProducerConfig() *confluent.ConfigMap {
	return &confluent.ConfigMap{
		"bootstrap.servers": getBootstrapServers(),
	}
}

func message(topic string, value string) *confluent.Message {
	return &confluent.Message{
		TopicPartition: confluent.TopicPartition{Topic: &topic, Partition: confluent.PartitionAny},
		Value:          []byte(value),
	}
}

func produceMessages(t *testing.T, messages []*confluent.Message) {
	p, err := confluent.NewProducer(getProducerConfig())
	if err != nil {
		t.Fatalf("failed to create producer: %s\n", err)
	}
	defer p.Close()

	deliveryChan := make(chan confluent.Event)
	defer close(deliveryChan)

	for _, message := range messages {
		err = p.Produce(message, deliveryChan)

		e := <-deliveryChan
		m := e.(*confluent.Message)

		if m.TopicPartition.Error != nil {
			t.Fatalf("delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			t.Logf("delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}

	p.Flush(10000)
}

func assertEquals(t *testing.T, a, b []*confluent.Message) {
	if len(a) != len(b) {
		t.Fatalf("messages length not equals, %v != %v", len(a), len(b))
	}

	for i, msg := range a {
		if !reflect.DeepEqual(msg.Headers, b[i].Headers) {
			t.Fatalf("headers not equals, %v != %v", msg.Headers, b[i].Headers)
		}
		if !bytes.Equal(msg.Key, b[i].Key) {
			t.Fatalf("keys not equals, %v != %v", msg.Key, b[i].Key)
		}
		if !bytes.Equal(msg.Value, b[i].Value) {
			t.Fatalf("values not equals, %v != %v", msg.Value, b[i].Value)
		}
	}
}

func consumeMessages(t *testing.T, topic string) []*confluent.Message {
	c, err := confluent.NewConsumer(getConsumerConfig(t, "group"))
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stopChan := make(chan bool)
	go func() {
		time.Sleep(10 * time.Second)
		stopChan <- true
	}()

	result := make([]*confluent.Message, 0)

	for {
		select {
		case <-stopChan:
			return result
		default:
			//print(".")
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *confluent.Message:
				result = append(result, e)
			case confluent.Error:
				t.Fatalf("unexpected error: %v", e)
			default:
				t.Logf("ignoring %v\n", e)
			}
		}
	}

	return result
}
