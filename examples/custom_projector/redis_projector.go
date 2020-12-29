package main

import (
	"log"
	"strconv"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v7"
)

// RedisProjector will project message to redis by implementing transformer.Projector interface
type RedisProjector struct {
	client *redis.Client
}

// NewRedisProjector creates a RedisProjector
func NewRedisProjector(host string) (RedisProjector, error) {

	if host == "" {
		host = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     host,
		Password: "",
		DB:       0,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return RedisProjector{}, err
	}

	return RedisProjector{
		client: client,
	}, nil
}

// Close for closing the redis client
func (r RedisProjector) Close() error {
	if err := r.client.Close(); err != nil {
		return err
	}

	return nil
}

// Project implements transformer.Projector interface
func (r RedisProjector) Project(msg *kafka.Message) {

	if len(msg.Value) == 0 {
		return
	}

	var key string
	if msg.Key != nil {
		key = string(msg.Key)
	} else {
		key = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	log.Printf("redis-projector: project message to redis with key=%s", key)

	r.client.Set(key, msg.Value, 10*time.Minute)
}
