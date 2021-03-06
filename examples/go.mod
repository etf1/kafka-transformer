module github.com/etf1/kafka-transformer/examples

go 1.15

require (
	github.com/confluentinc/confluent-kafka-go v1.5.2
	github.com/etf1/kafka-transformer v0.0.0-20200327090708-353621d904e9
	github.com/go-redis/redis/v7 v7.2.0
	github.com/prometheus/client_golang v1.5.1
	github.com/sirupsen/logrus v1.4.2
)

replace github.com/etf1/kafka-transformer => ../
