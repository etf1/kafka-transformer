module github.com/etf1/kafka-transformer/examples

go 1.22

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
	github.com/etf1/kafka-transformer v0.0.0-20200327090708-353621d904e9
	github.com/go-redis/redis/v7 v7.2.0
	github.com/prometheus/client_golang v1.5.1
	github.com/sirupsen/logrus v1.8.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.9.1 // indirect
	github.com/prometheus/procfs v0.0.8 // indirect
	golang.org/x/sys v0.6.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)

replace github.com/etf1/kafka-transformer => ../
