module github.com/etf1/kafka-transformer/examples

go 1.19

require (
	github.com/confluentinc/confluent-kafka-go v1.9.2
	github.com/etf1/kafka-transformer v0.0.2-0.20221208140254-a1ff542f7eab
	github.com/go-redis/redis/v7 v7.2.0
	github.com/prometheus/client_golang v1.5.1
	github.com/sirupsen/logrus v1.4.2
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.9.1 // indirect
	github.com/prometheus/procfs v0.0.8 // indirect
	golang.org/x/sys v0.0.0-20211007075335-d3039528d8ac // indirect
	google.golang.org/protobuf v1.28.0 // indirect
)

replace github.com/etf1/kafka-transformer => ../
