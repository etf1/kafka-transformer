
To use a local version of the kafka-transformer library:

`go mod edit -replace github.com/etf1/kafka-transformer=/Users/tf1/go/src/github.com/etf1/kafka-transformer`

To start all external dependencies, a docker-compose.yml is provided:

`docker-compose up -d`

and then, to stop all:

`docker-compose down -v`