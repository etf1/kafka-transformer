GOLANG_VERSION?=1.22

.PHONY: dev.up
dev.up:
	docker-compose -p dev up -d kafka

.PHONY: dev.down
dev.down:
	docker-compose -p dev down -v

.PHONY: build
build:
	go build -tags musl -v ./...
	cd examples && go build -v -tags musl ./...

.PHONY: verify
verify: build
	bash ./scripts/check_gofmt.sh
	bash ./scripts/check_golint.sh
	bash ./scripts/check_govet.sh

.PHONY: tests
tests: verify
	go test -count=1 -v -tags integration -tags musl ./...

.PHONY: tests.docker
tests.docker:
	docker-compose -p tests build --build-arg GOLANG_VERSION=${GOLANG_VERSION} tests
	docker-compose -p tests up tests | tee tests.result
	docker-compose -p tests down
	bash ./scripts/check_gotest.sh tests.result
