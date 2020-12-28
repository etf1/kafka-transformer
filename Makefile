GOLANG_VERSION?=1.15

.PHONY: dev.up
dev.up:
	docker-compose -p dev up -d kafka

.PHONY: dev.down
dev.down:
	docker-compose -p dev down -v

.PHONY: build
build: tests.docker

.PHONY: verify
verify:
	bash ./scripts/check_gofmt.sh
	bash ./scripts/check_golint.sh

.PHONY: tests
tests: verify
	go test -count=1 -v --tags integration ./...

.PHONY: tests.docker
tests.docker:
	docker-compose -p tests build --build-arg GOLANG_VERSION=${GOLANG_VERSION} tests
	docker-compose -p tests up tests | tee tests.result
	docker-compose -p tests down
	bash ./scripts/check_gotest.sh tests.result
