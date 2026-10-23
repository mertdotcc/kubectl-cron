.DEFAULT_GOAL := help
.PHONY: help build test check e2e

help:
	@echo "build     Build ./kubectl-cron"
	@echo "test      Run unit and fixture pipeline tests"
	@echo "check     Test, vet, and check formatting"
	@echo "e2e       Run the end-to-end suite in a disposable kind cluster"

build:
	go build -o kubectl-cron .

test:
	go test ./...

check: test
	go vet ./...
	@test -z "$$(gofmt -l .)"

e2e:
	./hack/test-kind.sh
