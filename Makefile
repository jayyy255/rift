.PHONY: help build fmt fmt-check vet test check clean

help:
	@echo "Targets: build fmt fmt-check vet test check clean"

build:
	go build -o bin/rift-broker ./cmd/rift-broker
	go build -o bin/rift ./cmd/rift

fmt:
	gofmt -w $$(git ls-files --cached --others --exclude-standard '*.go')

fmt-check:
	@test -z "$$(gofmt -l $$(git ls-files --cached --others --exclude-standard '*.go'))" || { echo "Run make fmt"; exit 1; }

vet:
	go vet ./...

test:
	go test -race ./...

check: fmt-check vet test build

clean:
	go clean
