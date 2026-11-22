.PHONY: help build fmt fmt-check vet lint test check proto proto-lint proto-check run docker dev-up dev-down clean

BUF  := go tool -modfile=tools/go.mod buf
LINT := go tool -modfile=tools/lint/go.mod golangci-lint
COMPOSE_DEV := docker compose -f deployments/compose/dev-deps.yaml

help:
	@echo "Build/test:  build fmt fmt-check vet lint test check clean"
	@echo "Protobuf:    proto proto-lint proto-check"
	@echo "Run:         run docker dev-up dev-down"

build:
	go build -o bin/rift-broker ./cmd/rift-broker
	go build -o bin/rift ./cmd/rift

fmt:
	gofmt -w $$(git ls-files --cached --others --exclude-standard '*.go')

fmt-check:
	@test -z "$$(gofmt -l $$(git ls-files --cached --others --exclude-standard '*.go'))" || { echo "Run make fmt"; exit 1; }

vet:
	go vet ./...

lint:
	$(LINT) run ./...

test:
	go test -race ./...

check: fmt-check vet lint proto-check test build

# Regenerate Go code from api/proto into api/gen (committed).
proto:
	$(BUF) generate

proto-lint:
	$(BUF) lint

# Fails if generated code is stale relative to the .proto sources.
proto-check: proto-lint proto
	@git diff --exit-code -- api/gen || { echo "Generated code is stale: run make proto and commit"; exit 1; }
	@test -z "$$(git ls-files --others --exclude-standard api/gen)" || { echo "Untracked generated files in api/gen: commit them"; exit 1; }

# Metrics listen on all interfaces so the dev Prometheus container can scrape them.
run: build
	./bin/rift-broker --metrics-listen 0.0.0.0:9100

docker:
	docker build -t rift:dev .

dev-up:
	$(COMPOSE_DEV) up -d

dev-down:
	$(COMPOSE_DEV) down

clean:
	go clean
	rm -rf bin
