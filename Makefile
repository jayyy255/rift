.PHONY: help build fmt fmt-check vet lint test test-short check r1-acceptance fuzz proto proto-lint proto-check run docker dev-up dev-down clean

BUF  := go tool -modfile=tools/go.mod buf
LINT := go tool -modfile=tools/lint/go.mod golangci-lint
COMPOSE_DEV := docker compose -f deployments/compose/dev-deps.yaml

help:
	@echo "Build/test:  build fmt fmt-check vet lint test test-short check clean"
	@echo "Acceptance:  r1-acceptance (writes JSON reports to bin/reports) fuzz"
	@echo "Protobuf:    proto proto-lint proto-check"
	@echo "Run:         run docker dev-up dev-down"

build:
	go build -o bin/rift-broker ./cmd/rift-broker
	go build -o bin/rift ./cmd/rift

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || { echo "Run make fmt"; exit 1; }

vet:
	go vet ./...

lint:
	$(LINT) run ./...

test:
	go test -race ./...

# Fewer crash iterations and restart cycles for quick local runs.
test-short:
	go test -race -short ./...

# R1 exit tests with machine-readable reports in bin/reports.
r1-acceptance:
	RIFT_REPORT_DIR=$(CURDIR)/bin/reports go test -race -count=1 -v -run 'TestR1' ./tests/...

FUZZTIME ?= 30s
fuzz:
	go test -run '^$$' -fuzz FuzzDecodeFrame -fuzztime $(FUZZTIME) ./internal/logstore

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
	./bin/rift-broker --data-dir ./data --metrics-listen 0.0.0.0:9100

docker:
	docker build -t rift:dev .

dev-up:
	$(COMPOSE_DEV) up -d

dev-down:
	$(COMPOSE_DEV) down

clean:
	go clean
	rm -rf bin
