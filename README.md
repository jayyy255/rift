# Rift

A Go-native distributed event streaming and work-queue engine.

**Status: early scaffold.** The broker serves a gRPC API (cluster status and
health checks) and Prometheus metrics; the `rift` CLI has the full target command
tree, but only `rift cluster status` is implemented. Storage, messaging,
replication, and queue semantics are not implemented yet.

## Local development

Prerequisites: Go 1.26 or later, Git, Make, a C compiler (for the race detector),
and Docker with Compose. Code-generation and lint tools (buf, protoc-gen-go,
protoc-gen-go-grpc, golangci-lint) are pinned in `tools/` and run through
`go tool`, so no system `protoc` is needed.

```sh
git clone https://github.com/jayyy255/rift.git
cd rift
make check                  # fmt, vet, lint, proto drift, race tests, build
make run                    # broker: gRPC on :7070, metrics on :9100
./bin/rift cluster status   # in another terminal
make dev-up                 # Prometheus :9090, Grafana :3000, Toxiproxy :8474
```

Other targets: `make proto` regenerates `api/gen` from `api/proto`, `make docker`
builds the broker image, `make dev-down` stops the development stack. Technology
choices and pinned versions are recorded in
[ADR 0001](docs/decisions/0001-technology-stack-and-tooling.md).

## Repository layout

```text
api/proto/              Versioned Protobuf API definitions
api/gen/                Generated Go code (committed; do not edit)
cmd/rift-broker/        Broker executable
cmd/rift/               Administrative CLI
internal/broker/       Broker process composition: gRPC, health, metrics
internal/cli/          rift command tree (Cobra)
internal/cluster/      Metadata consensus and membership
internal/logstore/     WAL, segments, indexes, and recovery
internal/replication/  Partition replication, epochs, and commit watermarks
internal/queue/        Leases, ACK/NACK, retry, and dead-letter handling
internal/routing/      Partition routing and future placement controls
internal/version/      Shared build information
pkg/client/            Public Go SDK
configs/               Configuration examples
examples/              Producer and consumer examples
tests/integration/     Broker and API integration tests
tests/chaos/           Crash and network fault scenarios
bench/                 Reproducible workload benchmarks
deployments/compose/   Dev observability/chaos stack; three-broker cluster later
tools/                  Pinned code-generation and lint tools
scripts/               Development and benchmark helpers
docs/                  Architecture, milestones, and decisions
.github/               CI and contribution templates
```

Start with [the roadmap](docs/roadmap.md), [architecture](docs/architecture.md),
and [contribution guide](CONTRIBUTING.md). The first implementation milestone is
single-node durable storage and messaging; distributed and adaptive features follow
correctness and crash-recovery checks.

## Intended guarantees

The design targets partition-local ordering and at-least-once delivery. Local and
quorum acknowledgments must have separate documented contracts. Queue ACKs do not
make external side effects atomic. These are design requirements, not current
implementation guarantees.

## License

No project license has been selected yet.
