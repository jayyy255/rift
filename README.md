# Rift

A Go-native distributed event streaming and work-queue engine.

**Status: repository scaffold.** Broker storage, messaging, replication, queue
semantics, and networking are not implemented yet. The command entry points
currently expose help and version information only.

## Local development

Prerequisites: Go 1.26 or later, Git, and Make. Race checks also require a C compiler
and CGO enabled. Docker Compose will be used for cluster testing in later milestones.

```sh
git clone https://github.com/jayyy255/rift.git
cd rift
make check
./bin/rift --version
./bin/rift-broker --help
```

Without Make, run `go vet ./...`, `go test -race ./...`, and `go build ./cmd/...`.
`make fmt` formats Go sources; `make build` writes the binaries to `bin/`.

## Repository layout

```text
api/proto/              Versioned Protobuf API definitions
cmd/rift-broker/        Broker executable
cmd/rift/               Administrative CLI
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
deployments/compose/   Future local three-broker cluster
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
