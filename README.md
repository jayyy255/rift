# Rift

A Go-native distributed event streaming and work-queue engine.

**Status: milestone R1 complete (single node).** One broker durably stores
partitioned topics in a crash-safe segmented log, serves producers and stream
consumers over gRPC, persists consumer-group offsets, enforces retention, and
exposes Prometheus metrics. Replication, consumer-group coordination, and queue
mode are later milestones; see [the roadmap](docs/roadmap.md).

## Local development

Prerequisites: Go 1.26 or later, Git, Make, a C compiler (for the race detector),
and Docker with Compose. Code-generation and lint tools (buf, protoc-gen-go,
protoc-gen-go-grpc, golangci-lint) are pinned in `tools/` and run through
`go tool`, so no system `protoc` is needed.

```sh
git clone https://github.com/jayyy255/rift.git
cd rift
make check            # fmt, vet, lint, proto drift, race tests (incl. crash tests), build
make r1-acceptance    # R1 exit tests; JSON reports in bin/reports/
make run              # broker: gRPC :7070, metrics :9100, data in ./data
make dev-up           # Prometheus :9090, Grafana :3000, Toxiproxy :8474
```

In another terminal:

```sh
./bin/rift topic create orders --partitions 6
./bin/rift publish orders --key customer-17 --data '{"order_id":101}'
seq 1 5 | ./bin/rift publish orders --stdin --durability written
./bin/rift consume orders --group analytics --mode stream --max-messages 6
./bin/rift topic describe orders
```

Publishes default to `fsynced` durability: the acknowledgment is sent only after
the record is on disk. See [the storage format and durability
contract](docs/storage-format.md).

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
internal/broker/       gRPC services, metrics, disk watermarks, process lifecycle
internal/cli/          rift command tree (Cobra)
internal/topics/       Topic registry: metadata, partition logs, retention
internal/offsets/      Durable consumer-group offsets
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

Implemented in R1 and covered by tests: ordering within a partition (records
with the same key keep publish order), at-least-once delivery, and two explicit
acknowledgment levels, `written` (page cache) and `fsynced` (on disk). No
quorum acknowledgment exists until replication (R3). Queue ACKs (R5) will not
make external side effects atomic.

## License

No project license has been selected yet.
